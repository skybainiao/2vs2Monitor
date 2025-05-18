import asyncio
import aiohttp
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import DictCursor
import functools
import time
from collections import defaultdict, ChainMap

# API配置
API_URLS = [
    "http://127.0.0.1:5001/get_odds1",
    "http://127.0.0.1:5002/get_odds2",
    "http://127.0.0.1:5003/get_odds3"
]

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒


def timed(func):
    """函数执行时间装饰器"""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        print(f"⏱️ {func.__name__} 执行时间: {(end_time - start_time) * 1000:.2f}ms")
        return result

    return wrapper


async def fetch_api(session, url, retries=MAX_RETRIES):
    """异步获取API数据，带重试机制"""
    for attempt in range(retries):
        start_time = datetime.now()

        try:
            async with session.get(url) as response:
                elapsed = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒

                if response.status == 200:
                    data = await response.json(content_type=None)  # 处理非标准JSON响应
                    print(f"✅ {url} 返回状态码 {response.status} (耗时: {elapsed:.2f}ms)")
                    return {
                        "url": url,
                        "status": "success",
                        "data": data,
                        "timestamp": datetime.now().isoformat(),
                        "response_time": elapsed
                    }
                else:
                    print(f"⚠️ {url} 返回错误状态码 {response.status} (耗时: {elapsed:.2f}ms)")

        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
            print(f"❗ 错误: {url} 请求失败 - {str(e)} (耗时: {elapsed:.2f}ms)")

        if attempt < retries - 1:
            print(f"⏰ 等待 {RETRY_DELAY} 秒后重试...")
            await asyncio.sleep(RETRY_DELAY)

    return {
        "url": url,
        "status": "error",
        "error_message": f"达到最大重试次数 ({retries})",
        "timestamp": datetime.now().isoformat(),
        "response_time": (datetime.now() - start_time).total_seconds() * 1000
    }


def get_db_connection():
    """获取数据库连接，带重试机制"""
    for attempt in range(MAX_RETRIES):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"✅ 数据库连接成功 (尝试 {attempt + 1}/{MAX_RETRIES})")
            return conn
        except Exception as e:
            print(f"❗ 数据库连接失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"⏰ 等待 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)

    print("❌ 数据库连接达到最大重试次数")
    return None


def batch_fetch_bindings(league_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """批量获取多个联赛的bindings数据"""
    if not league_names:
        return {}

    conn = get_db_connection()
    if not conn:
        return {}

    league_bindings = defaultdict(list)

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            query = """
            SELECT * FROM bindings 
            WHERE source3_league = ANY(%s)
            """
            cursor.execute(query, (list(league_names),))

            for binding in cursor.fetchall():
                league = binding['source3_league']
                league_bindings[league].append({
                    "source1_league": binding['source1_league'],
                    "source1_home_team": binding['source1_home_team'],
                    "source1_away_team": binding['source1_away_team'],
                    "source2_league": binding['source2_league'],
                    "source2_home_team": binding['source2_home_team'],
                    "source2_away_team": binding['source2_away_team'],
                    "source3_league": binding['source3_league'],
                    "source3_home_team": binding['source3_home_team'],
                    "source3_away_team": binding['source3_away_team']
                })

    except Exception as e:
        print(f"❗ 数据库查询错误: {str(e)}")
    finally:
        if conn:
            conn.close()

    return league_bindings


def create_team_mapping_cache(bindings: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """创建球队映射缓存，仅包含source1和source2均有值的记录"""
    mapping_cache = {}
    for binding in bindings:
        home_team = binding['source3_home_team']
        away_team = binding['source3_away_team']

        # 主队必须同时有source1和source2的映射
        if binding['source1_home_team'] and binding['source2_home_team']:
            mapping_cache[home_team] = {
                "source1": binding['source1_home_team'],
                "source2": binding['source2_home_team']
            }

        # 客队必须同时有source1和source2的映射
        if binding['source1_away_team'] and binding['source2_away_team']:
            mapping_cache[away_team] = {
                "source1": binding['source1_away_team'],
                "source2": binding['source2_away_team']
            }

    return mapping_cache


def create_api_index(api_data: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    """创建API数据索引，加速查找"""
    return {(match["league_name"], match["home_team"], match["away_team"]): match for match in api_data}


async def process_league(league_name: str, matches: List[Dict[str, Any]],
                         all_api_indexes: Dict[int, Dict[Tuple[str, str, str], Dict[str, Any]]],
                         league_bindings: List[Dict[str, Any]]):
    """处理单个联赛的比赛，必须同时匹配source1、source2、source3才视为成功"""
    if not league_bindings:
        print(f"❌ 联赛 {league_name} 未找到匹配的bindings记录，跳过所有比赛")
        return []

    # 创建球队映射缓存（要求source1和source2同时存在）
    team_mapping_cache = create_team_mapping_cache(league_bindings)
    if not team_mapping_cache:
        print(f"⚠️ 联赛 {league_name} 中所有球队均无完整绑定，跳过所有比赛")
        return []

    league_info = {
        "source1": league_bindings[0]["source1_league"],
        "source2": league_bindings[0]["source2_league"],
        "source3": league_name
    }

    results = []
    required_sources = {1, 2, 3}  # 必须同时匹配这三个数据源

    for match in matches:
        home_team = match["home_team"]
        away_team = match["away_team"]

        # 检查数据库绑定是否完整（source1和source2必须同时存在）
        if home_team not in team_mapping_cache or away_team not in team_mapping_cache:
            print(f"❌ 比赛 {home_team} vs {away_team} 缺少数据库绑定，作废")
            continue

        home_mapping = team_mapping_cache[home_team]
        away_mapping = team_mapping_cache[away_team]
        matched_apis = {}
        missing_sources = set(required_sources)  # 记录未匹配的数据源

        # 逐一匹配每个数据源
        for source_index in required_sources:
            if source_index == 3:
                # source3直接使用原始比赛信息（无需绑定，依赖API存在该比赛）
                db_key = (league_info["source3"], home_team, away_team)
            else:
                # source1/source2使用数据库绑定的名称
                league_key = league_info[f"source{source_index}"]
                home_key = home_mapping[f"source{source_index}"]
                away_key = away_mapping[f"source{source_index}"]
                db_key = (league_key, home_key, away_key)

            api_index = all_api_indexes.get(source_index, {})
            if db_key in api_index:
                matched_apis[source_index] = api_index[db_key]
                missing_sources.discard(source_index)  # 从缺失集合中移除已匹配的数据源
            else:
                print(f"❌ 数据源{source_index}未匹配: {db_key}")

        # 检查是否所有数据源均匹配
        if missing_sources:
            print(f"❌ 比赛 {home_team} vs {away_team} 缺少数据源: {missing_sources}，作废")
        else:
            results.append((match, {"home": home_mapping, "away": away_mapping, "league": league_info}, matched_apis))
            print(f"✅ 比赛 {home_team} vs {away_team} 全数据源匹配成功")

    return results


@timed
async def process_api_data(results: List[Dict[str, Any]]):
    """Process API data and cross-match with database data"""
    # Extract all API data and create indexes
    all_api_data = {}
    all_api_indexes = {}

    for i, url in enumerate(API_URLS, 1):
        result = next((r for r in results if r["url"] == url), {"status": "error"})
        if result["status"] == "success":
            all_api_data[i] = result["data"]
            all_api_indexes[i] = create_api_index(result["data"])
            print(f"🔍 Index created for data source {i}, containing {len(result['data'])} matches")
        else:
            all_api_data[i] = []
            all_api_indexes[i] = {}
            print(f"❌ Failed to create index for data source {i}, API request failed")

    # Get source3 data
    source3_result = next((r for r in results if r["url"] == API_URLS[2]), None)
    if not source3_result or source3_result["status"] != "success":
        print("❌ Failed to get source3 data")
        return

    source3_data = source3_result["data"]
    total_matches_source3 = len(source3_data)
    print(f"\n📊 Source3 Data Statistics:")
    print(f"  - Total Matches: {total_matches_source3}")

    print("\n" + "=" * 50)
    print("🔍 Starting Cross-Matching Process (Processing only fully bound matches)")
    print("=" * 50)

    # Group matches by league
    league_groups = defaultdict(list)
    for match in source3_data:
        league_groups[match["league_name"]].append(match)

    print(f"📊 Found {len(league_groups)} different leagues with {total_matches_source3} matches")

    # Batch fetch league bindings from database
    print("📡 Fetching league bindings from database...")
    league_bindings_map = batch_fetch_bindings(list(league_groups.keys()))
    print(f"✅ Successfully retrieved bindings for {len(league_bindings_map)} leagues")

    # Process each league in parallel
    print("\n🚀 Starting parallel league processing...")
    tasks = []
    for league_name, matches in league_groups.items():
        tasks.append(process_league(
            league_name,
            matches,
            all_api_indexes,
            league_bindings_map.get(league_name, [])
        ))

    league_results = await asyncio.gather(*tasks)

    # Combine all successfully matched matches
    all_matched_matches = [match for league_result in league_results for match in league_result]
    total_matched = len(all_matched_matches)

    print("\n" + "=" * 50)
    print(f"📊 Final Matching Statistics")
    print("=" * 50)
    print(f"  - Total Source3 Matches: {total_matched}")
    print(f"  - Matches with Complete Database Bindings: {sum(len(matches) for matches in league_groups.values())}")
    print(f"  - Successfully Matched Matches: {total_matched}")
    print(
        f"  - Matching Success Rate: {total_matched / total_matches_source3 * 100:.2f}% (Based on Source3 total matches)")
    print(
        f"  - Post-Binding Success Rate: {total_matched / (sum(len(matches) for matches in league_groups.values()) or 1) * 100:.2f}% (Based on successfully bound matches)")

    # Print detailed information for matched matches
    for match, team_mapping, matched_apis in all_matched_matches:
        print("\n" + "-" * 30)
        print(f"🎯 Successfully Bound and Matched Match: {match['home_team']} vs {match['away_team']}")

        # Print original names from source3
        print(f"\n📌 Original Names (Source3):")
        print(f"  Home Team: {match['home_team']}")
        print(f"  Away Team: {match['away_team']}")

        for source_index, api_match in sorted(matched_apis.items()):
            print(f"\n🏆 Data from Source {source_index}")
            print(f"League: {api_match['league_name']}")

            # Print source-specific team names
            if source_index == 3:
                # Use original names from source3
                print(f"Home Team: {api_match['home_team']}")
                print(f"Away Team: {api_match['away_team']}")
            else:
                # Use mapped names from database bindings
                source_key = f"source{source_index}"
                home_mapping = team_mapping["home"][source_key]
                away_mapping = team_mapping["away"][source_key]
                print(f"Home Team: {home_mapping}")
                print(f"Away Team: {away_mapping}")

            print(f"Odds: {json.dumps(api_match.get('odds', {}), indent=2)}")


@timed
async def main():
    """主函数：并发获取所有API数据并打印"""
    print("\n" + "=" * 50)
    print(f"🚀 开始并发API数据获取流程 - {datetime.now().isoformat()}")
    print("=" * 50)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_api(session, url) for url in API_URLS]
        results = await asyncio.gather(*tasks)

        print("\n" + "=" * 50)
        print(f"API请求状态汇总")
        print("=" * 50)
        for result in results:
            status_icon = "✅" if result["status"] == "success" else "❌"
            print(f"{status_icon} {result['url']}")

        await process_api_data(results)


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())