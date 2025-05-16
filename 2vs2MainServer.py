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
            # 使用参数化查询防止SQL注入
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
    """创建球队映射缓存，加速查找"""
    mapping_cache = {}

    for binding in bindings:
        # 处理主队映射
        home_team = binding['source3_home_team']
        if home_team not in mapping_cache:
            mapping_cache[home_team] = {
                "source1": binding['source1_home_team'],
                "source2": binding['source2_home_team']
            }

        # 处理客队映射
        away_team = binding['source3_away_team']
        if away_team not in mapping_cache:
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
    """处理单个联赛的所有比赛，带详细调试日志"""
    if not league_bindings:
        print(f"❌ 联赛 {league_name} 未找到匹配的bindings记录")
        return []

    # 创建球队映射缓存
    team_mapping_cache = create_team_mapping_cache(league_bindings)

    # 验证bindings中source1是否存在空值
    for binding in league_bindings:
        if not binding["source1_home_team"] or not binding["source1_away_team"]:
            print(f"⚠️ 数据库记录不完整: {binding['source3_league']} - {binding['source3_home_team']} vs {binding['source3_away_team']}")
            print(f"  - source1_home_team: {binding['source1_home_team']}")
            print(f"  - source1_away_team: {binding['source1_away_team']}")

    league_info = {
        "source1": league_bindings[0]["source1_league"],
        "source2": league_bindings[0]["source2_league"],
        "source3": league_name
    }

    results = []

    for match in matches:
        home_team = match["home_team"]
        away_team = match["away_team"]

        home_team_mapping = team_mapping_cache.get(home_team, {"source1": None, "source2": None})
        away_team_mapping = team_mapping_cache.get(away_team, {"source1": None, "source2": None})

        # 检查source1映射是否为空
        if not home_team_mapping["source1"] or not away_team_mapping["source1"]:
            print(f"❌ 比赛 {home_team} vs {away_team} 的source1映射为空")
            print(f"  - 主队映射: {home_team_mapping}")
            print(f"  - 客队映射: {away_team_mapping}")
            continue

        team_mapping = {
            "home": {
                "source1": home_team_mapping["source1"],
                "source2": home_team_mapping["source2"],
                "source3": home_team
            },
            "away": {
                "source1": away_team_mapping["source1"],
                "source2": away_team_mapping["source2"],
                "source3": away_team
            },
            "league": league_info
        }

        matched_apis = {}
        for source_index in [1, 2, 3]:
            if source_index not in all_api_indexes:
                print(f"❌ 数据源{source_index}索引未创建")
                continue

            # 生成数据库中的查询键
            db_key = (
                team_mapping["league"][f"source{source_index}"],
                team_mapping["home"][f"source{source_index}"],
                team_mapping["away"][f"source{source_index}"]
            )

            # 获取API中的实际键（前3个示例）
            api_keys_sample = list(all_api_indexes[source_index].keys())[:3]

            # 检查API中是否存在该键
            if db_key not in all_api_indexes[source_index]:
                print(f"❌ 数据源{source_index}中未找到键: {db_key}")
                print(f"  - 数据源{source_index}示例键: {api_keys_sample}")
            else:
                matched_apis[source_index] = all_api_indexes[source_index][db_key]
                print(f"✅ 数据源{source_index}匹配成功: {db_key}")

        if matched_apis:
            results.append((match, team_mapping, matched_apis))

    return results


@timed
async def process_api_data(results: List[Dict[str, Any]]):
    """处理API数据并与数据库数据交叉匹配"""
    # 提取所有API数据并创建索引
    all_api_data = {}
    all_api_indexes = {}

    for i, url in enumerate(API_URLS, 1):
        result = next((r for r in results if r["url"] == url), {"status": "error"})
        if result["status"] == "success":
            all_api_data[i] = result["data"]
            all_api_indexes[i] = create_api_index(result["data"])
            print(f"🔍 已为数据源{i}创建索引，包含 {len(result['data'])} 场比赛")
        else:
            all_api_data[i] = []
            all_api_indexes[i] = {}
            print(f"❌ 无法为数据源{i}创建索引，API请求失败")

    # 获取source3数据
    source3_result = next((r for r in results if r["url"] == API_URLS[2]), None)
    if not source3_result or source3_result["status"] != "success":
        print("❌ 无法获取source3数据")
        return

    source3_data = source3_result["data"]

    print("\n" + "=" * 50)
    print("🔍 开始交叉匹配数据")
    print("=" * 50)

    # 按联赛分组处理比赛
    league_groups = defaultdict(list)
    for match in source3_data:
        league_groups[match["league_name"]].append(match)

    print(f"📊 共发现 {len(league_groups)} 个不同联赛，{len(source3_data)} 场比赛")

    # 批量获取所有联赛的bindings数据
    print("📡 开始批量查询数据库中的联赛绑定数据...")
    league_bindings_map = batch_fetch_bindings(list(league_groups.keys()))
    print(f"✅ 已获取 {len(league_bindings_map)} 个联赛的绑定数据")

    # 并行处理每个联赛
    print("\n🚀 开始并行处理联赛...")
    tasks = []
    for league_name, matches in league_groups.items():
        tasks.append(process_league(
            league_name,
            matches,
            all_api_indexes,
            league_bindings_map.get(league_name, [])
        ))

    # 等待所有联赛处理完成
    league_results = await asyncio.gather(*tasks)

    # 合并结果并输出
    print("\n" + "=" * 50)
    print(f"📊 处理结果汇总 - 共处理 {len(league_groups)} 个联赛")
    print("=" * 50)

    total_matches = 0
    total_matched = 0

    for results in league_results:
        for match, team_mapping, matched_apis in results:
            total_matches += 1
            total_matched += len(matched_apis)

            # 打印组合后的比赛数据
            print("\n" + "-" * 30)
            print(f"📊 匹配的完整比赛数据 ({len(matched_apis)}个数据源)")
            print(f"🎯 原始比赛: {match['home_team']} vs {match['away_team']}")
            print("-" * 30)

            for source_index, api_match in sorted(matched_apis.items()):
                print(f"\n🏆 比赛数据 (数据源{source_index})")
                print(f"联赛: {api_match['league_name']}")
                print(f"主队: {api_match['home_team']}")
                print(f"客队: {api_match['away_team']}")
                print(f"时间: {api_match.get('time', 'N/A')}")
                print("赔率:")
                print(json.dumps(api_match.get('odds', {}), indent=2, ensure_ascii=False))

    print("\n" + "=" * 50)
    print(f"📊 最终统计:")
    print(f"  - 总比赛数: {total_matches}")
    print(f"  - 匹配成功数: {total_matched}")
    print(f"  - 平均每个比赛匹配数据源: {total_matched / total_matches:.2f}")
    print("=" * 50)


@timed
async def main():
    """主函数：并发获取所有API数据并打印"""
    print("\n" + "=" * 50)
    print(f"🚀 开始并发API数据获取流程 - {datetime.now().isoformat()}")
    print("=" * 50)

    # 创建会话
    print("🔄 创建HTTP会话...")
    async with aiohttp.ClientSession() as session:
        print("📡 准备并发请求以下API:")
        for url in API_URLS:
            print(f"  - {url}")

        print("\n🚨 开始并发请求 (这将同时发送所有请求)...")
        start_time = datetime.now()

        # 创建并执行所有请求任务
        tasks = [fetch_api(session, url) for url in API_URLS]
        results = await asyncio.gather(*tasks)

        total_time = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
        print(f"\n🎉 所有API请求已完成 (总耗时: {total_time:.2f}ms)")

        # 计算成功和失败的请求数
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = len(results) - success_count

        print(f"\n📊 请求统计:")
        print(f"  ✅ 成功: {success_count}")
        print(f"  ❌ 失败: {error_count}")

        # 格式化输出每个API的结果
        print("\n" + "=" * 50)
        print(f"API数据获取结果详情 - {datetime.now().isoformat()}")
        print("=" * 50)

        for result in results:
            print(f"\nURL: {result['url']}")
            print(f"状态: {'✅成功' if result['status'] == 'success' else '❌失败'}")
            print(f"响应时间: {result['response_time']:.2f}ms")

            if result['status'] == 'success':
                # 美化JSON输出
                print("数据:")
                try:
                    print(json.dumps(result['data'], indent=2, ensure_ascii=False)[:1000])  # 限制最大长度
                except (TypeError, json.JSONDecodeError):
                    print(f"[非JSON数据] {str(result['data'])[:1000]}")
            else:
                error_info = result.get('status_code', result.get('error_message', '未知错误'))
                print(f"错误信息: {error_info}")

        # 处理API数据并与数据库数据交叉匹配
        await process_api_data(results)


if __name__ == "__main__":
    # 解决Windows系统下的事件循环问题
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())