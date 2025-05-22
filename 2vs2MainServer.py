import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor
import functools
import time
from collections import defaultdict, ChainMap
from hashlib import md5  # 用于数据哈希对比
# 导入WebSocket库
import websockets




# === 配置区 ===
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

# 连接池配置
DB_POOL_CONFIG = {
    "minconn": 2,  # 最小连接数
    "maxconn": 10,  # 最大连接数
    **DB_CONFIG  # 继承基础数据库配置
}

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒

# 定时任务配置
FETCH_INTERVAL = 30  # 秒

# API失效处理配置
API_FAILURE_DELAY = 60  # 失效后暂停时间（秒）

# === 全局变量 ===
postgres_pool = None  # 数据库连接池
last_matches_data = {}  # 上次的比赛数据缓存 {match_name: (hash, data)}

# === 新增：全局比赛数据缓存 ===
all_matches_cache = {}  # 所有比赛的最新数据缓存 {match_name: data}


# === 新增：WebSocket相关配置 ===
WS_CONFIG = {
    "host": "160.25.20.18",
    "port": 8765
}

# 存储所有连接的客户端
connected_clients = set()

# === 装饰器 ===
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


# === API请求模块 ===
async def fetch_api(session, url, retries=MAX_RETRIES):
    """异步获取API数据，带重试机制"""
    for attempt in range(retries):
        start_time = datetime.now()
        try:
            async with session.get(url) as response:
                elapsed = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
                if response.status == 200:
                    data = await response.json(content_type=None)  # 处理非标准JSON响应
                    return {
                        "url": url,
                        "status": "success",
                        "data": data,
                        "timestamp": datetime.now().isoformat(),
                        "response_time": elapsed
                    }
        except Exception as e:
            pass
        if attempt < retries - 1:
            await asyncio.sleep(RETRY_DELAY)
    return {
        "url": url,
        "status": "error",
        "error_message": f"达到最大重试次数 ({retries})",
        "timestamp": datetime.now().isoformat(),
        "response_time": (datetime.now() - start_time).total_seconds() * 1000
    }


# === 数据库连接池模块 ===
def init_db_pool():
    """初始化数据库连接池"""
    global postgres_pool
    try:
        postgres_pool = pool.SimpleConnectionPool(**DB_POOL_CONFIG)
        print(
            f"✅ 数据库连接池初始化成功，最小连接数: {DB_POOL_CONFIG['minconn']}，最大连接数: {DB_POOL_CONFIG['maxconn']}")
        return True
    except Exception as e:
        print(f"❌ 数据库连接池初始化失败: {e}")
        return False


def get_db_connection():
    """从连接池获取数据库连接"""
    if postgres_pool is None:
        if not init_db_pool():
            return None
    try:
        return postgres_pool.getconn()
    except Exception as e:
        print(f"❌ 从连接池获取连接失败: {e}")
        return None


def release_db_connection(conn):
    """将数据库连接释放回连接池"""
    if conn and postgres_pool:
        try:
            postgres_pool.putconn(conn)
        except Exception as e:
            print(f"❌ 释放连接回池失败: {e}")


# === 新增：初始化数据库表 ===
def init_db_tables():
    """创建必要的数据库表"""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            # 创建比赛信息表（添加start_time_beijing到唯一约束）
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                match_name TEXT NOT NULL,
                league_name TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                start_time_beijing TEXT NOT NULL,  -- 新增非空约束
                time_until_start TEXT,
                CONSTRAINT unique_match_time UNIQUE (match_name, start_time_beijing)  -- 显式命名约束
            )
            """)

            # 创建赔率变化记录表 - 让分盘
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS spread_odds (
                id SERIAL PRIMARY KEY,
                match_id INTEGER NOT NULL,
                source INTEGER NOT NULL,
                spread_value TEXT NOT NULL,
                side TEXT NOT NULL,  -- 'home' 或 'away'
                odds_value NUMERIC(5,2) NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches (id)
            )
            """)

            # 创建赔率变化记录表 - 大小球
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS total_odds (
                id SERIAL PRIMARY KEY,
                match_id INTEGER NOT NULL,
                source INTEGER NOT NULL,
                total_value TEXT NOT NULL,
                side TEXT NOT NULL,  -- 'over' 或 'under'
                odds_value NUMERIC(5,2) NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches (id)
            )
            """)

            # 创建索引以加速查询（包含start_time_beijing）
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_matches_name_time ON matches (match_name, start_time_beijing)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_spread_odds ON spread_odds (match_id, source, spread_value, side, recorded_at)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_total_odds ON total_odds (match_id, source, total_value, side, recorded_at)")

            conn.commit()
            print("✅ 数据库表初始化成功")
            return True
    except Exception as e:
        print(f"❌ 数据库表初始化失败: {e}")
        conn.rollback()
        return False
    finally:
        release_db_connection(conn)


# === 新增：将比赛信息存入数据库（包含start_time_beijing） ===
def save_match_info(match_name: str, match_data: Dict) -> Optional[int]:
    """保存比赛基本信息到数据库，返回match_id（使用match_name + start_time_beijing作为唯一标识）"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cursor:
            # 插入或更新比赛信息（基于match_name和start_time_beijing）
            cursor.execute("""
            INSERT INTO matches (match_name, league_name, home_team, away_team, start_time_beijing, time_until_start)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_name, start_time_beijing) DO UPDATE
            SET league_name = EXCLUDED.league_name,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                time_until_start = EXCLUDED.time_until_start
            RETURNING id
            """, (
                match_name,
                match_data["league_name"],
                match_data["home_team"],
                match_data["away_team"],
                match_data["start_time_beijing"],  # 必须非空
                match_data["time_until_start"]
            ))

            match_id = cursor.fetchone()[0]
            conn.commit()
            return match_id
    except Exception as e:
        print(f"❌ 保存比赛信息失败: {e}")
        conn.rollback()
        return None
    finally:
        release_db_connection(conn)


# === 新增：将赔率变化存入数据库 ===
def save_odds_changes(match_id: int, match_name: str, changes: List[Dict]):
    """保存赔率变化到数据库"""
    if not changes:
        return

    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            for change in changes:
                if change["type"] == "spread":
                    # 保存让分盘变化
                    cursor.execute("""
                    INSERT INTO spread_odds (match_id, source, spread_value, side, odds_value)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (
                        match_id,
                        change["source"],
                        change["spread"],
                        change["side"],
                        change["new_value"]
                    ))
                else:
                    # 保存大小球变化
                    cursor.execute("""
                    INSERT INTO total_odds (match_id, source, total_value, side, odds_value)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (
                        match_id,
                        change["source"],
                        change["total"],
                        change["side"],
                        change["new_value"]
                    ))

            conn.commit()
            print(f"✅ 已保存 {match_name} 的 {len(changes)} 条赔率变化记录")
    except Exception as e:
        print(f"❌ 保存赔率变化失败: {e}")
        conn.rollback()
    finally:
        release_db_connection(conn)


# === 数据处理模块（未修改）===
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
        print(f"❌ 数据库查询失败: {e}")
    finally:
        release_db_connection(conn)
    return league_bindings


def create_team_mapping_cache(bindings: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """创建球队映射缓存，仅包含source1和source2均有值的记录"""
    mapping_cache = {}
    for binding in bindings:
        home_team = binding['source3_home_team']
        away_team = binding['source3_away_team']
        if binding['source1_home_team'] and binding['source2_home_team']:
            mapping_cache[home_team] = {
                "source1": binding['source1_home_team'],
                "source2": binding['source2_home_team']
            }
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
        return []
    team_mapping_cache = create_team_mapping_cache(league_bindings)
    if not team_mapping_cache:
        return []
    league_info = {
        "source1": league_bindings[0]["source1_league"],
        "source2": league_bindings[0]["source2_league"],
        "source3": league_name
    }
    results = []
    required_sources = {1, 2, 3}
    for match in matches:
        home_team = match["home_team"]
        away_team = match["away_team"]
        if home_team not in team_mapping_cache or away_team not in team_mapping_cache:
            continue
        home_mapping = team_mapping_cache[home_team]
        away_mapping = team_mapping_cache[away_team]
        matched_apis = {}
        missing_sources = set(required_sources)
        for source_index in required_sources:
            if source_index == 3:
                db_key = (league_info["source3"], home_team, away_team)
            else:
                league_key = league_info[f"source{source_index}"]
                home_key = home_mapping[f"source{source_index}"]
                away_key = away_mapping[f"source{source_index}"]
                db_key = (league_key, home_key, away_key)
            api_index = all_api_indexes.get(source_index, {})
            if db_key in api_index:
                matched_apis[source_index] = api_index[db_key]
                missing_sources.discard(source_index)
        if not missing_sources:
            results.append((match, {"home": home_mapping, "away": away_mapping, "league": league_info}, matched_apis))
    return results


# === 核心数据处理（未修改）===
@timed
async def process_api_data(results: List[Dict[str, Any]]):
    """Process API data and cross-match with database data"""
    all_api_data = {}
    all_api_indexes = {}

    for i, url in enumerate(API_URLS, 1):
        result = next((r for r in results if r["url"] == url), {"status": "error"})
        if result["status"] == "success":
            all_api_data[i] = result["data"]
            all_api_indexes[i] = create_api_index(result["data"])
        else:
            all_api_data[i] = []
            all_api_indexes[i] = {}

    source3_result = next((r for r in results if r["url"] == API_URLS[2]), None)
    if not source3_result or source3_result["status"] != "success":
        return None

    source3_data = source3_result["data"]
    total_matches_source3 = len(source3_data)

    league_groups = defaultdict(list)
    for match in source3_data:
        league_groups[match["league_name"]].append(match)

    league_bindings_map = batch_fetch_bindings(list(league_groups.keys()))

    tasks = []
    for league_name, matches in league_groups.items():
        tasks.append(process_league(
            league_name,
            matches,
            all_api_indexes,
            league_bindings_map.get(league_name, [])
        ))

    league_results = await asyncio.gather(*tasks)

    all_matched_matches = [match for league_result in league_results for match in league_result]
    total_matched = len(all_matched_matches)

    # 统计信息打印
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

    # 用于存储所有比赛的数据
    all_matches_data = {}

    for match, team_mapping, matched_apis in all_matched_matches:
        # 提取三个数据源的赔率数据
        source1_odds = matched_apis[1].get('odds', {'spreads': {}, 'totals': {}})
        source2_odds = matched_apis[2].get('odds', {'spreads': {}, 'totals': {}})
        source3_odds = matched_apis[3].get('odds', {'spreads': {}, 'totals': {}})

        # ---------------------- 1. 计算盘口数值交集 ----------------------
        common_spreads = set(source1_odds['spreads'].keys()) & \
                         set(source2_odds['spreads'].keys()) & \
                         set(source3_odds['spreads'].keys())

        common_totals = set(source1_odds['totals'].keys()) & \
                        set(source2_odds['totals'].keys()) & \
                        set(source3_odds['totals'].keys())

        # ---------------------- 2. 为每个盘口数值独立计算键交集 ----------------------
        def get_spread_side_keys(odds_data, spread_key):
            """获取单个让分盘口的键"""
            return set(odds_data['spreads'].get(spread_key, {}).keys())

        def get_total_side_keys(odds_data, total_key):
            """获取单个大小球盘口的键"""
            return set(odds_data['totals'].get(total_key, {}).keys())

        # 构建让分盘口的键交集映射：{spread_key: common_side_keys}
        spread_key_intersections = {}
        for spread_key in common_spreads:
            s1_keys = get_spread_side_keys(source1_odds, spread_key)
            s2_keys = get_spread_side_keys(source2_odds, spread_key)
            s3_keys = get_spread_side_keys(source3_odds, spread_key)
            spread_key_intersections[spread_key] = s1_keys & s2_keys & s3_keys

        # 构建大小球盘口的键交集映射：{total_key: common_side_keys}
        total_key_intersections = {}
        for total_key in common_totals:
            s1_keys = get_total_side_keys(source1_odds, total_key)
            s2_keys = get_total_side_keys(source2_odds, total_key)
            s3_keys = get_total_side_keys(source3_odds, total_key)
            total_key_intersections[total_key] = s1_keys & s2_keys & s3_keys

        # ---------------------- 3. 独立过滤每个盘口的键 ----------------------
        def filter_odds(odds_data):
            """按盘口数值独立过滤键"""
            filtered_spreads = {}
            for spread_key in common_spreads:
                # 获取该盘口的三方共有键
                common_keys = spread_key_intersections.get(spread_key, set())
                spread_info = odds_data['spreads'].get(spread_key, {})
                filtered_spreads[spread_key] = {
                    k: v for k, v in spread_info.items() if k in common_keys
                }

            filtered_totals = {}
            for total_key in common_totals:
                # 获取该盘口的三方共有键
                common_keys = total_key_intersections.get(total_key, set())
                total_info = odds_data['totals'].get(total_key, {})
                filtered_totals[total_key] = {
                    k: v for k, v in total_info.items() if k in common_keys
                }

            return {
                "spreads": filtered_spreads,
                "totals": filtered_totals
            }

        # 应用过滤
        filtered_odds = {
            1: filter_odds(source1_odds),
            2: filter_odds(source2_odds),
            3: filter_odds(source3_odds)
        }

        # 使用数据源2的名称作为比赛名称
        match_name = f"{team_mapping['league']['source2']} - {team_mapping['home']['source2']} vs {team_mapping['away']['source2']}"

        # 构建每个数据源的数据
        source_data = []
        for source_index in sorted(matched_apis.keys()):
            api_match = matched_apis[source_index]

            # 处理主客队名称
            if source_index == 3:
                home_team = api_match['home_team']
                away_team = api_match['away_team']
                league_name = api_match['league_name']
            else:
                source_key = f"source{source_index}"
                home_team = team_mapping["home"][source_key]
                away_team = team_mapping["away"][source_key]
                league_name = team_mapping["league"][source_key]

            # 构建单个数据源的数据
            source_entry = {
                "source": source_index,
                "league": league_name,
                "home_team": home_team,
                "away_team": away_team,
                "odds": filtered_odds[source_index]
            }
            source_data.append(source_entry)

        # 从数据源1的原始数据中获取start_time_beijing和time_until_start
        source1_match_key = (team_mapping['league']['source1'],
                             team_mapping['home']['source1'],
                             team_mapping['away']['source1'])

        # 查找数据源1中对应的比赛
        source1_raw_match = None
        for match in all_api_data[1]:
            if (match.get('league_name') == source1_match_key[0] and
                    match.get('home_team') == source1_match_key[1] and
                    match.get('away_team') == source1_match_key[2]):
                source1_raw_match = match
                break

        # 提取所需字段（确保start_time_beijing非空）
        start_time_beijing = source1_raw_match.get('start_time_beijing', '') if source1_raw_match else ''
        if not start_time_beijing:
            print(f"警告: 比赛 {match_name} 的start_time_beijing为空，可能导致数据库唯一约束失败")
            continue  # 跳过空时间的比赛（根据需求处理）

        time_until_start = source1_raw_match.get('time_until_start', '') if source1_raw_match else ''

        # 将该比赛的数据添加到总数据中
        all_matches_data[match_name] = {
            "league_name": team_mapping['league']['source2'],
            "home_team": team_mapping['home']['source2'],
            "away_team": team_mapping['away']['source2'],
            "start_time_beijing": start_time_beijing,
            "time_until_start": time_until_start,
            "sources": source_data
        }

    return all_matches_data


# === 新增：计算赔率数据哈希 ===
def calculate_odds_hash(match_data: Dict) -> str:
    """计算比赛赔率数据的MD5哈希值"""
    if not match_data:
        return ""

    # 提取并序列化赔率数据
    odds_data = {}
    for source in match_data.get("sources", []):
        source_id = source.get("source")
        odds_data[source_id] = source.get("odds", {})

    # 转换为JSON字符串并排序键，确保相同赔率生成相同哈希
    odds_str = json.dumps(odds_data, sort_keys=True, ensure_ascii=False, default=str).encode('utf-8')
    return md5(odds_str).hexdigest()


# === 新增：对比两个比赛的赔率，找出具体变化 ===
def compare_odds(old_data: Dict, new_data: Dict) -> List[Dict]:
    """对比两个比赛的赔率，返回详细变化列表（保留None值的比较）"""
    changes = []

    # 按source分组比较
    old_sources = {s["source"]: s["odds"] for s in old_data.get("sources", [])}
    new_sources = {s["source"]: s["odds"] for s in new_data.get("sources", [])}

    # 遍历所有数据源
    for source_id in set(old_sources.keys()) | set(new_sources.keys()):
        old_odds = old_sources.get(source_id, {})
        new_odds = new_sources.get(source_id, {})

        # 比较让分盘
        for spread_key in set(old_odds.get("spreads", {}).keys()) | set(new_odds.get("spreads", {}).keys()):
            old_spread = old_odds.get("spreads", {}).get(spread_key, {})
            new_spread = new_odds.get("spreads", {}).get(spread_key, {})

            for side in set(old_spread.keys()) | set(new_spread.keys()):
                old_value = old_spread.get(side)
                new_value = new_spread.get(side)

                # 修改点：保留None值的比较，只要新旧值不同就记录变化
                if old_value != new_value:
                    changes.append({
                        "type": "spread",
                        "source": source_id,
                        "spread": spread_key,
                        "side": side,
                        "old_value": old_value,
                        "new_value": new_value
                    })

        # 比较大小球
        for total_key in set(old_odds.get("totals", {}).keys()) | set(new_odds.get("totals", {}).keys()):
            old_total = old_odds.get("totals", {}).get(total_key, {})
            new_total = new_odds.get("totals", {}).get(total_key, {})

            for side in set(old_total.keys()) | set(new_total.keys()):
                old_value = old_total.get(side)
                new_value = new_total.get(side)

                # 修改点：保留None值的比较，只要新旧值不同就记录变化
                if old_value != new_value:
                    changes.append({
                        "type": "total",
                        "source": source_id,
                        "total": total_key,
                        "side": side,
                        "old_value": old_value,
                        "new_value": new_value
                    })

    return changes


# === 新增：检查API响应是否有失败 ===
def check_api_failures(results: List[Dict[str, Any]]) -> bool:
    """检查API请求结果中是否有失败的情况"""
    failed_apis = [result["url"] for result in results if result["status"] == "error"]
    if failed_apis:
        print(f"❗ 检测到API失效: {', '.join(failed_apis)}")
        return True
    return False


# === 新增：维护全局比赛数据缓存 ===
def update_matches_cache(matches_data: Dict):
    """更新全局比赛数据缓存"""
    global all_matches_cache

    # 先清除已不存在的比赛
    current_matches = set(matches_data.keys())
    old_matches = set(all_matches_cache.keys())
    removed_matches = old_matches - current_matches

    for match_name in removed_matches:
        del all_matches_cache[match_name]

    # 更新或添加比赛数据
    for match_name, data in matches_data.items():
        # 添加更新时间戳
        data_with_timestamp = {
            **data,
            "last_updated": datetime.now().isoformat()
        }
        all_matches_cache[match_name] = data_with_timestamp

    print(f"✅ 比赛数据缓存已更新，当前缓存大小: {len(all_matches_cache)}")


# === 新增：WebSocket广播函数（修改为非阻塞）===
async def broadcast_matches_data():
    """定期广播最新比赛数据给所有连接的客户端"""
    while True:
        try:
            if connected_clients and all_matches_cache:
                data_to_send = {
                    "timestamp": datetime.now().isoformat(),
                    "matches": list(all_matches_cache.values())
                }
                await asyncio.gather(
                    *[client.send(json.dumps(data_to_send)) for client in connected_clients]
                )
        except Exception as e:
            print(f"❌ WebSocket广播失败: {e}")
        await asyncio.sleep(5)  # 每5秒广播一次


async def ws_handler(websocket, path):
    """处理WebSocket连接"""
    # 添加客户端到连接集合
    connected_clients.add(websocket)
    print(f"✅ 新的WebSocket连接，当前连接数: {len(connected_clients)}")

    try:
        # 保持连接打开
        await websocket.wait_closed()
    finally:
        # 连接关闭时移除客户端
        connected_clients.remove(websocket)
        print(f"ℹ️ WebSocket连接已关闭，当前连接数: {len(connected_clients)}")
# === 主函数 ===
@timed
async def main():
    """主函数：周期性获取所有API数据并通过WebSocket推送更新"""
    global postgres_pool, last_matches_data

    # 初始化数据库连接池和表
    if not init_db_pool() or not init_db_tables():
        print("❌ 数据库初始化失败，程序退出")
        return

    try:
        print(f"\n{'=' * 20} 程序启动，获取初始数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'=' * 20}")

        # === WebSocket 服务启动 ===
        ws_server = await websockets.serve(ws_handler, WS_CONFIG["host"], WS_CONFIG["port"])
        print(f"✅ WebSocket服务已启动: ws://{WS_CONFIG['host']}:{WS_CONFIG['port']}")

        # 首次数据获取与初始化
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_api(session, url) for url in API_URLS]
            results = await asyncio.gather(*tasks)

            if check_api_failures(results):
                print(f"⚠️ 程序将暂停 {API_FAILURE_DELAY} 秒后继续运行...")
                await asyncio.sleep(API_FAILURE_DELAY)
                return

            all_matches_data = await process_api_data(results)

        if all_matches_data:
            print("\n" + "=" * 50)
            print(f"📥 初始化：保存初始比赛数据到数据库")
            print("=" * 50)

            for match_name, match_data in all_matches_data.items():
                match_id = save_match_info(match_name, match_data)
                if match_id:
                    dummy_changes = []
                    for source in match_data.get("sources", []):
                        source_id = source.get("source")
                        for spread_key, spread_data in source.get("odds", {}).get("spreads", {}).items():
                            for side, value in spread_data.items():
                                dummy_changes.append({
                                    "type": "spread",
                                    "source": source_id,
                                    "spread": spread_key,
                                    "side": side,
                                    "old_value": None,
                                    "new_value": value
                                })
                        for total_key, total_data in source.get("odds", {}).get("totals", {}).items():
                            for side, value in total_data.items():
                                dummy_changes.append({
                                    "type": "total",
                                    "source": source_id,
                                    "total": total_key,
                                    "side": side,
                                    "old_value": None,
                                    "new_value": value
                                })
                    if dummy_changes:
                        save_odds_changes(match_id, match_name, dummy_changes)
                        print(f"✅ 已保存初始数据: {match_name} ({len(dummy_changes)} 条赔率记录)")

            for match_name, match_data in all_matches_data.items():
                cache_key = (match_name, match_data["start_time_beijing"])
                last_matches_data[cache_key] = (calculate_odds_hash(match_data), match_data)

            update_matches_cache(all_matches_data)

            # 将广播函数作为独立任务运行，不阻塞主循环
            broadcast_task = asyncio.create_task(broadcast_matches_data())

            print(f"\n✅ 初始数据保存完成，共 {len(all_matches_data)} 场比赛")
        else:
            print("ℹ️ 初始数据为空，程序将继续运行但无数据可保存")

        # 主循环：周期性数据处理
        print(f"\n{'=' * 20} 开始周期性数据获取 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'=' * 20}")

        while True:
            try:
                start_time = time.time()
                print(f"\n{'=' * 20} 开始新一轮数据获取 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'=' * 20}")

                # 获取最新数据
                async with aiohttp.ClientSession() as session:
                    tasks = [fetch_api(session, url) for url in API_URLS]
                    results = await asyncio.gather(*tasks)

                if check_api_failures(results):
                    print(f"⚠️ 检测到API请求失败，跳过此轮数据处理")
                    await asyncio.sleep(API_FAILURE_DELAY)
                    continue

                all_matches_data = await process_api_data(results)

                if not all_matches_data:
                    print("ℹ️ 本轮获取的比赛数据为空")
                    await asyncio.sleep(FETCH_INTERVAL)
                    continue

                # 数据对比与变化检测
                new_matches = []  # 新增比赛
                changed_matches = []  # 赔率变化的比赛
                removed_matches = []  # 移除的比赛
                detailed_changes = {}  # 详细赔率变化

                # 使用match_name + start_time_beijing作为唯一标识
                current_cache_keys = {(match_name, data["start_time_beijing"]) for match_name, data in
                                      all_matches_data.items()}
                previous_cache_keys = set(last_matches_data.keys())

                # 检查新增比赛
                for cache_key in current_cache_keys - previous_cache_keys:
                    match_name, _ = cache_key
                    if match_name in all_matches_data:
                        new_matches.append(match_name)
                        current_data = all_matches_data[match_name]
                        current_hash = calculate_odds_hash(current_data)
                        last_matches_data[cache_key] = (current_hash, current_data)

                        # 保存新比赛到数据库
                        match_id = save_match_info(match_name, current_data)
                        if match_id:
                            # 提取所有赔率作为"变化"保存
                            initial_changes = []
                            for source in current_data.get("sources", []):
                                source_id = source.get("source")
                                for spread_key, spread_data in source.get("odds", {}).get("spreads", {}).items():
                                    for side, value in spread_data.items():
                                        initial_changes.append({
                                            "type": "spread",
                                            "source": source_id,
                                            "spread": spread_key,
                                            "side": side,
                                            "old_value": None,
                                            "new_value": value
                                        })
                                for total_key, total_data in source.get("odds", {}).get("totals", {}).items():
                                    for side, value in total_data.items():
                                        initial_changes.append({
                                            "type": "total",
                                            "source": source_id,
                                            "total": total_key,
                                            "side": side,
                                            "old_value": None,
                                            "new_value": value
                                        })
                            if initial_changes:
                                save_odds_changes(match_id, match_name, initial_changes)
                                print(f"✅ 已保存新比赛数据: {match_name} ({len(initial_changes)} 条赔率记录)")

                # 检查赔率变化
                for cache_key in current_cache_keys & previous_cache_keys:
                    match_name, _ = cache_key
                    current_data = all_matches_data[match_name]
                    current_hash = calculate_odds_hash(current_data)
                    previous_hash, previous_data = last_matches_data[cache_key]

                    if current_hash != previous_hash:
                        changed_matches.append(match_name)
                        last_matches_data[cache_key] = (current_hash, current_data)

                        # 计算详细变化
                        changes = compare_odds(previous_data, current_data)
                        if changes:
                            detailed_changes[match_name] = changes

                            # 保存变化到数据库
                            match_id = save_match_info(match_name, current_data)
                            if match_id:
                                save_odds_changes(match_id, match_name, changes)

                # 检查移除的比赛
                for cache_key in previous_cache_keys - current_cache_keys:
                    match_name, _ = cache_key
                    if cache_key in last_matches_data:
                        removed_matches.append(match_name)
                        del last_matches_data[cache_key]

                # 更新全局缓存
                update_matches_cache(all_matches_data)

                # 打印变化统计
                print("\n" + "=" * 50)
                print(f"📊 数据变化统计")
                print("=" * 50)
                print(f"  - 新增比赛: {len(new_matches)}")
                print(f"  - 赔率变化: {len(changed_matches)}")
                print(f"  - 移除比赛: {len(removed_matches)}")

                # 打印新增比赛
                if new_matches:
                    print("\n📈 新增比赛:")
                    for match_name in new_matches:
                        print(f"  - {match_name}")

                # 打印详细赔率变化
                if detailed_changes:
                    print("\n📊 详细赔率变化:")
                    for match_name, changes in detailed_changes.items():
                        print(f"\n  - {match_name}")
                        for change in changes:
                            if change["type"] == "spread":
                                print(
                                    f"    🔹 数据源{change['source']} 让分盘 {change['spread']} - {change['side']}: {change['old_value']} → {change['new_value']}")
                            else:
                                print(
                                    f"    🔹 数据源{change['source']} 大小球 {change['total']} - {change['side']}: {change['old_value']} → {change['new_value']}")

                # 打印移除比赛
                if removed_matches:
                    print("\n❌ 移除比赛:")
                    for match_name in removed_matches:
                        print(f"  - {match_name}")

                if not new_matches and not changed_matches and not removed_matches:
                    print("\nℹ️ 无数据变化")

                # 计算处理时间和下一次获取时间
                elapsed = time.time() - start_time
                next_fetch_in = max(0, FETCH_INTERVAL - elapsed)
                print(f"\n{'=' * 50}")
                print(f"📊 本轮数据处理完成")
                print(f"  - 处理时间: {elapsed:.2f}秒")
                print(f"  - 下次数据获取将在 {next_fetch_in:.2f}秒后进行")
                print(f"{'=' * 50}\n")

                # 等待下一个周期
                await asyncio.sleep(next_fetch_in)

            except Exception as e:
                print(f"❌ 周期数据获取异常: {e}")
                # 记录完整堆栈跟踪
                import traceback
                traceback.print_exc()
                # 等待一段时间再重试
                await asyncio.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        print("\n👋 用户手动终止程序")
    finally:
        # 资源清理
        if 'ws_server' in locals():
            ws_server.close()
            await ws_server.wait_closed()

        # 取消广播任务
        if 'broadcast_task' in locals():
            broadcast_task.cancel()
            await broadcast_task

        # 关闭数据库连接池
        if postgres_pool:
            postgres_pool.closeall()
            print("✅ 数据库连接池已关闭")

        print("👋 程序已退出")


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())