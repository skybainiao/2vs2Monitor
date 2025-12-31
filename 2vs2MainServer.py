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
# API配置（新增：两个source1的API，source2固定）
SOURCE1_URLS = [
  #"http://127.0.0.1:5001/get_odds1",
    "http://103.67.53.137:5001/get_odds1",
    "http://122.10.118.13:5001/get_odds1",
    "http://154.222.29.140:5001/get_odds1"

]
SOURCE2_URL = "http://127.0.0.1:5002/get_odds2"  # source2固定API

# 保留原API_URLS结构（动态生成，确保其他逻辑兼容）
API_URLS = [SOURCE1_URLS[0], SOURCE2_URL]  # 初始值，后续会动态更新

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
    "maxconn": 50,  # 最大连接数
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

# 新增：source1轮换相关
current_source1_index = 0  # 轮换索引（0和1交替）

# === 新增：全局比赛数据缓存 ===
all_matches_cache = {}  # 所有比赛的最新数据缓存 {match_name: data}
current_api_errors = set()  # 存储当前失败的API URL


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
                print(url)
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
                result_value NUMERIC(10, 2) DEFAULT NULL,  -- 新增字段
                total_result NUMERIC(10, 2) DEFAULT NULL,  -- 新增：大小球盘指数结果
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
                odds_value NUMERIC(6,3),
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
                odds_value NUMERIC(6,3),
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
            # 提取result值（从calculate_is189的返回结果中获取）
            result_value = match_data.get("result", None)  # 关键行：获取计算结果
            # 提取大小球盘指数结果（新增）
            total_result = match_data.get("total_result", None)
            # 插入或更新比赛信息（基于match_name和start_time_beijing）
            cursor.execute("""
            INSERT INTO matches (match_name, league_name, home_team, away_team, start_time_beijing, time_until_start, result_value, total_result)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_name, start_time_beijing) DO UPDATE
            SET league_name = EXCLUDED.league_name,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                time_until_start = EXCLUDED.time_until_start,
                result_value = EXCLUDED.result_value,  -- 仅更新result字段
                total_result = EXCLUDED.total_result
            RETURNING id
            """, (
                match_name,
                match_data["league_name"],
                match_data["home_team"],
                match_data["away_team"],
                match_data["start_time_beijing"],  # 必须非空
                match_data["time_until_start"],
                result_value,
                total_result
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


# === 修改后的保存赔率变化函数 ===
def save_odds_changes(match_id: int, match_name: str, changes: List[Dict]):
    """保存赔率变化到数据库（明确区分方向，避免混淆）"""
    if not changes:
        return

    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            for change in changes:
                table = "spread_odds" if change["type"] == "spread" else "total_odds"
                field = "spread_value" if change["type"] == "spread" else "total_value"

                # 确保数值类型正确（处理可能的字符串格式）
                value = change[f"{change['type']}_value"]
                try:
                    # 尝试转换为浮点数（适用于盘口值如 "0.5" 或 "-0.75"）
                    float(value)
                except ValueError:
                    # 若无法转换（如数据源返回异常值），记录原始值
                    pass

                cursor.execute(f"""
                INSERT INTO {table} (match_id, source, {field}, side, odds_value)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    match_id,
                    change["source"],
                    change[f"{change['type']}_value"],
                    change["side"],
                    change["new_value"]  # 允许 None 值
                ))

            conn.commit()
            print(f"✅ 已保存 {match_name} 的 {len(changes)} 条赔率变化记录")
    except Exception as e:
        print(f"❌ 保存赔率变化失败: {e}")
        conn.rollback()
    finally:
        release_db_connection(conn)


def batch_fetch_bindings(league_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """批量获取多个联赛的bindings数据，使用投票机制选择最常见的联赛名称（新增前三候选）"""
    if not league_names:
        return {}
    conn = get_db_connection()
    if not conn:
        return {}
    league_bindings = defaultdict(list)

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 修改查询条件：基于source2_league而非source3_league
            query = """
            SELECT * FROM bindings 
            WHERE source2_league = ANY(%s)
            """
            cursor.execute(query, (list(league_names),))

            # 统计各联赛在source1中的名称频率
            league_votes = defaultdict(lambda: {"source1": defaultdict(int)})
            all_bindings = defaultdict(list)

            # 收集所有绑定记录并统计名称频率
            for binding in cursor.fetchall():
                s2_league = binding['source2_league']
                all_bindings[s2_league].append(binding)
                league_votes[s2_league]["source1"][binding['source1_league']] += 1

            # 为每个联赛生成最终的绑定记录（新增：保存前三候选）
            for s2_league, votes in league_votes.items():
                # 确定source1的候选名称（票数前三，降序排列）
                s1_votes = votes["source1"]
                s1_candidates = sorted(s1_votes.items(), key=lambda x: (-x[1], x[0]))[:3]  # 取票数前三
                s1_candidate_names = [name for name, _ in s1_candidates] if s1_candidates else []

                # 处理无投票数据的情况（用原始绑定名称补全）
                if not s1_candidate_names and all_bindings[s2_league]:
                    s1_candidate_names = [binding['source1_league'] for binding in all_bindings[s2_league]][:3]

                # 构建最终的绑定记录（新增source1_candidates字段）
                for binding in all_bindings[s2_league]:
                    league_bindings[s2_league].append({
                        "source1_league": s1_candidate_names[0] if s1_candidate_names else binding['source1_league'],
                        "source1_candidates": s1_candidate_names,  # 新增：前三候选列表
                        "source1_home_team": binding['source1_home_team'],
                        "source1_away_team": binding['source1_away_team'],
                        "source2_league": s2_league,
                        "source2_home_team": binding['source2_home_team'],
                        "source2_away_team": binding['source2_away_team'],
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
        home_team = binding['source2_home_team']  # 现在以source2为基准
        away_team = binding['source2_away_team']  # 现在以source2为基准
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
    """处理单个联赛的比赛，必须同时匹配source1和source2才视为成功（新增source1候选匹配）"""
    if not league_bindings:
        print(f"❌ 联赛 {league_name} 没有找到绑定数据，跳过处理")
        return []

    team_mapping_cache = create_team_mapping_cache(league_bindings)
    if not team_mapping_cache:
        print(f"❌ 联赛 {league_name} 没有有效的球队映射数据，跳过处理")
        return []

    league_info = {
        "source1": league_bindings[0]["source1_league"],
        "source2": league_name  # 现在以source2为基准
    }
    # 获取source1前三候选联赛名称（新增）
    s1_candidates = league_bindings[0].get("source1_candidates", [league_info["source1"]])[:3]

    results = []
    required_sources = {1, 2}  # 仅需匹配source1和source2

    # 统计信息
    total_matches = len(matches)
    matched_count = 0
    unmatched_count = 0
    missing_team_mapping = 0

    print(f"📊 开始处理联赛: {league_name}, 共有 {total_matches} 场比赛")
    print(f"🔍 source1候选联赛名称: {s1_candidates}")  # 新增：打印候选列表

    for match in matches:
        home_team = match["home_team"]
        away_team = match["away_team"]

        # 检查球队是否有映射关系
        if home_team not in team_mapping_cache or away_team not in team_mapping_cache:
            print(f"❌ 比赛 {home_team} vs {away_team} 无法匹配：球队映射缺失")
            missing_team_mapping += 1
            unmatched_count += 1
            continue

        home_mapping = team_mapping_cache[home_team]
        away_mapping = team_mapping_cache[away_team]

        # 打印各源球队名称用于对比
        print(f"🔍 比赛: {home_team} vs {away_team}")
        print(f"  source2球队: {home_team} vs {away_team}")
        print(f"  source1映射: {home_mapping['source1']} vs {away_mapping['source1']}")

        # 尝试在两个数据源中查找匹配的比赛
        matched_apis = {}
        missing_sources = set(required_sources)

        # 先处理source2（逻辑不变）
        if 2 in required_sources:
            db_key = (league_info["source2"], home_team, away_team)
            source_league = league_info["source2"]
            source_home = home_team
            source_away = away_team
            api_index = all_api_indexes.get(2, {})
            print(f"  🔍 source2 匹配键: ({source_league}, {source_home}, {source_away})")

            if db_key in api_index:
                matched_apis[2] = api_index[db_key]
                missing_sources.discard(2)
                print(f"  ✅ source2 匹配成功")
            else:
                print(f"  ⚠️ source2 未找到匹配键")

        # 处理source1（新增：遍历前三候选）
        if 1 in required_sources and 2 in matched_apis:  # source2匹配成功后才尝试source1
            api_index = all_api_indexes.get(1, {})
            home_key = home_mapping["source1"]
            away_key = away_mapping["source1"]
            match_found = False

            # 依次尝试前三候选
            for i, league_key in enumerate(s1_candidates):
                if not league_key:
                    continue
                db_key = (league_key, home_key, away_key)
                print(f"  🔍 source1 候选{i+1}匹配键: ({league_key}, {home_key}, {away_key})")

                if db_key in api_index:
                    matched_apis[1] = api_index[db_key]
                    missing_sources.discard(1)
                    print(f"  ✅ source1 候选{i+1}匹配成功")
                    match_found = True
                    break
                else:
                    print(f"  ⚠️ source1 候选{i+1}未找到匹配键")

            if not match_found:
                print(f"  ❌ source1 前三候选均匹配失败")

        # 只有当两个数据源都匹配成功时才添加到结果中
        if not missing_sources:
            # 为保持数据结构一致，添加空的source3数据
            matched_apis[3] = {
                "league_name": league_info["source2"],  # 使用source2的联赛名作为默认值
                "home_team": home_team,
                "away_team": away_team,
                "odds": {"spreads": {}, "totals": {}}  # 空赔率数据
            }

            results.append((match, {"home": home_mapping, "away": away_mapping, "league": league_info}, matched_apis))
            matched_count += 1
            print(f"✅ 比赛 {home_team} vs {away_team} 两源匹配成功")
        else:
            missing_msg = ", ".join([f"source{src}" for src in missing_sources])
            print(f"❌ 比赛 {home_team} vs {away_team} 匹配失败：缺少数据源 {missing_msg}")
            unmatched_count += 1

    # 打印联赛处理统计信息
    print(f"📊 联赛 {league_name} 处理完成:")
    print(f"  - 总比赛数: {total_matches}")
    print(f"  - 成功匹配: {matched_count} ({matched_count / total_matches * 100:.2f}%)")
    print(f"  - 匹配失败: {unmatched_count} ({unmatched_count / total_matches * 100:.2f}%)")
    print(f"  - 因球队映射缺失失败: {missing_team_mapping}")

    return results


def calculate_common_odds(source1_odds, source2_odds, source3_odds):
    """
    修复：只计算source1和source2的赔率交集（忽略source3，因为它已为空）
    返回格式: {
        "spreads": {盘口值: {"home": 是否存在, "away": 是否存在}},
        "totals": {盘口值: {"over": 是否存在, "under": 是否存在}}
    }
    """
    common_spreads = {}
    common_totals = {}

    # 处理让分盘交集（仅考虑source1和source2）
    all_spreads = set()
    all_spreads.update(source1_odds.get('spreads', {}).keys())
    all_spreads.update(source2_odds.get('spreads', {}).keys())

    for spread in all_spreads:
        # 只检查source1和source2是否有该盘口
        has_source1 = spread in source1_odds.get('spreads', {})
        has_source2 = spread in source2_odds.get('spreads', {})

        if has_source1 and has_source2:
            # 检查主客队方向是否在两个数据源都存在
            home_in_all = (
                    'home' in source1_odds['spreads'][spread] and
                    'home' in source2_odds['spreads'][spread]
            )
            away_in_all = (
                    'away' in source1_odds['spreads'][spread] and
                    'away' in source2_odds['spreads'][spread]
            )

            if home_in_all or away_in_all:
                common_spreads[spread] = {
                    'home': home_in_all,
                    'away': away_in_all
                }

    # 处理大小球交集（仅考虑source1和source2）
    all_totals = set()
    all_totals.update(source1_odds.get('totals', {}).keys())
    all_totals.update(source2_odds.get('totals', {}).keys())

    for total in all_totals:
        # 只检查source1和source2是否有该盘口
        has_source1 = total in source1_odds.get('totals', {})
        has_source2 = total in source2_odds.get('totals', {})

        if has_source1 and has_source2:
            over_in_all = (
                    'over' in source1_odds['totals'][total] and
                    'over' in source2_odds['totals'][total]
            )
            under_in_all = (
                    'under' in source1_odds['totals'][total] and
                    'under' in source2_odds['totals'][total]
            )

            if over_in_all or under_in_all:
                common_totals[total] = {
                    'over': over_in_all,
                    'under': under_in_all
                }

    return {
        'spreads': common_spreads,
        'totals': common_totals
    }


def calculate_odds_max(matches_data: Dict) -> Dict:
    """计算每场比赛每个盘口的最大赔率并添加到数据中，处理字符串赔率和特殊正负数逻辑"""
    for match_key, match_data in matches_data.items():
        # 初始化max字段
        match_data['max'] = {
            'spreads': {},
            'totals': {}
        }

        # 获取所有数据源的赔率
        source_odds = [source.get('odds', {}) for source in match_data.get('sources', [])]

        # 收集所有存在的盘口值
        all_spread_values = set()
        for source_odd in source_odds:
            all_spread_values.update(source_odd.get('spreads', {}).keys())

        all_total_values = set()
        for source_odd in source_odds:
            all_total_values.update(source_odd.get('totals', {}).keys())

        # === 计算让分盘的最大值（处理字符串、空值和正负数逻辑） ===
        for spread_value in all_spread_values:
            match_data['max']['spreads'][spread_value] = {
                'home': None,
                'away': None
            }
            for direction in ['home', 'away']:
                values = []
                for source_odd in source_odds:
                    if spread_value in source_odd.get('spreads', {}) and direction in source_odd['spreads'][
                        spread_value]:
                        value = source_odd['spreads'][spread_value][direction]
                        # 处理字符串值
                        if isinstance(value, str):
                            try:
                                value = float(value)
                            except ValueError:
                                value = None  # 无法转换则设为 None
                        # 仅添加有效数值
                        if isinstance(value, (int, float)) and value is not None:
                            values.append(value)

                # === 新增：处理正负数逻辑 ===
                if values:
                    has_positive = any(v > 0 for v in values)
                    has_negative = any(v < 0 for v in values)

                    if has_positive and has_negative:  # 同时存在正负数
                        negatives = [v for v in values if v < 0]
                        if negatives:  # 如果有负数，则取负数的最大值
                            match_data['max']['spreads'][spread_value][direction] = max(negatives)
                        else:  # 理论上不会出现（已有负数判断）
                            match_data['max']['spreads'][spread_value][direction] = max(values)
                    else:  # 全正数或全负数，直接取最大值
                        match_data['max']['spreads'][spread_value][direction] = max(values)

        # === 计算大小球的最大值（同理） ===
        for total_value in all_total_values:
            match_data['max']['totals'][total_value] = {
                'over': None,
                'under': None
            }
            for direction in ['over', 'under']:
                values = []
                for source_odd in source_odds:
                    if total_value in source_odd.get('totals', {}) and direction in source_odd['totals'][total_value]:
                        value = source_odd['totals'][total_value][direction]
                        # 处理字符串值
                        if isinstance(value, str):
                            try:
                                value = float(value)
                            except ValueError:
                                value = None  # 无法转换则设为 None
                        # 仅添加有效数值
                        if isinstance(value, (int, float)) and value is not None:
                            values.append(value)

                # === 处理正负数逻辑 ===
                if values:
                    has_positive = any(v > 0 for v in values)
                    has_negative = any(v < 0 for v in values)

                    if has_positive and has_negative:  # 同时存在正负数
                        negatives = [v for v in values if v < 0]
                        if negatives:  # 如果有负数，则取负数的最大值
                            match_data['max']['totals'][total_value][direction] = max(negatives)
                        else:  # 理论上不会出现（已有负数判断）
                            match_data['max']['totals'][total_value][direction] = max(values)
                    else:  # 全正数或全负数，直接取最大值
                        match_data['max']['totals'][total_value][direction] = max(values)

        # === 过滤掉值为 None 的方向 ===
        # 处理让分盘
        spreads = match_data['max']['spreads']
        for spread_value in list(spreads.keys()):
            directions = spreads[spread_value]
            valid_directions = {k: v for k, v in directions.items() if v is not None}
            if valid_directions:
                spreads[spread_value] = valid_directions
            else:
                del spreads[spread_value]

        # 处理大小球（同理）
        totals = match_data['max']['totals']
        for total_value in list(totals.keys()):
            directions = totals[total_value]
            valid_directions = {k: v for k, v in directions.items() if v is not None}
            if valid_directions:
                totals[total_value] = valid_directions
            else:
                del totals[total_value]

    return matches_data


def calculate_is189(match_data: Dict) -> Dict:
    """
    计算189指数，包含0盘口（平手盘）的特殊处理
    修复1和-1盘口匹配问题，确保浮点数转换为字符串时保留整数格式
    改动：计算所有有效盘口，返回最大的189指数值
    """
    source2 = next((s for s in match_data.get("sources", []) if s["source"] == 2), None)
    if not source2 or not source2.get("odds") or not source2["odds"].get("spreads"):
        return {
            "is189": False,
            "result": None,
            "calculation_detail": "数据源2缺失或无让分盘数据",
            "all_calculations": []  # 新增：存储所有计算结果
        }

    spreads = source2["odds"]["spreads"]
    max_result = None
    best_direction = None
    all_calculations = []  # 新增：存储所有计算结果

    for spread_str, directions in spreads.items():
        try:
            home_spread = float(spread_str)
            opposite_spread = -home_spread

            # 关键修改：将浮点数转换为整数格式字符串（如-1.0转为-1）
            if home_spread.is_integer():
                spread_str_clean = f"{int(home_spread)}"
            else:
                spread_str_clean = f"{home_spread}"

            if opposite_spread.is_integer():
                opposite_spread_str = f"{int(opposite_spread)}"
            else:
                opposite_spread_str = f"{opposite_spread}"

            # 处理0盘口
            if home_spread == 0:
                opposite_spread_str = spread_str_clean  # 0的相反数还是0

            if opposite_spread_str not in spreads:
                continue  # 确保客队盘口存在

            # 提取赔率
            home_odd_str = directions.get("home", "0")
            away_odd_str = spreads[opposite_spread_str].get("away", "0")

            # 0盘口兼容处理
            if home_spread == 0 and not away_odd_str:
                away_odd_str = directions.get("away", "0")

            home_odd = float(home_odd_str)
            away_odd = float(away_odd_str)

            if home_odd == 0 or away_odd == 0:
                continue

            h_abs = abs(home_odd)
            a_abs = abs(away_odd)
            abs_diff = max(h_abs, a_abs) - min(h_abs, a_abs)

            calculation_steps = [
                f"盘口: {spread_str_clean}（主队） vs {opposite_spread_str}（客队）",
                f"主队赔率: {home_odd_str}",
                f"客队赔率: {away_odd_str}",
                f"绝对值差: {abs_diff:.3f}",
            ]

            if home_odd * away_odd > 0:
                current_result = (home_odd + away_odd) * 100
            else:
                current_result = (2 - abs_diff) * 100

            current_result_rounded = round(current_result, 2)

            # 新增：记录所有计算结果
            calculation_record = {
                "spread": spread_str_clean,
                "opposite_spread": opposite_spread_str,
                "home_odd": home_odd,
                "away_odd": away_odd,
                "result": current_result_rounded,
                "steps": calculation_steps
            }
            all_calculations.append(calculation_record)

            # 更新最大值
            if max_result is None or current_result_rounded > max_result:
                max_result = current_result_rounded
                best_direction = calculation_steps

        except (ValueError, TypeError):
            continue

    if max_result is not None:
        return {
            "is189": max_result == 189.0,
            "result": max_result,
            "calculation_detail": "\n".join(best_direction) if best_direction else "",
            "all_calculations": all_calculations  # 新增：返回所有计算结果
        }
    else:
        return {
            "is189": False,
            "result": None,
            "calculation_detail": "未找到有效的盘口对（含0盘口）",
            "all_calculations": []
        }


def calculate_total_189(match_data: Dict) -> Dict:
    """
    大小球盘189指数计算（盘口匹配方式独立，计算逻辑与让分盘一致）
    直接使用同一盘口的大球和小球赔率，核心计算逻辑与让分盘完全相同
    改动：计算所有有效盘口，返回最大的189指数值
    """
    source2 = next((s for s in match_data.get("sources", []) if s["source"] == 2), None)
    if not source2 or not source2.get("odds") or not source2["odds"].get("totals"):
        return {
            "is_total_189": False,
            "total_result": None,
            "total_calculation_detail": "数据源2缺失或无大小球盘数据",
            "all_total_calculations": []  # 新增：存储所有计算结果
        }

    totals = source2["odds"]["totals"]
    max_result = None
    best_direction = None
    all_calculations = []  # 新增：存储所有计算结果

    for total_str, directions in totals.items():
        try:
            # 大小球盘口无需计算相反数，直接使用当前盘口
            total_value = float(total_str)

            # 规范化盘口字符串格式（与让分盘一致的处理方式）
            if total_value.is_integer():
                total_str_clean = f"{int(total_value)}"
            else:
                total_str_clean = f"{total_value}"

            # 确保盘口同时包含大球和小球赔率
            if "over" not in directions or "under" not in directions:
                continue

            # 提取赔率（同一盘口的大球和小球赔率）
            over_odd_str = directions.get("over", "0")
            under_odd_str = directions.get("under", "0")

            over_odd = float(over_odd_str)
            under_odd = float(under_odd_str)

            if over_odd == 0 or under_odd == 0:
                continue

            # 计算赔率绝对值（与让分盘一致的处理逻辑）
            h_abs = abs(over_odd)
            a_abs = abs(under_odd)
            abs_diff = max(h_abs, a_abs) - min(h_abs, a_abs)

            calculation_steps = [
                f"大小球盘: {total_str_clean}",
                f"大球赔率: {over_odd_str}",
                f"小球赔率: {under_odd_str}",
                f"绝对值差: {abs_diff:.3f}",
            ]

            # 核心计算逻辑与让分盘完全一致
            if over_odd * under_odd > 0:
                current_result = (over_odd + under_odd) * 100
            else:
                current_result = (2 - abs_diff) * 100

            current_result_rounded = round(current_result, 2)

            # 新增：记录所有计算结果
            calculation_record = {
                "total": total_str_clean,
                "over_odd": over_odd,
                "under_odd": under_odd,
                "result": current_result_rounded,
                "steps": calculation_steps
            }
            all_calculations.append(calculation_record)

            # 更新最大值
            if max_result is None or current_result_rounded > max_result:
                max_result = current_result_rounded
                best_direction = calculation_steps

        except (ValueError, TypeError):
            continue

    if max_result is not None:
        return {
            "is_total_189": max_result == 189.0,
            "total_result": max_result,
            "total_calculation_detail": "\n".join(best_direction) if best_direction else "",
            "all_total_calculations": all_calculations  # 新增：返回所有计算结果
        }
    else:
        return {
            "is_total_189": False,
            "total_result": None,
            "total_calculation_detail": "未找到包含大球和小球赔率的有效盘口",
            "all_total_calculations": []
        }

# === 核心数据处理 ===
@timed
async def process_api_data(results: List[Dict[str, Any]]):
    """处理API数据并生成最终比赛数据（使用唯一键：match_name + start_time_beijing）"""
    print("============================================")
    print("开始处理API数据...")

    all_api_data = {}
    all_api_indexes = {}

    for i, url in enumerate(API_URLS, 1):  # 现在只有两个API
        result = next((r for r in results if r["url"] == url), {"status": "error"})
        if result["status"] == "success":
            all_api_data[i] = result["data"]
            all_api_indexes[i] = create_api_index(result["data"])
            print(f"✅ 从source{i}获取了 {len(result['data'])} 场比赛数据")
        else:
            all_api_data[i] = []
            all_api_indexes[i] = {}
            print(f"❌ 从source{i}获取数据失败: {result.get('error_message', '未知错误')}")

    # 现在以source2为基准数据源
    source2_result = next((r for r in results if r["url"] == API_URLS[1]), None)
    if not source2_result or source2_result["status"] != "success":
        print("❌ 未获取到source2的数据，无法继续处理")
        return None, 0, 0  # 新增：返回默认值避免后续错误

    source2_data = source2_result["data"]
    total_matches_source2 = len(source2_data)
    print(f"📊 source2共有 {total_matches_source2} 场比赛")

    league_groups = defaultdict(list)
    for match in source2_data:
        league_groups[match["league_name"]].append(match)

    print(f"🔍 发现 {len(league_groups)} 个不同的联赛")

    league_bindings_map = batch_fetch_bindings(list(league_groups.keys()))

    # 统计联赛绑定情况
    total_leagues = len(league_groups)
    leagues_with_bindings = sum(
        1 for league in league_groups if league in league_bindings_map and league_bindings_map[league])
    print(f"📊 联赛绑定情况: {leagues_with_bindings}/{total_leagues} 个联赛有绑定数据")

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
    total_matched = len(all_matched_matches)  # 这就是两源匹配成功数

    print(f"============================================")
    print(f"📊 匹配结果汇总:")
    print(f"  - source2总比赛数: {total_matches_source2}")
    print(f"  - 两源匹配成功: {total_matched} ({total_matched / total_matches_source2 * 100:.2f}%)")
    print(f"  - 匹配失败: {total_matches_source2 - total_matched} ({(total_matches_source2 - total_matched) / total_matches_source2 * 100:.2f}%)")
    print(f"============================================")

    # 用于存储所有比赛的数据（使用唯一键：match_name + start_time_beijing）
    all_matches_data = {}

    for match_tuple in all_matched_matches:
        match, team_mapping, matched_apis = match_tuple

        # 提取三个数据源的赔率数据
        source1_odds = matched_apis.get(1, {}).get('odds', {'spreads': {}, 'totals': {}})
        source2_odds = matched_apis.get(2, {}).get('odds', {'spreads': {}, 'totals': {}})
        source3_odds = matched_apis.get(3, {}).get('odds', {'spreads': {}, 'totals': {}})

        # 使用新的交集计算方法
        common_odds = calculate_common_odds(source1_odds, source2_odds, source3_odds)

        # 使用数据源2的名称作为比赛名称
        match_name = f"{team_mapping['league']['source2']} - {team_mapping['home']['source2']} vs {team_mapping['away']['source2']}"

        # ========== 关键修改 ==========
        # 直接从matched_apis获取已匹配的source1/source2数据（无需重新遍历）
        source1_raw_match = matched_apis.get(1, {})
        source2_raw_match = matched_apis.get(2, {})
        # 新增：从SOURCE1提取event_id和line_id
        event_id = source1_raw_match.get('event_id', '')
        line_id = source1_raw_match.get('line_id', '')
        league_id = source1_raw_match.get('league_id', '')

        # 提取start_time_beijing（优先级：source1 → source2 → 当前时间）
        start_time_beijing = source1_raw_match.get('start_time_beijing', '')
        if not start_time_beijing:
            start_time_beijing = source2_raw_match.get('start_time_beijing', '')
        if not start_time_beijing:
            start_time_beijing = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"⚠️ 比赛 {match_name} 的start_time_beijing为空，已用当前时间兜底：{start_time_beijing}")
        # ========== 关键修改结束 ==========

        time_until_start = source1_raw_match.get('time_until_start', '') or source2_raw_match.get('time_until_start',
                                                                                                  '')

        # 构建数据源结构
        source_data = []
        for source_index in sorted(matched_apis.keys()):
            api_match = matched_apis[source_index]
            if source_index == 3:
                home_team = api_match['home_team']
                away_team = api_match['away_team']
                league_name = api_match['league_name']
            else:
                source_key = f"source{source_index}"
                home_team = team_mapping["home"][source_key]
                away_team = team_mapping["away"][source_key]
                league_name = team_mapping["league"][source_key]

            source_entry = {
                "source": source_index,
                "league": league_name,
                "home_team": home_team,
                "away_team": away_team,
                "odds": {
                    "spreads": {},
                    "totals": {}
                }
            }

            # 处理让分盘（仅保留交集部分）
            for spread, directions in common_odds['spreads'].items():
                if spread in api_match.get('odds', {}).get('spreads', {}):
                    filtered_spread = {}
                    spread_data = api_match['odds']['spreads'][spread]  # 获取原始盘口数据

                    # 仅在source1数据中添加altLineId字段
                    if source_index == 1 and 'altLineId' in spread_data:
                        filtered_spread['altLineId'] = spread_data['altLineId']

                    if directions['home'] and 'home' in spread_data:
                        filtered_spread['home'] = spread_data['home']
                    if directions['away'] and 'away' in spread_data:
                        filtered_spread['away'] = spread_data['away']
                    if filtered_spread:  # 确保有数据才添加
                        source_entry['odds']['spreads'][spread] = filtered_spread

            # 处理大小球（仅保留交集部分）
            for total, directions in common_odds['totals'].items():
                if total in api_match.get('odds', {}).get('totals', {}):
                    filtered_total = {}
                    total_data = api_match['odds']['totals'][total]  # 获取原始盘口数据

                    # 仅在source1数据中添加altLineId字段
                    if source_index == 1 and 'altLineId' in total_data:
                        filtered_total['altLineId'] = total_data['altLineId']

                    if directions['over'] and 'over' in total_data:
                        filtered_total['over'] = total_data['over']
                    if directions['under'] and 'under' in total_data:
                        filtered_total['under'] = total_data['under']
                    if filtered_total:
                        source_entry['odds']['totals'][total] = filtered_total

            source_data.append(source_entry)

        # 使用唯一键存储比赛数据（match_name + start_time_beijing）
        unique_key = f"{match_name}-{start_time_beijing}"
        all_matches_data[unique_key] = {
            "match_name": match_name,
            "league_name": team_mapping['league']['source2'],
            "home_team": team_mapping['home']['source2'],
            "away_team": team_mapping['away']['source2'],
            "start_time_beijing": start_time_beijing,
            "time_until_start": time_until_start,
            "event_id": event_id,  # 新增字段
            "line_id": line_id,  # 新增字段
            "league_id": league_id,
            "sources": source_data
        }

    # 在构建完所有比赛数据后计算max字段
    all_matches_data = calculate_odds_max(all_matches_data)

    # 在构建all_matches_data后计算is189
    for match_data in all_matches_data.values():
        is189_data = calculate_is189(match_data)
        # 直接将计算结果合并到match_data中
        match_data.update(is189_data)
        match_data["result"] = is189_data.get("result", None)

        # 新增：计算大小球盘指数
        total_189_data = calculate_total_189(match_data)
        match_data.update(total_189_data)
        match_data["total_result"] = total_189_data.get("total_result", None)
        match_data["is_total_189"] = total_189_data.get("is_total_189", False)

    # 打印最终统计信息（核心：修复除以零错误）
    print(f"============================================")
    print(f"📊 数据处理完成:")
    print(f"  - 总比赛数: {len(all_matches_data)}")
    # 修复：判断total_matched是否为0，避免除以零
    if total_matched > 0:
        print(f"  - 成功率: {len(all_matches_data) / total_matched * 100:.2f}% (基于两源匹配成功数)")
    else:
        print(f"  - 成功率: 0% (两源匹配成功数为0，无法计算)")
    if total_matches_source2 > 0:
        print(f"  - 成功率: {len(all_matches_data) / total_matches_source2 * 100:.2f}% (基于source2总比赛数)")
    else:
        print(f"  - 成功率: 0% (source2无数据)")
    print(f"============================================")

    # 新增：返回三个关键值，供主函数判断状态
    return all_matches_data, total_matched, total_matches_source2


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


# === 修改后的赔率对比函数 ===
def compare_odds(old_data: Dict, new_data: Dict) -> List[Dict]:
    """对比两个比赛的赔率，独立跟踪每个方向的变化（home/away 分离）"""
    changes = []
    old_sources = {s["source"]: s["odds"] for s in old_data.get("sources", [])}
    new_sources = {s["source"]: s["odds"] for s in new_data.get("sources", [])}

    for source_id in set(old_sources.keys()) | set(new_sources.keys()):
        old_odds = old_sources.get(source_id, {})
        new_odds = new_sources.get(source_id, {})

        # 处理让分盘（spread）
        process_odds_direction(changes, old_odds, new_odds, source_id, "spreads", "spread", ["home", "away"])
        # 处理大小球（total）
        process_odds_direction(changes, old_odds, new_odds, source_id, "totals", "total", ["over", "under"])

    return changes


def process_odds_direction(
        changes: List[Dict],
        old_odds: Dict,
        new_odds: Dict,
        source_id: int,
        odds_type: str,
        change_type: str,
        directions: List[str]
):
    """独立处理每个方向的赔率变化（home/away 或 over/under）"""
    old_items = old_odds.get(odds_type, {})
    new_items = new_odds.get(odds_type, {})

    for key in set(old_items.keys()) | set(new_items.keys()):
        old_dir_data = old_items.get(key, {})
        new_dir_data = new_items.get(key, {})

        for direction in directions:
            old_value = old_dir_data.get(direction)
            new_value = new_dir_data.get(direction)

            if old_value != new_value:
                # 确保使用正确的字段名：spread_value 或 total_value
                value_key = f"{change_type}_value"

                changes.append({
                    "type": change_type,
                    "source": source_id,
                    value_key: key,  # 使用 spread_value 或 total_value
                    "side": direction,
                    "old_value": old_value,
                    "new_value": new_value
                })


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
    """更新全局比赛数据缓存（使用唯一键，严格校验数据完整性）"""
    global all_matches_cache

    # 清除无效键（确保键为 match_name-start_time_beijing 格式）
    valid_matches = {}
    for key, data in matches_data.items():
        # 校验键格式（可选：确保键包含分隔符）
        if '-' not in key:
            print(f"⚠️ 无效缓存键 {key}，格式必须为 match_name-start_time_beijing")
            continue
        # 校验数据完整性
        required_fields = ["match_name", "start_time_beijing", "sources"]
        if any(field not in data for field in required_fields):
            print(f"⚠️ 比赛 {key} 缺少必要字段，不加入缓存")
            continue
        valid_matches[key] = data

    # 添加时间戳并更新缓存
    current_time = datetime.now().isoformat()
    for key in valid_matches:
        valid_matches[key]["last_updated"] = current_time

    all_matches_cache = valid_matches
    print(f"✅ 比赛数据缓存已更新，有效数据量: {len(all_matches_cache)}")


# === 新增：WebSocket广播函数（修改为数据更新后调用）===
async def broadcast_matches_data():
    """广播完整比赛数据（直接推送缓存中的值列表）"""
    try:
        if connected_clients and all_matches_cache:
            # 转换为列表时保留完整数据（键已包含在数据中）
            matches_list = list(all_matches_cache.values())

            # 检查数据格式（确保包含前端所需字段）
            if any("start_time_beijing" not in m for m in matches_list):
                print("⚠️ 检测到不完整比赛数据，跳过本次广播")
                return

            data_to_send = {
                "timestamp": datetime.now().isoformat(),
                "matches": matches_list,
                "api_errors": list(current_api_errors),  # 当前失败的API列表
                "connection_count": len(connected_clients)  # 当前WebSocket连接数
            }
            await asyncio.gather(
                *[client.send(json.dumps(data_to_send, default=str)) for client in connected_clients]
            )
            print(f"📢 广播 {len(matches_list)} 场比赛数据")
    except Exception as e:
        print(f"❌ WebSocket广播失败: {e}")


async def ws_handler(websocket, path):
    """处理WebSocket连接"""
    # 添加客户端到连接集合
    connected_clients.add(websocket)
    print(f"✅ 新的WebSocket连接，当前连接数: {len(connected_clients)}")
    await broadcast_matches_data()
    try:
        # 保持连接打开
        await websocket.wait_closed()
    finally:
        # 连接关闭时移除客户端
        connected_clients.remove(websocket)
        print(f"ℹ️ WebSocket连接已关闭，当前连接数: {len(connected_clients)}")

# === 主函数 ===
async def main():
    """主函数：周期性获取所有API数据并通过WebSocket推送更新"""
    global postgres_pool, last_matches_data, current_source1_index  # 新增current_source1_index全局变量

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
            # 首次使用SOURCE1_URLS[0]作为source1
            current_source1_url = SOURCE1_URLS[current_source1_index]
            print(f"📥 首次获取：使用source1 API（索引{current_source1_index}）- {current_source1_url}")
            # 构建任务列表（source1用第一个URL，source2固定）
            tasks = [
                fetch_api(session, current_source1_url),  # source1（第一个API）
                fetch_api(session, SOURCE2_URL)           # source2（固定）
            ]
            results = await asyncio.gather(*tasks)
            # 更新API_URLS确保后续逻辑兼容
            API_URLS[0] = current_source1_url

            current_source1_index = (current_source1_index + 1) % len(SOURCE1_URLS)  # 0→1→2→0循环

            if check_api_failures(results):
                print(f"⚠️ 程序将暂停 {API_FAILURE_DELAY} 秒后继续运行...")
                await asyncio.sleep(API_FAILURE_DELAY)
                return

            # 首次处理数据：接收三个返回值
            all_matches_data, total_matched, total_matches_source2 = await process_api_data(results)

        if all_matches_data and len(all_matches_data) > 0:
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
                                    "spread_value": spread_key,
                                    "side": side,
                                    "old_value": None,
                                    "new_value": value
                                })
                        for total_key, total_data in source.get("odds", {}).get("totals", {}).items():
                            for side, value in total_data.items():
                                dummy_changes.append({
                                    "type": "total",
                                    "source": source_id,
                                    "total_value": total_key,
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

            # 初始数据加载后立即广播
            await broadcast_matches_data()

            print(f"\n✅ 初始数据保存完成，共 {len(all_matches_data)} 场比赛")
        else:
            print("ℹ️ 初始数据为空，程序将继续运行但无数据可保存")

        # 主循环：周期性数据处理
        print(f"\n{'=' * 20} 开始周期性数据获取 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'=' * 20}")

        # 新增：控制等待间隔（默认1秒，无匹配时5分钟）
        fetch_interval = 10

        while True:
            try:
                start_time = time.time()
                print(f"\n{'=' * 20} 开始新一轮数据获取 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'=' * 20}")

                # 获取最新数据（轮换source1 API）
                async with aiohttp.ClientSession() as session:
                    # 根据当前索引选择source1 URL
                    current_source1_url = SOURCE1_URLS[current_source1_index]
                    print(f"📥 本轮获取：使用source1 API（索引{current_source1_index}）- {current_source1_url}")
                    # 构建任务列表（source1轮换，source2固定）
                    tasks = [
                        fetch_api(session, current_source1_url),  # 轮换的source1
                        fetch_api(session, SOURCE2_URL)           # 固定的source2
                    ]
                    results = await asyncio.gather(*tasks)
                    # 更新API_URLS确保后续逻辑兼容
                    API_URLS[0] = current_source1_url

                    current_source1_index = (current_source1_index + 1) % len(SOURCE1_URLS)  # 核心修改

                global current_api_errors
                current_api_errors = {result["url"] for result in results if result["status"] == "error"}

                # 无论是否失败，都广播当前状态
                await broadcast_matches_data()  # 提前广播状态，确保错误及时显示

                if check_api_failures(results):
                    print(f"⚠️ 检测到API请求失败，跳过此轮数据处理")
                    await asyncio.sleep(fetch_interval)
                    continue

                # 核心：接收处理后的数据和匹配数（total_matched是关键）
                all_matches_data, total_matched, total_matches_source2 = await process_api_data(results)

                # 核心逻辑：如果两源匹配成功数为0，进入5分钟待机
                if total_matched == 0:
                    print(f"⚠️ 两源匹配成功数为0，进入待机状态（每5分钟重试一次）")
                    fetch_interval = 300  # 切换为5分钟
                    await asyncio.sleep(fetch_interval)
                    continue
                else:
                    # 有匹配成功的数据，恢复正常间隔（1秒）
                    fetch_interval = 10

                if not all_matches_data:
                    print("ℹ️ 本轮获取的比赛数据为空")
                    await asyncio.sleep(fetch_interval)
                    continue

                # 数据对比与变化检测（保持不变）
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
                                            "spread_value": spread_key,
                                            "side": side,
                                            "old_value": None,
                                            "new_value": value
                                        })
                                for total_key, total_data in source.get("odds", {}).get("totals", {}).items():
                                    for side, value in total_data.items():
                                        initial_changes.append({
                                            "type": "total",
                                            "source": source_id,
                                            "total_value": total_key,
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

                # 数据更新完成后立即广播
                await broadcast_matches_data()

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
                                    f"    🔹 数据源{change['source']} 让分盘 {change['spread_value']} - {change['side']}: {change['old_value']} → {change['new_value']}")
                            else:
                                print(
                                    f"    🔹 数据源{change['source']} 大小球 {change['total_value']} - {change['side']}: {change['old_value']} → {change['new_value']}")

                # 打印移除比赛
                if removed_matches:
                    print("\n❌ 移除比赛:")
                    for match_name in removed_matches:
                        print(f"  - {match_name}")

                if not new_matches and not changed_matches and not removed_matches:
                    print("\nℹ️ 无数据变化")

                # 计算处理时间和下一次获取时间
                elapsed = time.time() - start_time
                print(f"\n{'=' * 50}")
                print(f"📊 本轮数据处理完成")
                print(f"  - 处理时间: {elapsed:.2f}秒")
                print(f"  - 下次数据获取将在{fetch_interval}秒后进行")
                print(f"{'=' * 50}\n")

                # 等待下一个周期
                await asyncio.sleep(fetch_interval)

            except Exception as e:
                print(f"❌ 周期数据获取异常: {e}")
                # 记录完整堆栈跟踪
                import traceback
                traceback.print_exc()
                # 等待一段时间再重试
                await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("\n👋 用户手动终止程序")
    finally:
        # 资源清理
        if 'ws_server' in locals():
            ws_server.close()
            await ws_server.wait_closed()

        # 关闭数据库连接池
        if postgres_pool:
            postgres_pool.closeall()
            print("✅ 数据库连接池已关闭")

        print("👋 程序已退出")


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())