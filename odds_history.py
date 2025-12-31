import json
import os
import uvicorn
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Tuple, Set, Union
import psycopg2
from psycopg2.extras import DictCursor
import logging
import time
from datetime import datetime, timedelta
import threading
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# 创建FastAPI应用
app = FastAPI(title="体育赔率历史数据API")



# 配置CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}


# 新增配置模型字段
class MonitorConfig(BaseModel):
    check_interval: Optional[int] = None
    consecutive_decreases: Optional[int] = None
    time_window: Optional[int] = None
    enabled: Optional[bool] = None
    min_odds: Optional[float] = 0.75  # 最低赔率值（正数需≥此值，负数无限制）
    max_odds: Optional[float] = -0.85  # 最高赔率值（负数需≤此值，正数无限制）
    # 新增：需要检查的数据源列表，默认为[1,2,3]
    required_sources: Optional[List[int]] = [2]
    # 新增：189指数阈值配置
    threshold_189: Optional[float] = 189.0  # 189指数阈值

    # 新增点位监控参数
    point_monitor_enabled: Optional[bool] = True  # 点位监控是否启用
    point_check_minutes: Optional[int] = 30  # 检查的时间窗口(分钟)
    point_threshold: Optional[float] = 30  # 触发警报的跌幅点位阈值
    point_monitor_sources: Optional[List[int]] = [1, 2]  # 点位监控的数据源


# 更新监控配置
MONITOR_CONFIG = {
    "check_interval": 5,
    "consecutive_decreases": 1,
    "time_window": 2,
    "enabled": False,
    "min_odds": 0.8,
    "max_odds": -0.85,
    # 新增配置项
    "required_sources": [1, 2],
    # 新增：189指数阈值
    "threshold_189": 188.0,

    # 新增点位监控配置
    "point_monitor_enabled": False,
    "point_check_minutes": 30,
    "point_threshold": 30,
    "point_monitor_sources": [1]
}


# 数据模型
class OddsRecord(BaseModel):
    source: int
    odds: float
    time: str


class HistoryResponse(BaseModel):
    status: str
    data: Optional[List[OddsRecord]] = None
    message: Optional[str] = None


class WarningMessage(BaseModel):
    match_name: str
    start_time_beijing: str
    type: str
    value: str
    side: str
    warning_time: str
    sources: List[int]
    # 新增字段（从matches表获取）
    league_name: str  # 联赛名称
    home_team: str  # 主队名称
    away_team: str  # 客队名称
    result_value: str  # 比赛结果值（如比分）


# 新增点位监控的警告消息模型
class PointWarningMessage(WarningMessage):
    time_window: int  # 监控的时间窗口(分钟)
    drop_points: float  # 实际下跌的点位
    threshold_points: float  # 触发警报的点位阈值
    previous_odds: float  # 之前的赔率
    current_odds: float  # 当前的赔率


# 首先更新数据模型以包含新字段
class DailyMatchOdds(BaseModel):
    """单场比赛的所有盘口赔率记录"""
    match_id: int
    match_name: str
    league_name: str
    home_team: str
    away_team: str
    start_time_beijing: str
    # 新增比赛结果字段
    full_time: Optional[str] = None  # 全场比赛结果
    half_time: Optional[str] = None  # 半场比赛结果
    spread_odds: List[Dict]  # 让分盘数据，每个元素对应一个盘口
    total_odds: List[Dict]  # 大小球盘数据，每个元素对应一个盘口



# 更新响应模型以支持日期区间
class DailyOddsResponse(BaseModel):
    """日期区间所有比赛盘口赔率响应"""
    status: str
    start_date: str  # 原date字段改为start_date
    end_date: str    # 新增end_date字段
    count: int       # 区间内比赛总数
    data: Optional[List[DailyMatchOdds]] = None
    message: Optional[str] = None


# 全局缓存 - 存储当前警告
CURRENT_WARNINGS = []
WARNING_LOCK = threading.Lock()  # 用于线程安全的锁
# 在全局变量区域添加（如CURRENT_WARNINGS下方）
# 存储已触发警告的唯一标识：{match_id_type_value_side_source}
TRIGGERED_WARNINGS = set()
TRIGGER_LOCK = threading.Lock()  # 线程安全锁

def get_unique_warning_key(match_id: int, odds_type: str, value: str, side: str, source_id: int) -> str:
    """生成警告的唯一标识：比赛ID+盘口类型+盘口值+方向+数据源"""
    return f"{match_id}_{odds_type}_{value}_{side}_{source_id}"

# 首先在数据库配置后添加新的表创建函数（首次运行时执行）
# 修改初始化点位警告表的函数
def init_point_warnings_table():
    """初始化点位警告存储表（确保唯一约束正确创建）"""
    conn = get_db_connection()
    if not conn:
        logger.error("数据库连接失败，无法初始化点位警告表")
        return

    try:
        with conn.cursor() as cursor:
            # 1. 先检查并删除旧表（如果存在且结构不兼容）
            cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.table_constraints 
                WHERE table_name = 'point_warnings' 
                AND constraint_name = 'unique_point_warning'
            )
            """)
            constraint_exists = cursor.fetchone()[0]

            # 如果表存在但约束不存在，删除表重新创建
            if not constraint_exists:
                cursor.execute("DROP TABLE IF EXISTS point_warnings")
                logger.warning("已删除旧的point_warnings表，将重新创建")

            # 2. 创建新表（包含正确的唯一约束）
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS point_warnings (
                id SERIAL PRIMARY KEY,
                match_name VARCHAR(255) NOT NULL,
                start_time_beijing TIMESTAMP NOT NULL,
                type VARCHAR(50) NOT NULL,
                value VARCHAR(50) NOT NULL,
                side VARCHAR(50) NOT NULL,
                warning_time TIMESTAMP NOT NULL,
                sources JSONB NOT NULL,
                league_name VARCHAR(100) NOT NULL,
                home_team VARCHAR(100) NOT NULL,
                away_team VARCHAR(100) NOT NULL,
                result_value VARCHAR(100) NOT NULL,
                time_window INTEGER NOT NULL,
                drop_points NUMERIC(10, 2) NOT NULL,
                threshold_points NUMERIC(10, 2) NOT NULL,
                previous_odds NUMERIC(10, 3) NOT NULL,
                current_odds NUMERIC(10, 3) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- 生成列：提取sources数组的第一个元素（数据源ID）
                source_first INTEGER GENERATED ALWAYS AS (
                    (sources::jsonb ->> 0)::integer
                ) STORED,
                -- 创建唯一约束
                CONSTRAINT unique_point_warning UNIQUE (
                    match_name, type, value, side, source_first
                )
            )
            """)

            # 3. 验证约束是否创建成功
            cursor.execute("""
            SELECT 1 
            FROM information_schema.table_constraints 
            WHERE table_name = 'point_warnings' 
            AND constraint_name = 'unique_point_warning'
            """)
            if cursor.fetchone():
                conn.commit()
                logger.info("点位警告表及唯一约束创建成功")
            else:
                raise Exception("创建unique_point_warning约束失败")

    except Exception as e:
        logger.error(f"初始化点位警告表失败: {str(e)}", exc_info=True)
        conn.rollback()
    finally:
        if conn:
            conn.close()


# 添加一个手动修复约束的函数（可临时调用）
def repair_point_warnings_constraint():
    """手动修复point_warnings表的唯一约束"""
    conn = get_db_connection()
    if not conn:
        logger.error("数据库连接失败，无法修复约束")
        return False

    try:
        with conn.cursor() as cursor:
            # 检查约束是否存在
            cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.table_constraints 
                WHERE table_name = 'point_warnings' 
                AND constraint_name = 'unique_point_warning'
            )
            """)
            if not cursor.fetchone()[0]:
                # 添加约束
                cursor.execute("""
                ALTER TABLE point_warnings
                ADD CONSTRAINT unique_point_warning UNIQUE (
                    match_name, type, value, side, source_first
                )
                """)
                conn.commit()
                logger.info("成功修复unique_point_warning约束")
                return True
            else:
                logger.info("unique_point_warning约束已存在，无需修复")
                return True
    except Exception as e:
        logger.error(f"修复约束失败: {str(e)}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# 新增：将点位警告存入数据库的函数
def save_point_warning_to_db(warning: PointWarningMessage):
    """将点位警告信息存入数据库（忽略重复）"""
    conn = get_db_connection()
    if not conn:
        logger.error("数据库连接失败，无法保存点位警告")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO point_warnings (
                match_name, start_time_beijing, type, value, side, warning_time,
                sources, league_name, home_team, away_team, result_value,
                time_window, drop_points, threshold_points, previous_odds, current_odds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            -- 重复时不执行任何操作
            ON CONFLICT ON CONSTRAINT unique_point_warning DO NOTHING
            """, (
                warning.match_name,
                warning.start_time_beijing,
                warning.type,
                warning.value,
                warning.side,
                warning.warning_time,
                json.dumps(warning.sources),
                warning.league_name,
                warning.home_team,
                warning.away_team,
                warning.result_value,
                warning.time_window,
                warning.drop_points,
                warning.threshold_points,
                warning.previous_odds,
                warning.current_odds
            ))
            conn.commit()
            # 仅在成功插入时打印日志
            if cursor.rowcount > 0:
                logger.info(f"点位警告已保存到数据库: {warning.match_name}")
            else:
                logger.info(f"点位警告已存在（数据库去重）: {warning.match_name}")
    except Exception as e:
        logger.error(f"保存点位警告到数据库失败: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()


def load_point_warnings_from_db() -> List[PointWarningMessage]:
    """从数据库加载点位警告信息"""
    conn = get_db_connection()
    if not conn:
        logger.error("数据库连接失败，无法加载点位警告")
        return []

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 查询最近24小时的点位警告
            cursor.execute("""
            SELECT * FROM point_warnings
            WHERE created_at >= NOW() - INTERVAL '48 hours'
            ORDER BY warning_time DESC
            """)
            rows = cursor.fetchall()

            warnings = []
            for row in rows:
                # 修复sources字段解析问题
                sources_data = row["sources"]
                # 检查数据类型，如果是字符串则解析为JSON，否则直接使用（如果已经是列表）
                if isinstance(sources_data, str):
                    sources = json.loads(sources_data)
                else:
                    sources = sources_data  # 如果已经是列表则直接使用

                warnings.append(PointWarningMessage(
                    match_name=row["match_name"],
                    start_time_beijing=row["start_time_beijing"].strftime("%Y-%m-%d %H:%M:%S"),
                    type=row["type"],
                    value=row["value"],
                    side=row["side"],
                    warning_time=row["warning_time"].strftime("%Y-%m-%d %H:%M:%S"),
                    sources=sources,  # 使用处理后的sources数据
                    league_name=row["league_name"],
                    home_team=row["home_team"],
                    away_team=row["away_team"],
                    result_value=row["result_value"],
                    time_window=row["time_window"],
                    drop_points=float(row["drop_points"]),
                    threshold_points=float(row["threshold_points"]),
                    previous_odds=float(row["previous_odds"]),
                    current_odds=float(row["current_odds"])
                ))
            return warnings
    except Exception as e:
        logger.error(f"从数据库加载点位警告失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


# 数据库连接工具函数
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return None


# 新增：连续相同赔率去重函数
def deduplicate_consecutive_odds(records):
    """
    仅对连续相同的赔率去重，保留最新一条；非连续的相同赔率全部保留
    :param records: 按时间升序排列的记录列表（每条含"odds"和"time"字段）
    :return: 去重后的记录列表（按时间升序）
    """
    if not records:
        return []

    deduplicated = [records[0]]  # 初始化，保留第一条记录

    for i in range(1, len(records)):
        current = records[i]
        prev = deduplicated[-1]  # 取上一条已保留的记录

        # 如果当前赔率与上一条相同（连续相同），则替换上一条为当前（保留最新）
        if current["odds"] == prev["odds"]:
            deduplicated[-1] = current
        else:
            # 不同则直接保留
            deduplicated.append(current)

    return deduplicated


# API路由 - 历史赔率查询（核心修改：去重逻辑）
@app.get("/api/odds-history", response_model=HistoryResponse)
async def get_odds_history(
        match_name: str = Query(..., description="完整比赛名称，格式：联赛 - 主队 vs 客队-日期时间"),
        start_time_beijing: str = Query(..., description="比赛开始时间（北京时间）"),
        type: str = Query(..., enum=["spread", "total"], description="盘口类型"),
        value: str = Query(..., description="盘口值"),
        side: str = Query(..., enum=["home", "away", "over", "under"], description="投注方向")
):
    """查询指定盘口的历史赔率记录"""
    logger.info(f"收到历史赔率查询请求: match_name={match_name}, start_time={start_time_beijing}")

    conn = get_db_connection()
    if not conn:
        return {"status": "error", "message": "数据库连接失败"}

    try:
        # 确定表名和字段名
        table = "spread_odds" if type == "spread" else "total_odds"
        field = "spread_value" if type == "spread" else "total_value"

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 查询比赛ID
            cursor.execute("""
            SELECT id FROM matches 
            WHERE match_name = %s
              AND start_time_beijing = %s
            """, (match_name, start_time_beijing))

            match = cursor.fetchone()

            if not match:
                logger.warning(f"未找到匹配比赛: match_name={match_name}, start_time={start_time_beijing}")
                return {"status": "error", "message": "比赛不存在"}

            match_id = match["id"]
            logger.info(f"找到比赛ID: {match_id}")

            # 为每个source分别查询并去重（核心修改）
            all_records = []
            for source_id in [1, 2, 3]:
                # 查询时按时间升序排列（便于连续去重）
                cursor.execute(f"""
                SELECT source, odds_value, recorded_at
                FROM {table}
                WHERE match_id = %s
                  AND {field} = %s
                  AND side = %s
                  AND source = %s
                ORDER BY recorded_at ASC  -- 改为升序排列
                """, (match_id, value, side, source_id))

                # 收集原始记录（保留datetime对象用于排序）
                source_records = []
                for row in cursor.fetchall():
                    odds = row["odds_value"]
                    if odds is None:
                        logger.warning(f"忽略无效赔率值 (source={source_id}, match_id={match_id})")
                        continue
                    source_records.append({
                        "source": row["source"],
                        "odds": odds,
                        "time": row["recorded_at"]  # 保留datetime类型
                    })

                # 使用新的连续去重函数
                deduplicated_records = deduplicate_consecutive_odds(source_records)

                # 转换时间格式并限制数量（按时间倒序排列，最新的在前）
                formatted_records = [
                    {
                        "source": r["source"],
                        "odds": r["odds"],
                        "time": r["time"].strftime("%Y-%m-%d %H:%M:%S")
                    }
                    for r in sorted(deduplicated_records, key=lambda x: x["time"], reverse=True)[:200]
                ]

                all_records.extend(formatted_records)

        logger.info(f"成功查询到 {len(all_records)} 条去重后的历史记录")
        return {"status": "success", "data": all_records}

    except Exception as e:
        logger.error(f"查询历史赔率失败: {e}")
        return {"status": "error", "message": "查询失败"}

    finally:
        if conn:
            conn.close()


@app.get("/api/latest-odds-source2", response_model=HistoryResponse)
async def get_latest_odds_source2(
        match_name: str = Query(..., description="完整比赛名称，格式：联赛 - 主队 vs 客队-日期时间"),
        start_time_beijing: str = Query(..., description="比赛开始时间（北京时间）"),
        type: str = Query(..., enum=["spread", "total"], description="盘口类型"),
        value: str = Query(..., description="盘口值"),
        side: str = Query(..., enum=["home", "away", "over", "under"], description="投注方向")
):
    """查询指定盘口的数据源2最新赔率记录"""
    logger.info(f"收到数据源2最新赔率查询请求: match_name={match_name}, start_time={start_time_beijing}")

    conn = get_db_connection()
    if not conn:
        return {"status": "error", "message": "数据库连接失败"}

    try:
        # 确定表名和字段名
        table = "spread_odds" if type == "spread" else "total_odds"
        field = "spread_value" if type == "spread" else "total_value"

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 查询比赛ID
            cursor.execute("""
            SELECT id FROM matches 
            WHERE match_name = %s
              AND start_time_beijing = %s
            """, (match_name, start_time_beijing))

            match = cursor.fetchone()

            if not match:
                logger.warning(f"未找到匹配比赛: match_name={match_name}, start_time={start_time_beijing}")
                return {"status": "error", "message": "比赛不存在"}

            match_id = match["id"]
            logger.info(f"找到比赛ID: {match_id}")

            # 只查询数据源2的记录
            source_id = 2

            # 查询时按时间升序排列（便于连续去重）
            cursor.execute(f"""
            SELECT source, odds_value, recorded_at
            FROM {table}
            WHERE match_id = %s
              AND {field} = %s
              AND side = %s
              AND source = %s
            ORDER BY recorded_at ASC
            """, (match_id, value, side, source_id))

            # 收集原始记录（保留datetime对象用于排序）
            source_records = []
            for row in cursor.fetchall():
                odds = row["odds_value"]
                if odds is None:
                    logger.warning(f"忽略无效赔率值 (source={source_id}, match_id={match_id})")
                    continue
                source_records.append({
                    "source": row["source"],
                    "odds": odds,
                    "time": row["recorded_at"]  # 保留datetime类型
                })

            # 使用连续去重函数
            deduplicated_records = deduplicate_consecutive_odds(source_records)

            # 转换时间格式并只保留最新的一条记录
            formatted_records = []
            if deduplicated_records:
                # 按时间倒序排列，取第一条（最新的）
                latest_record = sorted(deduplicated_records, key=lambda x: x["time"], reverse=True)[0]
                formatted_records.append({
                    "source": latest_record["source"],
                    "odds": latest_record["odds"],
                    "time": latest_record["time"].strftime("%Y-%m-%d %H:%M:%S")
                })

        logger.info(f"成功查询到数据源2的最新赔率记录: {len(formatted_records)} 条")
        return {"status": "success", "data": formatted_records}

    except Exception as e:
        logger.error(f"查询数据源2最新赔率失败: {e}")
        return {"status": "error", "message": "查询失败"}

    finally:
        if conn:
            conn.close()


# 新增：简化版批量查询比赛开赛时间接口
@app.post("/api/match-start-time/simple")
async def simple_batch_query_match_start_time(queries: List[dict]):
    """修复时间格式处理，确保与调用方一致"""
    logger.info(f"收到简化版批量查询开赛时间请求，共 {len(queries)} 条")

    conn = get_db_connection()
    if not conn:
        return {"status": "error", "message": "数据库连接失败", "results": []}

    results = []

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            for query in queries:
                try:
                    league = query.get("league_name")
                    home = query.get("home_team")
                    away = query.get("away_team")
                    order_time = query.get("order_time")

                    if not all([league, home, away, order_time]):
                        results.append({
                            **query,
                            "match_start_time": None,
                            "status": "invalid_params"
                        })
                        continue

                    # 修复：兼容带毫秒的时间格式（与调用方一致）
                    try:
                        # 尝试解析带毫秒的时间（%Y-%m-%d %H:%M:%S.%f）
                        order_datetime = datetime.strptime(order_time, "%Y-%m-%d %H:%M:%S.%f")
                    except ValueError:
                        # 失败则尝试解析不带毫秒的时间（兼容容错）
                        order_datetime = datetime.strptime(order_time, "%Y-%m-%d %H:%M:%S")

                    # 查询符合条件的比赛（开赛时间 > 订单时间）
                    cursor.execute("""
                    SELECT start_time_beijing 
                    FROM matches 
                    WHERE league_name = %s
                      AND home_team = %s
                      AND away_team = %s
                      AND start_time_beijing::timestamp > %s
                    ORDER BY start_time_beijing::timestamp ASC
                    """, (league, home, away, order_datetime))

                    matches = cursor.fetchall()

                    result = {**query,
                              "match_start_time": None,
                              "status": "not_found"
                              }

                    if matches:
                        # 取第一个最接近的比赛时间
                        result["match_start_time"] = matches[0]["start_time_beijing"]
                        result["status"] = "success"
                        if len(matches) > 1:
                            result["status"] = "multiple_matches"

                    results.append(result)

                except ValueError as e:
                    # 时间格式错误（无论是带不带毫秒）
                    results.append({
                        **query,
                        "match_start_time": None,
                        "status": "invalid_time_format"
                    })
                    logger.warning(f"时间格式错误: {str(e)}, query: {query}")
                except Exception as e:
                    logger.error(f"处理查询项失败: {e}")
                    results.append({**query,
                                    "match_start_time": None,
                                    "status": "error"
                                    })

        return {"status": "success", "results": results}

    except Exception as e:
        logger.error(f"批量查询开赛时间失败: {e}")
        return {"status": "error", "message": str(e), "results": []}

    finally:
        if conn:
            conn.close()


@app.get("/api/debug/matches")
async def debug_matches(
        search: str = Query(..., description="搜索关键词")
):
    """搜索匹配的比赛名称（调试用）"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("""
            SELECT id, match_name, start_time_beijing 
            FROM matches 
            WHERE match_name ILIKE %s
            LIMIT 20
            """, (f"%{search}%",))

            return {"status": "success", "data": cursor.fetchall()}
    finally:
        conn.close()


# 监控系统 - 获取所有赔率数据
def get_all_odds_data(match_id: int, type: str, value: str, side: str) -> Dict[int, List[Dict]]:
    """获取指定比赛和盘口的所有赔率数据"""
    table = "spread_odds" if type == "spread" else "total_odds"
    field = "spread_value" if type == "spread" else "total_value"

    conn = get_db_connection()
    if not conn:
        logger.error(f"数据库连接失败，无法获取赔率数据 (match_id={match_id})")
        return {}

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 查询所有数据源的赔率数据
            cursor.execute(f"""
            SELECT source, odds_value, recorded_at
            FROM {table}
            WHERE match_id = %s
              AND {field} = %s
              AND side = %s
            ORDER BY source, recorded_at DESC
            """, (match_id, value, side))

            rows = cursor.fetchall()

            # 按数据源分组
            source_data = {}
            for row in rows:
                source = row["source"]
                if source not in source_data:
                    source_data[source] = []
                source_data[source].append({
                    "odds": row["odds_value"],
                    "time": row["recorded_at"]
                })

            return source_data

    except Exception as e:
        logger.error(f"获取所有赔率数据失败: {e}")
        return {}
    finally:
        if conn:
            conn.close()


# 监控系统 - 分析赔率趋势
def analyze_odds_trend(odds_data: List[Dict], consecutive_decreases: int) -> bool:
    """分析赔率趋势，判断是否连续下降指定次数（马来盘逻辑）"""
    # 过滤掉None值的赔率记录
    valid_odds = [record for record in odds_data if record["odds"] is not None]

    if len(valid_odds) < consecutive_decreases:
        return False

    # 按时间排序（最新的在前）
    sorted_data = sorted(valid_odds, key=lambda x: x["time"], reverse=True)

    # 检查是否连续下降
    for i in range(consecutive_decreases):
        if i + 1 >= len(sorted_data):
            return False

        # 获取当前和前一个赔率
        new_odds = sorted_data[i]["odds"]
        old_odds = sorted_data[i + 1]["odds"]

        # 转换为十进制赔率再比较
        new_decimal = malay_to_decimal(new_odds)
        old_decimal = malay_to_decimal(old_odds)

        if new_decimal >= old_decimal:  # 十进制赔率未下降
            return False

    return True


def malay_to_decimal(malay_odds: float) -> float:
    """将马来盘赔率转换为十进制赔率"""
    if malay_odds >= 0:
        return 1 + malay_odds  # 正数马来赔率直接加1
    else:
        return 1 + (1 / abs(malay_odds))  # 负数马来赔率按公式转换


# 新增：马来盘点位计算函数
def calculate_malay_points(old_odds: float, new_odds: float) -> float:
    """
    计算马来盘赔率变化的点位
    正数表示下跌，负数表示上涨
    """
    # 处理无效赔率
    if old_odds is None or new_odds is None:
        return 0.0

    try:
        # 转换为浮点数
        old = float(old_odds)
        new = float(new_odds)

        # 情况1: 负数到正数（下跌）
        if old < 0 and new >= 0:
            return (100 - abs(old * 100)) + (100 - (new * 100))

        # 情况2: 都为负数（-0.6到-0.9为跌幅30点位）
        elif old < 0 and new < 0:
            return abs(new * 100) - abs(old * 100)

        # 情况3: 都为正数（0.9到0.6跌幅为30个点位）
        elif old >= 0 and new >= 0:
            return (old * 100) - (new * 100)

        # 情况4: 正数到负数（上涨）
        else:  # old >= 0 and new < 0
            return 0.0

    except (ValueError, TypeError):
        return 0.0


# 新增：赔率点位跌幅分析函数
def analyze_odds_point_drop(odds_data: List[Dict], time_window_minutes: int) -> Tuple[bool, float, float, float]:
    """
    分析赔率在指定时间窗口内的跌幅是否超过阈值
    :param odds_data: 赔率数据列表
    :param time_window_minutes: 时间窗口(分钟)
    :return: (是否超过阈值, 跌幅点位, 之前的赔率, 当前的赔率)
    """
    # 过滤无效数据
    valid_odds = [record for record in odds_data if record["odds"] is not None]
    if len(valid_odds) < 2:
        return (False, 0.0, 0.0, 0.0)

    # 按时间排序（最新的在前）
    sorted_data = sorted(valid_odds, key=lambda x: x["time"], reverse=True)
    current_time = sorted_data[0]["time"]
    current_odds = sorted_data[0]["odds"]

    # 计算时间窗口的起始时间
    window_start_time = current_time - timedelta(minutes=time_window_minutes)

    # 找到时间窗口内最早的赔率记录
    previous_odds = None
    for record in sorted_data:
        if record["time"] <= window_start_time:
            previous_odds = record["odds"]
            break

    # 如果没有找到时间窗口内的记录，使用最早的记录
    if previous_odds is None:
        return (False, 0.0, 0.0, 0.0)  # 无有效对比数据，不触发警告

    # 计算跌幅点位
    drop_points = calculate_malay_points(previous_odds, current_odds)

    # 返回结果
    return (True, drop_points, previous_odds, current_odds)


# 修改：检查未开始比赛的函数（包含点位监控逻辑）
def check_active_matches():
    """检查所有未开始的比赛（开始时间 > 当前时间），看是否有赔率下降警告"""
    conn = get_db_connection()
    if not conn:
        logger.error("数据库连接失败，无法检查未开始的比赛")
        return []

    try:
        now = datetime.now()  # 获取当前时间（北京时间）
        # 下限：当前时间 + time_window（例如配置为2，则表示2小时后）
        start_time_threshold = now + timedelta(hours=MONITOR_CONFIG["time_window"])

        # 上限：固定为12小时（无论time_window配置多少，上限都是12小时后）
        end_time_threshold = now + timedelta(hours=12)

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 查询开始时间在 [now, now+time_window] 内的未开始比赛
            cursor.execute("""
            SELECT id, match_name, start_time_beijing, 
                   league_name, home_team, away_team, result_value
            FROM matches 
            WHERE start_time_beijing::timestamp >= %s  
              AND start_time_beijing::timestamp < %s   
            """, (start_time_threshold, end_time_threshold))

            matches = cursor.fetchall()

            warnings = []

            # 检查每种盘口类型
            for match in matches:
                match_id = match["id"]
                match_name = match["match_name"]
                start_time = match["start_time_beijing"]
                league_name = match["league_name"]
                home_team = match["home_team"]
                away_team = match["away_team"]

                # 检查让分盘和大小球盘
                check_types = [
                    {"type": "spread", "table": "spread_odds", "field": "spread_value"},
                    {"type": "total", "table": "total_odds", "field": "total_value"}
                ]

                for check_type in check_types:
                    # 获取该比赛的所有盘口值和投注方向
                    cursor.execute(f"""
                    SELECT DISTINCT {check_type["field"]} as value, side 
                    FROM {check_type["table"]}
                    WHERE match_id = %s
                    """, (match_id,))

                    lines = cursor.fetchall()

                    for line in lines:
                        value = line["value"]
                        side = line["side"]

                        # 获取source2的原始赔率数据（用于计算具体盘口的189指数）
                        cursor.execute(f"""
                        SELECT {check_type["field"]} as value, side, odds_value 
                        FROM {check_type["table"]}
                        WHERE match_id = %s AND source = 2  -- 只取source2的数据
                        """, (match_id,))
                        source2_odds_rows = cursor.fetchall()

                        # 格式化source2数据为字典（{盘口值: {方向: 赔率}}）
                        source2_odds = defaultdict(dict)
                        for row in source2_odds_rows:
                            v = row["value"]
                            s = row["side"]
                            source2_odds[v][s] = row["odds_value"]

                        # 计算当前盘口的189指数
                        current_189 = -1
                        if check_type["type"] == "spread":
                            current_189 = calculate_single_spread_189(source2_odds, value, side)
                        else:
                            current_189 = calculate_single_total_189(source2_odds, value, side)

                        # 获取所有赔率数据
                        all_data = get_all_odds_data(
                            match_id,
                            check_type["type"],
                            value,
                            side
                        )

                        # 1. 原有连续下降监控逻辑
                        required_sources = MONITOR_CONFIG["required_sources"]
                        decreasing_sources = []
                        for source_id in required_sources:
                            if source_id in all_data and analyze_odds_trend(
                                    all_data[source_id],
                                    MONITOR_CONFIG["consecutive_decreases"]
                            ):
                                decreasing_sources.append(source_id)

                        # 检查赔率范围
                        if 2 in all_data and all_data[2]:
                            latest_odds = all_data[2][0]["odds"]  # 取source=2的最新赔率
                            if latest_odds is not None:
                                # 判断是否符合赔率范围
                                odds_in_range = False
                                if latest_odds >= 0 and latest_odds >= MONITOR_CONFIG["min_odds"]:
                                    odds_in_range = True
                                elif latest_odds < 0 and latest_odds <= MONITOR_CONFIG["max_odds"]:
                                    odds_in_range = True

                                # 过滤：仅保留当前盘口189指数≥阈值的情况
                                if current_189 >= MONITOR_CONFIG["threshold_189"]:
                                    # 生成连续下降警告
                                    if odds_in_range and len(decreasing_sources) == len(required_sources):
                                        warning = WarningMessage(
                                            match_name=match_name,
                                            start_time_beijing=start_time,
                                            type=check_type["type"],
                                            value=value,
                                            side=side,
                                            warning_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            sources=decreasing_sources,
                                            league_name=league_name,
                                            home_team=home_team,
                                            away_team=away_team,
                                            result_value=str(current_189)  # 显示当前盘口的189指数
                                        )
                                        warnings.append(warning)

                        # 2. 新增点位监控逻辑
                        if MONITOR_CONFIG["point_monitor_enabled"]:
                            point_sources = MONITOR_CONFIG["point_monitor_sources"]
                            time_window = MONITOR_CONFIG["point_check_minutes"]
                            threshold = MONITOR_CONFIG["point_threshold"]

                            for source_id in point_sources:
                                if source_id in all_data and len(all_data[source_id]) >= 2:
                                    # 分析点位跌幅
                                    has_drop, drop_points, prev_odds, curr_odds = analyze_odds_point_drop(
                                        all_data[source_id],
                                        time_window
                                    )

                                    # 如果跌幅超过阈值，添加警告
                                    if has_drop and drop_points >= threshold:
                                        # 生成唯一标识
                                        unique_key = get_unique_warning_key(
                                            match_id=match_id,
                                            odds_type=check_type["type"],
                                            value=value,
                                            side=side,
                                            source_id=source_id
                                        )

                                        # 检查是否已触发过该警告
                                        with TRIGGER_LOCK:
                                            if unique_key in TRIGGERED_WARNINGS:
                                                logger.info(f"重复点位警告拦截（唯一标识存在）：{unique_key}")
                                                continue  # 已触发过，直接跳过
                                            TRIGGERED_WARNINGS.add(unique_key)  # 未触发过，添加到集合
                                        warning = PointWarningMessage(
                                            match_name=match_name,
                                            start_time_beijing=start_time,
                                            type=check_type["type"],
                                            value=value,
                                            side=side,
                                            warning_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            sources=[source_id],
                                            league_name=league_name,
                                            home_team=home_team,
                                            away_team=away_team,
                                            result_value=f"跌幅{drop_points:.1f}点",
                                            time_window=time_window,
                                            drop_points=drop_points,
                                            threshold_points=threshold,
                                            previous_odds=prev_odds,
                                            current_odds=curr_odds
                                        )
                                        warnings.append(warning)
                                        # 新增：保存到数据库
                                        save_point_warning_to_db(warning)

            return warnings

    except Exception as e:
        logger.error(f"检查未开始的比赛失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


# 监控系统 - 更新缓存中的警告
def update_warnings_cache():
    """更新缓存中的警告信息"""
    global CURRENT_WARNINGS

    try:
        # 获取最新警告
        new_warnings = check_active_matches()

        # 使用锁保证线程安全
        with WARNING_LOCK:
            # 替换当前警告缓存
            CURRENT_WARNINGS = new_warnings
            logger.info(f"警告缓存已更新，当前有 {len(CURRENT_WARNINGS)} 条警告")

    except Exception as e:
        logger.error(f"更新警告缓存失败: {e}")


# 监控系统 - 主循环
def monitor_loop():
    """赔率监控主循环"""
    logger.info("赔率监控系统已启动")

    # 初始更新一次警告缓存
    update_warnings_cache()

    while True:
        # 检查是否需要运行（任一监控启用）
        if MONITOR_CONFIG["enabled"] or MONITOR_CONFIG["point_monitor_enabled"]:
            try:
                start_time = time.time()

                # 更新警告缓存
                update_warnings_cache()

                end_time = time.time()
                execution_time = end_time - start_time

                # 计算下一次检查的等待时间
                wait_time = max(0, MONITOR_CONFIG["check_interval"])
                logger.info(f"监控周期完成，耗时 {execution_time:.2f} 秒，下次检查将在 {wait_time:.2f} 秒后进行")

                time.sleep(wait_time)

            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                time.sleep(MONITOR_CONFIG["check_interval"])
        else:
            # 两种监控都禁用时，休眠较长时间
            time.sleep(60)


# 新增：计算单个让分盘口的189指数
def calculate_single_spread_189(source2_spreads, spread_value, side) -> float:
    """
    计算单个让分盘口的189指数（仅针对source2）
    :param source2_spreads: source2的所有让分盘数据（{盘口值: {"home": 赔率, "away": 赔率}}）
    :param spread_value: 当前盘口值（如"-1"）
    :param side: 投注方向（"home"或"away"）
    :return: 该盘口的189指数（保留2位小数），-1表示计算失败
    """
    try:
        # 转换当前盘口为浮点数
        spread_float = float(spread_value)
        # 计算相对盘口（相反数）
        opposite_spread_float = -spread_float

        # 规范化盘口字符串（统一格式，如-1.0→"-1"）
        def format_spread(s):
            return f"{int(s)}" if s.is_integer() else f"{s}"

        spread_str = format_spread(spread_float)
        opposite_spread_str = format_spread(opposite_spread_float)

        # 0盘口特殊处理（相对盘口还是0）
        if spread_float == 0:
            opposite_spread_str = spread_str

        # 检查相对盘口是否存在
        if opposite_spread_str not in source2_spreads:
            return -1  # 相对盘口不存在，无效

        # 获取当前盘口和相对盘口的赔率
        current_odds = source2_spreads[spread_str].get(side)
        opposite_side = "away" if side == "home" else "home"
        opposite_odds = source2_spreads[opposite_spread_str].get(opposite_side)

        # 0盘口兼容（可能在同一盘口下取相反方向）
        if spread_float == 0 and opposite_odds is None:
            opposite_odds = source2_spreads[spread_str].get(opposite_side)

        # 转换赔率为浮点数
        current_odds = float(current_odds) if current_odds else None
        opposite_odds = float(opposite_odds) if opposite_odds else None

        if not current_odds or not opposite_odds:
            return -1  # 赔率无效

        # 核心计算逻辑
        if current_odds * opposite_odds > 0:  # 同号
            result = (current_odds + opposite_odds) * 100
        else:  # 异号
            abs_diff = abs(abs(current_odds) - abs(opposite_odds))
            result = (2 - abs_diff) * 100

        return round(result, 2)
    except (ValueError, KeyError, TypeError):
        return -1  # 计算失败


# 新增：计算单个大小球盘口的189指数
def calculate_single_total_189(source2_totals, total_value, side) -> float:
    """
    计算单个大小球盘口的189指数（仅针对source2）
    :param source2_totals: source2的所有大小球盘数据（{盘口值: {"over": 赔率, "under": 赔率}}）
    :param total_value: 当前盘口值（如"220.5"）
    :param side: 投注方向（"over"或"under"）
    :return: 该盘口的189指数（保留2位小数），-1表示计算失败
    """
    try:
        # 转换盘口为浮点数并规范化字符串
        total_float = float(total_value)
        total_str = f"{int(total_float)}" if total_float.is_integer() else f"{total_float}"

        # 检查当前盘口是否存在且包含对应方向赔率
        if total_str not in source2_totals:
            return -1
        total_data = source2_totals[total_str]
        if side not in total_data:
            return -1

        # 获取大球/小球赔率
        current_odds = total_data[side]
        opposite_side = "under" if side == "over" else "over"
        opposite_odds = total_data.get(opposite_side)

        # 转换赔率为浮点数
        current_odds = float(current_odds) if current_odds else None
        opposite_odds = float(opposite_odds) if opposite_odds else None

        if not current_odds or not opposite_odds:
            return -1

        # 核心计算逻辑
        if current_odds * opposite_odds > 0:  # 同号
            result = (current_odds + opposite_odds) * 100
        else:  # 异号
            abs_diff = abs(abs(current_odds) - abs(opposite_odds))
            result = (2 - abs_diff) * 100

        return round(result, 2)
    except (ValueError, KeyError, TypeError):
        return -1  # 计算失败


# 修改：更新获取警告的API接口，从数据库加载点位警告
@app.get("/api/cached-warnings", response_model=List[Union[WarningMessage, PointWarningMessage]])
async def get_cached_warnings():
    """获取缓存中的赔率下降警告和数据库中的点位警告"""
    with WARNING_LOCK:
        # 获取普通警告（保持原有逻辑）
        normal_warnings = CURRENT_WARNINGS

        # 从数据库获取点位警告
        point_warnings = load_point_warnings_from_db()

        # 合并两种警告，按警告时间倒序排列
        all_warnings = normal_warnings + point_warnings
        all_warnings.sort(key=lambda x: x.warning_time, reverse=True)

        return all_warnings


# API路由 - 获取当前监控配置
@app.get("/api/monitor/config", response_model=MonitorConfig)
async def get_monitor_config():
    """获取当前监控系统配置参数"""
    return MONITOR_CONFIG


# 更新API路由 - 更新监控配置
@app.put("/api/monitor/config", response_model=MonitorConfig)
async def update_monitor_config(config: MonitorConfig):
    """更新监控系统配置参数"""
    global MONITOR_CONFIG

    # 只更新传入的参数，未传入的保持不变
    if config.check_interval is not None:
        MONITOR_CONFIG["check_interval"] = config.check_interval
    if config.consecutive_decreases is not None:
        MONITOR_CONFIG["consecutive_decreases"] = config.consecutive_decreases
    if config.time_window is not None:
        MONITOR_CONFIG["time_window"] = config.time_window
    if config.enabled is not None:
        MONITOR_CONFIG["enabled"] = config.enabled
    if config.min_odds is not None:
        MONITOR_CONFIG["min_odds"] = config.min_odds
    if config.max_odds is not None:
        MONITOR_CONFIG["max_odds"] = config.max_odds
    if config.required_sources is not None:
        MONITOR_CONFIG["required_sources"] = config.required_sources
    if config.threshold_189 is not None:
        MONITOR_CONFIG["threshold_189"] = config.threshold_189

    # 新增点位监控参数更新
    if config.point_monitor_enabled is not None:
        MONITOR_CONFIG["point_monitor_enabled"] = config.point_monitor_enabled
    if config.point_check_minutes is not None:
        MONITOR_CONFIG["point_check_minutes"] = config.point_check_minutes
    if config.point_threshold is not None:
        MONITOR_CONFIG["point_threshold"] = config.point_threshold
    if config.point_monitor_sources is not None:
        MONITOR_CONFIG["point_monitor_sources"] = config.point_monitor_sources

    if config.enabled or config.point_monitor_enabled:
        # 如果重新启用，立即更新一次缓存
        threading.Thread(target=update_warnings_cache, daemon=True).start()

    logger.info(f"监控配置已更新: {MONITOR_CONFIG}")
    return MONITOR_CONFIG


# API路由 - 启用/禁用监控
@app.post("/api/monitor/{status}")
async def toggle_monitor(status: bool):
    """启用或禁用赔率监控系统"""
    MONITOR_CONFIG["enabled"] = status
    if status:
        logger.info("赔率监控系统已启用")
        # 如果重新启用，立即更新一次缓存
        threading.Thread(target=update_warnings_cache, daemon=True).start()
    else:
        logger.info("赔率监控系统已禁用")
    return {"status": "success", "enabled": status}


# 新增：启用/禁用点位监控
@app.post("/api/monitor/point/{status}")
async def toggle_point_monitor(status: bool):
    """启用或禁用点位监控功能"""
    MONITOR_CONFIG["point_monitor_enabled"] = status
    if status:
        logger.info("赔率点位监控系统已启用")
        # 立即更新一次缓存
        threading.Thread(target=update_warnings_cache, daemon=True).start()
    else:
        logger.info("赔率点位监控系统已禁用")
    return {
        "status": "success",
        "point_monitor_enabled": status,
        "current_settings": {
            "time_window_minutes": MONITOR_CONFIG["point_check_minutes"],
            "threshold_points": MONITOR_CONFIG["point_threshold"],
            "sources": MONITOR_CONFIG["point_monitor_sources"]
        }
    }


# 修改API接口实现
@app.get("/api/daily-odds", response_model=DailyOddsResponse)
async def get_daily_odds(
        # 将单个日期参数改为日期区间参数，start_date为必填，end_date可选
        start_date: str = Query(..., description="查询开始日期，格式：YYYY-MM-DD"),
        end_date: Optional[str] = Query(None, description="查询结束日期，格式：YYYY-MM-DD，默认与开始日期相同"),
        source_filter: Optional[List[int]] = Query(None, description="可选：筛选数据源，如[1,2,3]")
):
    """查询指定日期范围内所有比赛的所有盘口（让分+大小球）历史赔率记录"""
    logger.info(f"收到日期区间赔率查询请求：开始日期={start_date}，结束日期={end_date}，数据源筛选={source_filter}")

    # 处理结束日期，默认为开始日期
    if not end_date:
        end_date = start_date

    # 验证日期格式
    try:
        start_query_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_query_date = datetime.strptime(end_date, "%Y-%m-%d")

        # 确保开始日期不晚于结束日期
        if start_query_date > end_query_date:
            return {
                "status": "error",
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "message": "开始日期不能晚于结束日期"
            }
    except ValueError:
        return {
            "status": "error",
            "start_date": start_date,
            "end_date": end_date,
            "count": 0,
            "message": "日期格式错误，请使用YYYY-MM-DD"
        }

    # 计算查询日期范围的时间区间（开始日期00:00:00至结束日期23:59:59）
    start_of_period = start_query_date.replace(hour=0, minute=0, second=0)
    end_of_period = end_query_date.replace(hour=23, minute=59, second=59)

    conn = get_db_connection()
    if not conn:
        return {
            "status": "error",
            "start_date": start_date,
            "end_date": end_date,
            "count": 0,
            "message": "数据库连接失败"
        }

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 1. 查询该日期范围内的所有比赛，新增full_time和half_time字段
            cursor.execute("""
            SELECT id, match_name, league_name, home_team, away_team, start_time_beijing,
                   full_time, half_time  -- 新增字段
            FROM matches
            WHERE start_time_beijing::timestamp BETWEEN %s AND %s
            ORDER BY start_time_beijing ASC
            """, (start_of_period, end_of_period))
            period_matches = cursor.fetchall()
            if not period_matches:
                return {
                    "status": "success",
                    "start_date": start_date,
                    "end_date": end_date,
                    "count": 0,
                    "data": [],
                    "message": "该日期范围内无比赛记录"
                }

            result_data = []
            # 2. 遍历每场比赛，查询其所有盘口的赔率记录（原有逻辑保持不变）
            for match in period_matches:
                match_id = match["id"]
                # 2.1 查询让分盘数据（原有逻辑保持不变）
                spread_query = f"""
                SELECT spread_value, side, source, odds_value, recorded_at
                FROM spread_odds
                WHERE match_id = %s
                """
                spread_params = [match_id]
                # 应用数据源筛选
                if source_filter:
                    spread_query += " AND source = ANY(%s)"
                    spread_params.append(source_filter)
                spread_query += " ORDER BY spread_value, side, source, recorded_at ASC"

                cursor.execute(spread_query, spread_params)
                spread_records = cursor.fetchall()

                # 按（盘口值+方向）分组整理让分盘数据
                spread_odds = defaultdict(lambda: {"spread_value": None, "side": None, "sources": {}})
                for rec in spread_records:
                    key = (rec["spread_value"], rec["side"])
                    spread_odds[key]["spread_value"] = rec["spread_value"]
                    spread_odds[key]["side"] = rec["side"]
                    # 按数据源分组，记录赔率历史（去重连续相同赔率）
                    source = rec["source"]
                    if source not in spread_odds[key]["sources"]:
                        spread_odds[key]["sources"][source] = []
                    spread_odds[key]["sources"][source].append({
                        "odds": rec["odds_value"],
                        "time": rec["recorded_at"]
                    })
                # 对每个数据源的赔率进行连续去重
                for key in spread_odds:
                    for source in spread_odds[key]["sources"]:
                        spread_odds[key]["sources"][source] = deduplicate_consecutive_odds(
                            spread_odds[key]["sources"][source]
                        )
                        # 转换时间格式
                        for rec in spread_odds[key]["sources"][source]:
                            rec["time"] = rec["time"].strftime("%Y-%m-%d %H:%M:%S")
                # 转换为列表格式
                formatted_spread = list(spread_odds.values())

                # 2.2 查询大小球盘数据（原有逻辑保持不变）
                total_query = f"""
                SELECT total_value, side, source, odds_value, recorded_at
                FROM total_odds
                WHERE match_id = %s
                """
                total_params = [match_id]
                if source_filter:
                    total_query += " AND source = ANY(%s)"
                    total_params.append(source_filter)
                total_query += " ORDER BY total_value, side, source, recorded_at ASC"

                cursor.execute(total_query, total_params)
                total_records = cursor.fetchall()

                total_odds = defaultdict(lambda: {"total_value": None, "side": None, "sources": {}})
                for rec in total_records:
                    key = (rec["total_value"], rec["side"])
                    total_odds[key]["total_value"] = rec["total_value"]
                    total_odds[key]["side"] = rec["side"]
                    source = rec["source"]
                    if source not in total_odds[key]["sources"]:
                        total_odds[key]["sources"][source] = []
                    total_odds[key]["sources"][source].append({
                        "odds": rec["odds_value"],
                        "time": rec["recorded_at"]
                    })
                # 连续去重+时间格式化
                for key in total_odds:
                    for source in total_odds[key]["sources"]:
                        total_odds[key]["sources"][source] = deduplicate_consecutive_odds(
                            total_odds[key]["sources"][source]
                        )
                        for rec in total_odds[key]["sources"][source]:
                            rec["time"] = rec["time"].strftime("%Y-%m-%d %H:%M:%S")
                formatted_total = list(total_odds.values())

                # 3. 整理单场比赛数据，包含新增的full_time和half_time字段
                result_data.append({
                    "match_id": match_id,
                    "match_name": match["match_name"],
                    "league_name": match["league_name"],
                    "home_team": match["home_team"],
                    "away_team": match["away_team"],
                    "start_time_beijing": match["start_time_beijing"],
                    "full_time": match.get("full_time"),  # 新增字段
                    "half_time": match.get("half_time"),  # 新增字段
                    "spread_odds": formatted_spread,
                    "total_odds": formatted_total
                })

            return {
                "status": "success",
                "start_date": start_date,
                "end_date": end_date,
                "count": len(period_matches),
                "data": result_data
            }

    except Exception as e:
        logger.error(f"查询日期区间盘口赔率失败：{e}")
        return {
            "status": "error",
            "start_date": start_date,
            "end_date": end_date,
            "count": 0,
            "message": f"查询失败：{str(e)}"
        }
    finally:
        if conn:
            conn.close()


@app.get("/api/upcoming-odds-full", response_model=DailyOddsResponse)
async def get_upcoming_odds_full():
    """
    获取所有未开赛比赛的完整赔率数据（无参数）
    返回所有未来开赛的比赛及其所有盘口和完整历史赔率，格式与get_daily_odds一致
    优化了查询性能以支持高频率调用
    """
    try:
        # 获取当前时间，用于判断未开赛比赛
        now = datetime.now()
        # 设定合理的未来时间范围（可根据业务调整）
        future_limit = now + timedelta(days=1)  # 只查询未来7天内的比赛

        # 保留：查询开始提示（含时间范围）
        print(
            f"[{datetime.now()}] 开始查询未开赛比赛数据（{now.strftime('%Y-%m-%d')} 至 {future_limit.strftime('%Y-%m-%d')}）")

        conn = get_db_connection()
        if not conn:
            print(f"[{datetime.now()}] 数据库连接失败")
            return {
                "status": "error",
                "start_date": now.strftime("%Y-%m-%d"),
                "end_date": future_limit.strftime("%Y-%m-%d"),
                "count": 0,
                "message": "数据库连接失败"
            }

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 1. 查询所有未开赛的比赛（当前时间之后开赛）
            cursor.execute("""
            SELECT id, match_name, league_name, home_team, away_team, start_time_beijing
            FROM matches
            WHERE start_time_beijing::timestamp > %s
              AND start_time_beijing::timestamp <= %s
            ORDER BY start_time_beijing ASC
            """, (now, future_limit))

            upcoming_matches = cursor.fetchall()
            # 保留：总比赛数量
            print(f"[{datetime.now()}] 共查询到 {len(upcoming_matches)} 场未开赛比赛")

            if not upcoming_matches:
                print(f"[{datetime.now()}] 未查询到任何未开赛比赛")
                return {
                    "status": "success",
                    "start_date": now.strftime("%Y-%m-%d"),
                    "end_date": future_limit.strftime("%Y-%m-%d"),
                    "count": 0,
                    "data": [],
                    "message": "当前无未开赛比赛"
                }

            result_data = []
            spread_total = 0  # 统计总让分盘数量
            total_total = 0  # 统计总大小球盘数量

            # 2. 为每一场比赛获取完整的赔率数据
            for match in upcoming_matches:
                match_id = match["id"]
                match_name = match["match_name"]

                # 移除：单场比赛处理进度打印

                # 2.1 查询让分盘所有数据
                cursor.execute("""
                SELECT spread_value, side, source, odds_value, recorded_at
                FROM spread_odds
                WHERE match_id = %s
                ORDER BY spread_value, side, source, recorded_at ASC
                """, (match_id,))

                spread_records = cursor.fetchall()
                spread_odds = defaultdict(lambda: {"spread_value": None, "side": None, "sources": {}})

                for rec in spread_records:
                    key = (rec["spread_value"], rec["side"])
                    spread_odds[key]["spread_value"] = rec["spread_value"]
                    spread_odds[key]["side"] = rec["side"]
                    source = rec["source"]

                    if source not in spread_odds[key]["sources"]:
                        spread_odds[key]["sources"][source] = []
                    spread_odds[key]["sources"][source].append({
                        "odds": rec["odds_value"],
                        "time": rec["recorded_at"]
                    })

                # 处理让分盘数据
                for key in spread_odds:
                    for source in spread_odds[key]["sources"]:
                        spread_odds[key]["sources"][source] = deduplicate_consecutive_odds(
                            spread_odds[key]["sources"][source]
                        )
                        for rec in spread_odds[key]["sources"][source]:
                            rec["time"] = rec["time"].strftime("%Y-%m-%d %H:%M:%S")

                spread_count = len(spread_odds)
                spread_total += spread_count

                # 2.2 查询大小球盘所有数据
                cursor.execute("""
                SELECT total_value, side, source, odds_value, recorded_at
                FROM total_odds
                WHERE match_id = %s
                ORDER BY total_value, side, source, recorded_at ASC
                """, (match_id,))

                total_records = cursor.fetchall()
                total_odds = defaultdict(lambda: {"total_value": None, "side": None, "sources": {}})

                for rec in total_records:
                    key = (rec["total_value"], rec["side"])
                    total_odds[key]["total_value"] = rec["total_value"]
                    total_odds[key]["side"] = rec["side"]
                    source = rec["source"]

                    if source not in total_odds[key]["sources"]:
                        total_odds[key]["sources"][source] = []
                    total_odds[key]["sources"][source].append({
                        "odds": rec["odds_value"],
                        "time": rec["recorded_at"]
                    })

                # 处理大小球盘数据
                for key in total_odds:
                    for source in total_odds[key]["sources"]:
                        total_odds[key]["sources"][source] = deduplicate_consecutive_odds(
                            total_odds[key]["sources"][source]
                        )
                        for rec in total_odds[key]["sources"][source]:
                            rec["time"] = rec["time"].strftime("%Y-%m-%d %H:%M:%S")

                total_count = len(total_odds)
                total_total += total_count

                # 移除：单场比赛处理完成的详细打印

                # 3. 整理结果数据
                result_data.append({
                    "match_id": match_id,
                    "match_name": match_name,
                    "league_name": match["league_name"],
                    "home_team": match["home_team"],
                    "away_team": match["away_team"],
                    "start_time_beijing": match["start_time_beijing"],
                    "full_time": None,
                    "half_time": None,
                    "spread_odds": list(spread_odds.values()),
                    "total_odds": list(total_odds.values())
                })

            # 保留：总体统计信息（关键指标）
            print(
                f"[{datetime.now()}] 数据处理完成：{len(upcoming_matches)}场比赛，总让分盘{spread_total}个，总大小球盘{total_total}个")

            return {
                "status": "success",
                "start_date": now.strftime("%Y-%m-%d"),
                "end_date": future_limit.strftime("%Y-%m-%d"),
                "count": len(upcoming_matches),
                "data": result_data
            }

    except Exception as e:
        error_msg = f"查询未开赛比赛完整赔率失败：{e}"
        print(f"[{datetime.now()}] 错误: {error_msg}")
        logger.error(error_msg)
        current_date = now.strftime("%Y-%m-%d") if 'now' in locals() else ""
        end_date = future_limit.strftime("%Y-%m-%d") if 'future_limit' in locals() else ""
        return {
            "status": "error",
            "start_date": current_date,
            "end_date": end_date,
            "count": 0,
            "message": f"查询失败：{str(e)}"
        }
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            # 保留：连接关闭提示
            print(f"[{datetime.now()}] 数据库连接已关闭")


# 启动监控线程
def start_monitor_thread():
    """启动赔率监控线程"""
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    logger.info("赔率监控线程已启动")


# 启动应用
if __name__ == "__main__":
    # 初始化点位警告表（首次运行时创建表）
    init_point_warnings_table()

    # 额外检查并修复约束（确保万无一失）
    repair_point_warnings_constraint()
    # 启动监控线程
    #start_monitor_thread()

    # 启动API服务
    uvicorn.run(app, host="0.0.0.0", port=8766)
    logger.info("赔率历史数据API服务已启动，监听端口8766")