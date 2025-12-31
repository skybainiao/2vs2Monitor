import re
import time
import requests
import psycopg2
import schedule
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union

from flask_cors import CORS
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
import threading
from threading import local

app = Flask(__name__)
analyzer = None  # 全局分析器实例
CORS(app)  # 允许跨域请求
# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}

# 全局固定参数 - 早收盘过滤分钟数
EARLY_CLOSE_FILTER_MINUTES = None  # 开赛前5分钟内需要有数据源1的赔率


class SportsOddsAnalyzer:
    def __init__(self):
        # 常量定义
        self.STAKE_PER_BET = 100  # 每单下注金额
        self.API_URL = "http://160.25.20.18:8766/api/daily-odds"  # 全量计算API地址

        # 新增：同方向盘口过滤开关
        self.side_filter_enabled = False  # 默认开启同方向盘口过滤

        # 排序状态变量
        self.league_sort_state = {
            'field': 'profit',
            'direction': 'desc'  # 'asc' 升序, 'desc' 降序
        }

        self.region_sort_state = {
            'continent': {'field': 'profit', 'direction': 'desc'},
            'country': {'field': 'profit', 'direction': 'desc'}
        }

        # 大洲-国家映射表
        self.CONTINENT_COUNTRY_MAP = {
            "欧洲": [
                "England", "Germany", "Spain", "Italy", "France", "Russia",
                "Czech Republic", "Netherlands", "Kazakhstan", "Armenia", "Albania",
                "Azerbaijan", "Andorra", "Austria", "Bulgaria", "Belarus", "Belgium",
                "Bosnia and Herzegovina", "Croatia", "Cyprus", "Czechia", "Denmark",
                "Estonia", "Northern Ireland", "Faroe Islands", "Georgia", "Gibraltar",
                "Greece", "Hungary", "Ireland", "Iceland", "Israel", "Latvia", "Lithuania",
                "Luxembourg", "Liechtenstein", "Montenegro", "Turkiye", "Slovakia",
                "Slovenia", "Sweden", "Switzerland", "San Marino", "Scotland", "Serbia",
                "Moldova", "Malta", "Kosovo", "Ukraine", "Poland", "Portugal",
                "North Macedonia", "Norway", "Finland"
            ],
            "非洲": [
                "Somalia", "Guinea", "South Sudan", "DR Congo", "Kenya", "Gambia",
                "Namibia", "Malawi", "Benin", "Zimbabwe", "Lesotho", "South Africa",
                "Uganda", "Mozambique", "Egypt", "Ethiopia", "Ivory Coast", "Burundi",
                "Mauritania", "Togo", "Morocco", "Niger", "Senegal", "Sudan", "Nigeria",
                "Rwanda", "Algeria", "Angola", "Burkina Faso", "Botswana", "Chad",
                "Cameroun", "Cabo Verde", "Cote d'Ivoire", "Congo", "Comoros",
                "Central African Republic", "Democratic Rep Congo", "Eswatini", "Eritrea",
                "Gabon", "Ghana", "Libya", "Liberia", "Mali", "Mauritius", "Madagascar",
                "Sao Tome & Principe", "Sierra Leone", "Seychelles", "Tanzania",
                "Tunisia", "Zambia"
            ],
            "亚洲": [
                "Australia", "Japan", "Korea", "China", "Afghanistan", "Bahrain",
                "Bhutan", "Brunei", "Bangladesh", "Emirates", "Laos", "Lebanon",
                "Iraq", "Iran", "India", "Indonesia", "Jordan", "Kyrgyz", "Kuwait",
                "Cambodia", "Myanmar", "Mongolia", "Malaysia", "Maldives", "Nepal",
                "North Korea", "Tajikistan", "Turkmenistan", "Thailand", "Timor-Leste",
                "Uzbekistan", "Viet Nam", "Saudi", "Syrian", "Sri Lanka", "Singapore",
                "Oman", "Philippines", "Palestine", "Pakistan", "Qatar", "Yemen"
            ],
            "大洋洲": [
                "New Zealand", "Papua New Guinea", "American Samoa", "Cook Islands",
                "New Caledonia", "Fiji", "Samoa", "Solomon Islands", "Vanuatu",
                "Tonga"
            ],
            "北美洲": [
                "Canada", "America", "USA", "Mexico", "Belize", "Bermuda", "Bahamas",
                "Barbados", "Cuba", "Curacao", "Costa Rica", "Dominicana", "El Salvador",
                "Guatemala", "Honduras", "Haiti", "Jamaica", "Nicaragua", "Panama",
                "Saint Lucia", "Trinidad and Tobago"
            ],
            "南美洲": [
                "Argentina", "Brazil", "Bolivia", "Colombia", "Chile", "Ecuador",
                "Guyana", "Peru", "Paraguay", "Suriname", "Venezuela", "Uruguay"
            ]
        }

        # 初始化默认参数
        self.selected_markets = ['spread', 'total']
        self.selected_sources = ['1', '2']
        self.comparison_type = 'high'
        self.filter_minutes = None
        self.min_odds = None
        self.max_odds = None
        # 使用全局固定参数作为早收盘过滤参数
        self.early_close_filter_minutes = EARLY_CLOSE_FILTER_MINUTES

        # 线程本地存储，为每个线程维护独立的数据库连接
        self.thread_local = local()

    # 数据库连接管理 - 关键修复：为每个线程提供独立连接
    def get_db_connection(self):
        """获取当前线程的数据库连接，每个线程独立维护一个连接"""
        try:
            # 检查当前线程是否已有连接
            if hasattr(self.thread_local, 'db_conn'):
                conn = self.thread_local.db_conn
                # 检查连接是否有效
                if conn is not None and conn.closed == 0:
                    return conn

            # 为当前线程创建新连接
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                database=DB_CONFIG["database"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                port=DB_CONFIG["port"]
            )
            # 存储到线程本地
            self.thread_local.db_conn = conn
            return conn
        except Exception as e:
            print(f"数据库连接错误: {str(e)}")
            return None

    def close_db_connection(self):
        """关闭当前线程的数据库连接"""
        try:
            if hasattr(self.thread_local, 'db_conn'):
                conn = self.thread_local.db_conn
                if conn is not None and conn.closed == 0:
                    conn.close()
                # 清除线程本地存储的连接
                del self.thread_local.db_conn
        except Exception as e:
            print(f"关闭数据库连接错误: {str(e)}")

    def init_database(self):
        """初始化数据库表结构
        保持原有30天数据表不变，新增7天数据表
        """
        conn = self.get_db_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cur:
                # 1. 原有30天数据的表（保持不变）
                # 创建计算结果汇总表（30天）
                cur.execute("""
                CREATE TABLE IF NOT EXISTS calculation_summary (
                    id SERIAL PRIMARY KEY,
                    calculation_time TIMESTAMP NOT NULL,
                    comparison_type VARCHAR(10) NOT NULL,
                    predicted_win_count INTEGER NOT NULL,
                    correct_count INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,
                    win_rate DECIMAL(5,2) NOT NULL,
                    total_score INTEGER NOT NULL,
                    undecidable_count INTEGER NOT NULL,
                    total_bet_count INTEGER NOT NULL,
                    total_stake DECIMAL(10,2) NOT NULL,
                    total_profit DECIMAL(10,2) NOT NULL,
                    roi DECIMAL(5,2) NOT NULL,
                    UNIQUE(comparison_type)  -- 确保每种类型只有一条记录（用于覆盖）
                )
                """)

                # 创建联赛统计表（30天）
                cur.execute("""
                CREATE TABLE IF NOT EXISTS league_statistics (
                    id SERIAL PRIMARY KEY,
                    calculation_id INTEGER NOT NULL REFERENCES calculation_summary(id),
                    league_name VARCHAR(100) NOT NULL,
                    count INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    win_rate DECIMAL(5,2) NOT NULL,
                    profit DECIMAL(10,2) NOT NULL,
                    roi DECIMAL(5,2) NOT NULL,
                    UNIQUE(calculation_id, league_name)
                )
                """)

                # 2. 新增7天数据的表（单独存储）
                # 创建计算结果汇总表（7天）
                cur.execute("""
                CREATE TABLE IF NOT EXISTS calculation_summary_7d (
                    id SERIAL PRIMARY KEY,
                    calculation_time TIMESTAMP NOT NULL,
                    comparison_type VARCHAR(10) NOT NULL,
                    predicted_win_count INTEGER NOT NULL,
                    correct_count INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,
                    win_rate DECIMAL(5,2) NOT NULL,
                    total_score INTEGER NOT NULL,
                    undecidable_count INTEGER NOT NULL,
                    total_bet_count INTEGER NOT NULL,
                    total_stake DECIMAL(10,2) NOT NULL,
                    total_profit DECIMAL(10,2) NOT NULL,
                    roi DECIMAL(5,2) NOT NULL,
                    UNIQUE(comparison_type)  -- 确保每种类型只有一条记录（用于覆盖）
                )
                """)

                # 创建联赛统计表（7天）
                cur.execute("""
                CREATE TABLE IF NOT EXISTS league_statistics_7d (
                    id SERIAL PRIMARY KEY,
                    calculation_id INTEGER NOT NULL REFERENCES calculation_summary_7d(id),
                    league_name VARCHAR(100) NOT NULL,
                    count INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    win_rate DECIMAL(5,2) NOT NULL,
                    profit DECIMAL(10,2) NOT NULL,
                    roi DECIMAL(5,2) NOT NULL,
                    UNIQUE(calculation_id, league_name)
                )
                """)

                conn.commit()
                return True
        except Exception as e:
            print(f"数据库初始化错误: {str(e)}")
            conn.rollback()
            return False
        finally:
            self.close_db_connection()

    def save_results_to_db(self, summary_stats, league_stats, comparison_type, is_7d=False):
        """
        将计算结果保存到数据库
        is_7d=True 保存到7天数据表，False保存到原有30天数据表
        """
        # 每次保存都使用新连接，避免线程间干扰
        conn = self.get_db_connection()
        if not conn:
            return False

        # 根据是否7天数据选择表名
        summary_table = "calculation_summary_7d" if is_7d else "calculation_summary"
        league_table = "league_statistics_7d" if is_7d else "league_statistics"

        try:
            with conn.cursor() as cur:
                # 先删除该类型的旧数据
                cur.execute(f"""
                DELETE FROM {league_table} 
                WHERE calculation_id IN (
                    SELECT id FROM {summary_table} 
                    WHERE comparison_type = %s
                )
                """, (comparison_type,))

                cur.execute(f"""
                DELETE FROM {summary_table} 
                WHERE comparison_type = %s
                """, (comparison_type,))

                # 插入新的汇总结果
                cur.execute(f"""
                INSERT INTO {summary_table} (
                    calculation_time, comparison_type, predicted_win_count,
                    correct_count, total_count, win_rate, total_score,
                    undecidable_count, total_bet_count, total_stake,
                    total_profit, roi
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, (
                    datetime.now(),
                    comparison_type,
                    summary_stats['predictedWinCount'],
                    summary_stats['correctCount'],
                    summary_stats['totalCount'],
                    (summary_stats['correctCount'] / summary_stats['totalCount'] * 100
                     if summary_stats['totalCount'] > 0 else 0),
                    summary_stats['totalScore'],
                    summary_stats['undecidableCount'],
                    summary_stats['totalBetCount'],
                    summary_stats['totalStake'],
                    summary_stats['totalProfit'],
                    (summary_stats['totalProfit'] / summary_stats['totalStake'] * 100
                     if summary_stats['totalStake'] > 0 else 0)
                ))

                calculation_id = cur.fetchone()[0]

                # 插入联赛统计
                for league_name, stats in league_stats.items():
                    win_rate = (stats['correct'] / stats['count'] * 100
                                if stats['count'] > 0 else 0)
                    roi = (stats['profit'] / stats['stake'] * 100
                           if stats['stake'] > 0 else 0)

                    cur.execute(f"""
                    INSERT INTO {league_table} (
                        calculation_id, league_name, count, score,
                        win_rate, profit, roi
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        calculation_id,
                        league_name,
                        stats['count'],
                        stats['score'],
                        win_rate,
                        stats['profit'],
                        roi
                    ))

                conn.commit()
                print(
                    f"计算结果已成功保存到数据库（{'7天' if is_7d else '30天'} | {comparison_type}），ID: {calculation_id}")
                return True
        except Exception as e:
            print(f"保存结果到数据库错误: {str(e)}")
            conn.rollback()
            return False
        finally:
            # 确保每次操作后关闭连接
            self.close_db_connection()

    def get_latest_results_from_db(self):
        """获取最新结果（30天数据，保持原有逻辑不变）"""
        conn = self.get_db_connection()
        if not conn:
            return None

        try:
            results = {
                'high': {
                    'summary': None,
                    'leagues': []
                },
                'low': {
                    'summary': None,
                    'leagues': []
                }
            }

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 获取high模式的最新结果
                cur.execute("""
                SELECT * FROM calculation_summary 
                WHERE comparison_type = 'high'
                ORDER BY calculation_time DESC
                LIMIT 1
                """)
                high_summary = cur.fetchone()
                if high_summary:
                    results['high']['summary'] = dict(high_summary)

                    # 获取对应的联赛统计
                    cur.execute("""
                    SELECT * FROM league_statistics 
                    WHERE calculation_id = %s
                    """, (high_summary['id'],))
                    high_leagues = cur.fetchall()
                    results['high']['leagues'] = [dict(league) for league in high_leagues]

                # 获取low模式的最新结果
                cur.execute("""
                SELECT * FROM calculation_summary 
                WHERE comparison_type = 'low'
                ORDER BY calculation_time DESC
                LIMIT 1
                """)
                low_summary = cur.fetchone()
                if low_summary:
                    results['low']['summary'] = dict(low_summary)

                    # 获取对应的联赛统计
                    cur.execute("""
                    SELECT * FROM league_statistics 
                    WHERE calculation_id = %s
                    """, (low_summary['id'],))
                    low_leagues = cur.fetchall()
                    results['low']['leagues'] = [dict(league) for league in low_leagues]

            return results
        except Exception as e:
            print(f"从数据库获取结果错误: {str(e)}")
            return None
        finally:
            self.close_db_connection()

    # 新增：获取7天数据结果
    def get_latest_7d_results_from_db(self):
        """获取7天数据的最新结果"""
        conn = self.get_db_connection()
        if not conn:
            return None

        try:
            results = {
                'high': {
                    'summary': None,
                    'leagues': []
                },
                'low': {
                    'summary': None,
                    'leagues': []
                }
            }

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 获取high模式的最新7天结果
                cur.execute("""
                SELECT * FROM calculation_summary_7d 
                WHERE comparison_type = 'high'
                ORDER BY calculation_time DESC
                LIMIT 1
                """)
                high_summary = cur.fetchone()
                if high_summary:
                    results['high']['summary'] = dict(high_summary)

                    # 获取对应的联赛统计
                    cur.execute("""
                    SELECT * FROM league_statistics_7d 
                    WHERE calculation_id = %s
                    """, (high_summary['id'],))
                    high_leagues = cur.fetchall()
                    results['high']['leagues'] = [dict(league) for league in high_leagues]

                # 获取low模式的最新7天结果
                cur.execute("""
                SELECT * FROM calculation_summary_7d 
                WHERE comparison_type = 'low'
                ORDER BY calculation_time DESC
                LIMIT 1
                """)
                low_summary = cur.fetchone()
                if low_summary:
                    results['low']['summary'] = dict(low_summary)

                    # 获取对应的联赛统计
                    cur.execute("""
                    SELECT * FROM league_statistics_7d 
                    WHERE calculation_id = %s
                    """, (low_summary['id'],))
                    low_leagues = cur.fetchall()
                    results['low']['leagues'] = [dict(league) for league in low_leagues]

            return results
        except Exception as e:
            print(f"从数据库获取7天结果错误: {str(e)}")
            return None
        finally:
            self.close_db_connection()

    # 从API获取数据
    def fetch_data_from_api(self, start_date: str, end_date: Optional[str] = None,
                            source_filter: Optional[List[int]] = None) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        从API获取赛事数据

        参数:
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD，默认为开始日期
            source_filter: 数据源筛选列表

        返回:
            (是否成功, 数据列表, 消息)
        """
        try:
            # 验证日期格式
            datetime.strptime(start_date, "%Y-%m-%d")
            if end_date:
                datetime.strptime(end_date, "%Y-%m-%d")

        except ValueError:
            return False, [], "日期格式错误，请使用YYYY-MM-DD"

        # 构建请求参数
        params = {
            "start_date": start_date
        }

        if end_date:
            params["end_date"] = end_date

        if source_filter:
            params["source_filter"] = source_filter

        try:
            # 发送请求
            response = requests.get(self.API_URL, params=params, timeout=300)
            response.raise_for_status()  # 抛出HTTP错误状态码

            data = response.json()

            # 检查API返回状态
            if data.get("status") != "success":
                return False, [], f"API返回错误: {data.get('message', '未知错误')}"

            return True, data.get("data", []), f"成功获取 {data.get('count', 0)} 场比赛数据"

        except requests.exceptions.RequestException as e:
            return False, [], f"请求API失败: {str(e)}"


    # 修改1：重写早收盘过滤函数，按盘口分组判断
    def filter_odds_by_early_close(self, match: Dict[str, Any], odds_list: List[Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        """
        按盘口分组过滤：仅保留“数据源1在阈值时间后有记录”的整个盘口
        """
        # 未启用过滤，直接返回原始列表
        if self.early_close_filter_minutes is None:
            return odds_list

        # 1. 解析比赛开始时间，失败则过滤所有盘口
        match_start_raw = self.get_match_start_time_raw(match)
        match_start_date = self.parse_date_time(match_start_raw)
        if not match_start_date:
            return []  # 无法解析开始时间，整个比赛的盘口都不参与

        # 2. 计算阈值时间：比赛开始时间 - N分钟
        threshold_time = match_start_date - timedelta(minutes=self.early_close_filter_minutes)

        # 3. 按盘口分组（market+handicap+side唯一确定一个盘口）
        odds_groups = {}
        for odd in odds_list:
            # 生成盘口唯一标识（市场类型+让分+投注方向）
            group_key = (
                odd.get('market'),
                odd.get('handicap'),
                odd.get('side')
            )
            if group_key not in odds_groups:
                odds_groups[group_key] = []
            odds_groups[group_key].append(odd)

        # 4. 对每个盘口组判断是否符合条件
        valid_odds = []
        for group in odds_groups.values():
            # 检查组内是否有“数据源1且时间在阈值后”的记录
            has_valid_source1 = False
            for odd in group:
                # 仅检查数据源1
                if odd.get('source') != '1':
                    continue
                # 解析该记录的时间
                odd_time = self.parse_date_time(odd.get('time'))
                if odd_time and odd_time >= threshold_time:
                    has_valid_source1 = True
                    break  # 找到1条符合条件的即可

            # 若符合条件，保留整个盘口的所有记录
            if has_valid_source1:
                valid_odds.extend(group)

        return valid_odds

    # 工具函数：安全数字转换
    def safe_number(self, v: Any) -> Optional[float]:
        try:
            v = float(v)
            return v if float('-inf') < v < float('inf') else None
        except (ValueError, TypeError):
            return None

    # 工具函数：解析日期时间
    def parse_date_time(self, v: Any) -> Optional[datetime]:
        if v is None:
            return None

        if isinstance(v, (int, float)):
            # 处理时间戳
            ms = v * 1000 if v < 1e12 else v
            try:
                return datetime.fromtimestamp(ms / 1000)
            except (ValueError, OSError):
                return None

        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None

            # 尝试ISO格式
            try:
                return datetime.fromisoformat(s.replace(' ', 'T'))
            except ValueError:
                pass

            # 尝试正则匹配
            match = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?', s)
            if match:
                try:
                    year = int(match.group(1))
                    mon = int(match.group(2)) - 1  # Python月份从0开始
                    day = int(match.group(3))
                    hh = int(match.group(4))
                    mm = int(match.group(5))
                    ss = int(match.group(6) or 0)
                    return datetime(year, mon, day, hh, mm, ss)
                except ValueError:
                    pass

            # 尝试通用解析
            try:
                return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass

            try:
                return datetime.strptime(s, '%Y-%m-%d %H:%M')
            except ValueError:
                pass

        return None

    # 获取比赛开始时间原始值
    def get_match_start_time_raw(self, match: Dict[str, Any]) -> Any:
        if not match or not isinstance(match, dict):
            return None

        time_fields = [
            'start_time_beijing', 'start_time', 'startTime',
            'kickoff_time', 'kickoff', 'start_ts'
        ]

        for field in time_fields:
            if field in match:
                return match[field]

        # 检查包含特定关键词的字段
        for key, value in match.items():
            if re.search(r'start|kickoff|begin', key, re.IGNORECASE):
                return value

        return None

    # 查找赔率列表
    def find_odds_list(self, match: Dict[str, Any]) -> List[Dict[str, Any]]:
        odds_list = []
        if not match or not isinstance(match, dict):
            return odds_list

        # 处理让分盘
        if 'spread_odds' in match and isinstance(match['spread_odds'], list):
            for item in match['spread_odds']:
                if not isinstance(item, dict):
                    continue

                handicap = item.get('spread_value')
                side = item.get('side')
                market = "spread"

                if 'sources' in item and isinstance(item['sources'], dict):
                    for src, odds_items in item['sources'].items():
                        if not isinstance(odds_items, list):
                            continue

                        for o in odds_items:
                            if not isinstance(o, dict) or 'odds' not in o:
                                continue

                            price_num = self.safe_number(o['odds'])
                            if price_num is None:
                                continue

                            odds_list.append({
                                'market': market,
                                'handicap': handicap,
                                'side': side,
                                'source': str(src),
                                'price': price_num,
                                'time': o.get('time')
                            })

        # 处理大小球盘
        if 'total_odds' in match and isinstance(match['total_odds'], list):
            for item in match['total_odds']:
                if not isinstance(item, dict):
                    continue

                handicap = item.get('total_value')
                side = item.get('side')
                market = "total"

                if 'sources' in item and isinstance(item['sources'], dict):
                    for src, odds_items in item['sources'].items():
                        if not isinstance(odds_items, list):
                            continue

                        for o in odds_items:
                            if not isinstance(o, dict) or 'odds' not in o:
                                continue

                            price_num = self.safe_number(o['odds'])
                            if price_num is None:
                                continue

                            odds_list.append({
                                'market': market,
                                'handicap': handicap,
                                'side': side,
                                'source': str(src),
                                'price': price_num,
                                'time': o.get('time')
                            })

        return odds_list

    # 解析最终比分
    def get_final_score(self, match: Dict[str, Any]) -> Dict[str, Any]:
        if not match or not isinstance(match, dict):
            return {'home': None, 'away': None, 'raw': None}

        # 尝试从full_time字段解析
        full_time_fields = ['full_time']
        for field in full_time_fields:
            if field in match and isinstance(match[field], str) and match[field].strip():
                raw = match[field].strip()
                match_score = re.match(r'^\s*(\d+)\s*[-:]\s*(\d+)\s*$', raw)
                if match_score:
                    return {
                        'home': int(match_score.group(1)),
                        'away': int(match_score.group(2)),
                        'raw': raw
                    }

        # 尝试从home_score和away_score字段解析
        home_score_fields = ['home_score', 'homeScore']
        away_score_fields = ['away_score', 'awayScore']

        for hf in home_score_fields:
            for af in away_score_fields:
                if hf in match and af in match:
                    hs = self.safe_number(match[hf])
                    ascore = self.safe_number(match[af])
                    if hs is not None and ascore is not None:
                        return {
                            'home': hs,
                            'away': ascore,
                            'raw': f"{hs}-{ascore}"
                        }

        return {'home': None, 'away': None, 'raw': None}

    # 判断某一方在该盘口是否"输"
    def did_side_lose(self, match: Dict[str, Any], market: str, handicap_raw: Any, side_raw: str) -> Optional[bool]:
        score = self.get_final_score(match)
        if score['home'] is None or score['away'] is None:
            return None

        home = score['home']
        away = score['away']
        side = str(side_raw).lower() if side_raw else ''
        market_lower = str(market).lower() if market else ''
        line = self.safe_number(handicap_raw)

        # 处理大小球盘
        if (market_lower == 'total' or
                'over' in side or 'under' in side or
                'total' in market_lower or 'over' in market_lower or 'under' in market_lower):

            if line is None:
                return None

            total = home + away
            side_is_over = ('over' in side or 'over' in market_lower or
                            (market_lower == 'total' and side == 'over'))
            side_is_under = ('under' in side or 'under' in market_lower or
                             (market_lower == 'total' and side == 'under'))

            if side_is_over:
                if total > line:
                    return False  # 没输
                elif total < line:
                    return True  # 输了
                else:
                    return False  # 走盘，不算输

            if side_is_under:
                if total < line:
                    return False  # 没输
                elif total > line:
                    return True  # 输了
                else:
                    return False  # 走盘，不算输

            return None

        # 处理让分盘
        if (market_lower == 'spread' or
                'spread' in market_lower or 'handicap' in market_lower):

            if line is None:
                return None

            # 判断投注方向
            side_is_home = (
                    'home' in side or '主' in side or
                    re.match(r'^h$', side, re.IGNORECASE) or
                    side == '主队' or side == 'home'
            )

            side_is_away = (
                    'away' in side or '客' in side or
                    re.match(r'^a$', side, re.IGNORECASE) or
                    side == '客队' or side == 'away'
            )

            # 如果无法识别side，默认主队
            if not side_is_home and not side_is_away:
                side_is_home = True

            # 应用让分
            if side_is_home:
                adj_side = home + line
                other = away
            else:
                adj_side = away + line
                other = home

            if adj_side > other:
                return False  # 没输
            elif adj_side < other:
                return True  # 输了
            else:
                return False  # 走盘，不算输

        # 其他市场无法判断
        return None

    # 转义HTML（防XSS）
    def escape_html(self, s: Any) -> str:
        if s is None:
            return ''
        s = str(s)
        return (s.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))

    # 拆分四分盘（.25/.75）为两个半注
    def split_quarter_handicap(self, line_raw: Any) -> List[Dict[str, float]]:
        line = self.safe_number(line_raw)
        if line is None:
            return [{'line': line_raw, 'weight': 1.0}]

        abs_line = abs(line)
        int_part = float(int(abs_line))
        frac = round((abs_line - int_part) * 100) / 100
        sign = 1 if line >= 0 else -1

        # .25 -> (base, base+0.5)
        if abs(frac - 0.25) < 1e-9:
            comp1 = sign * int_part
            comp2 = sign * (int_part + 0.5)
            return [{'line': comp1, 'weight': 0.5}, {'line': comp2, 'weight': 0.5}]

        # .75 -> (base+0.5, base+1)
        if abs(frac - 0.75) < 1e-9:
            comp1 = sign * (int_part + 0.5)
            comp2 = sign * (int_part + 1.0)
            return [{'line': comp1, 'weight': 0.5}, {'line': comp2, 'weight': 0.5}]

        # 其他不拆分
        return [{'line': line, 'weight': 1.0}]

    # 计算让分盘单个component的结果
    def eval_spread_component(self, home: float, away: float, component_line: float, side_is_home: bool) -> Optional[
        str]:
        if component_line is None:
            return None

        if side_is_home:
            adj = home + component_line
            other = away
        else:
            adj = away + component_line
            other = home

        if adj > other:
            return 'win'
        elif adj < other:
            return 'lose'
        else:
            return 'push'

    # 计算大小球盘单个component的结果
    def eval_total_component(self, home: float, away: float, component_line: float, side_is_over: bool) -> Optional[
        str]:
        if component_line is None:
            return None

        total = home + away
        if total > component_line:
            return 'win' if side_is_over else 'lose'
        elif total < component_line:
            return 'lose' if side_is_over else 'win'
        else:
            return 'push'

    # 赔率转换：概率转马来盘
    def probability_to_malay(self, p: float) -> Optional[float]:
        if p is None or p <= 0:
            return None
        decimal = 1 / p
        if decimal > 2:
            return -1 / (decimal - 1)
        return decimal - 1

    # 赔率转换：马来盘转概率
    def malay_to_probability(self, m: float) -> Optional[float]:
        if m is None or not isinstance(m, (int, float)) or m != m:  # m != m 检查NaN
            return None
        if m >= 0:
            return 1 / (1 + m)
        return (-m) / (1 - m)

    # 计算一单下注的净盈亏
    def compute_profit_for_bet(self, match: Dict[str, Any], market: str,
                               handicap_raw: Any, side_raw: str,
                               stake: float, decimal_odds: float) -> Optional[float]:
        score = self.get_final_score(match)
        if score['home'] is None or score['away'] is None:
            return None

        home = score['home']
        away = score['away']

        if not isinstance(decimal_odds, (int, float)) or decimal_odds <= 0:
            return None

        # 转换为马来盘赔率
        malay_odds = self.probability_to_malay(1 / decimal_odds)
        if malay_odds is None or malay_odds != malay_odds:  # 检查NaN
            return None

        market_lower = str(market).lower() if market else ''
        side = str(side_raw).lower() if side_raw else ''

        # 判断side是否为主队/客队
        side_is_home = (
                'home' in side or '主' in side or
                re.match(r'^h$', side, re.IGNORECASE) or
                side == '主队' or side == 'home'
        )

        side_is_away = (
                'away' in side or '客' in side or
                re.match(r'^a$', side, re.IGNORECASE) or
                side == '客队' or side == 'away'
        )

        if not side_is_home and not side_is_away:
            side_is_home = True  # 兼容默认主队

        # 判断over/under
        s_trim = side.strip().lower()
        side_is_over = (s_trim == 'over' or s_trim == 'o' or
                        re.search(r'\bover\b', s_trim) or '大' in s_trim)

        side_is_under = (s_trim == 'under' or s_trim == 'u' or
                         re.search(r'\bunder\b', s_trim) or '小' in s_trim)

        # 拆分四分盘
        components = self.split_quarter_handicap(handicap_raw)

        total_profit = 0.0
        for comp in components:
            comp_line = self.safe_number(comp['line'])
            comp_stake = stake * comp['weight']
            outcome = None

            # 判断是大小球还是让分盘
            market_suggests_total = (market_lower.find('total') != -1 or
                                     market_lower.find('over') != -1 or
                                     market_lower.find('under') != -1)

            if market_suggests_total or side_is_over or side_is_under:
                # 处理大小球
                using_over = (side_is_over or market_lower.find('over') != -1 or
                              (market_lower.find('total') != -1 and s_trim == 'over'))

                using_under = (side_is_under or market_lower.find('under') != -1 or
                               (market_lower.find('total') != -1 and s_trim == 'under'))

                if not using_over and not using_under:
                    return None

                outcome = self.eval_total_component(home, away, comp_line, using_over)
            elif (market_lower == 'spread' or
                  market_lower.find('spread') != -1 or
                  market_lower.find('handicap') != -1):
                # 处理让分盘
                outcome = self.eval_spread_component(home, away, comp_line, side_is_home)
            else:
                # 未知市场
                return None

            if outcome is None:
                return None

            # 计算盈亏
            if outcome == 'win':
                if malay_odds >= 0:
                    total_profit += comp_stake * malay_odds  # 正赔率：净赢 = stake * odds
                else:
                    total_profit += comp_stake  # 负赔率：净赢 = 本金
            elif outcome == 'push':
                # 走盘：盈亏为0
                total_profit += 0
            elif outcome == 'lose':
                if malay_odds >= 0:
                    total_profit -= comp_stake  # 正赔率：输掉本金
                else:
                    total_profit -= comp_stake * abs(malay_odds)  # 负赔率：按赔率绝对值计算亏损
            else:
                return None

        return total_profit

    # 预测结果评分函数
    def score_result(self, predicted_win: bool, actual_win: bool) -> int:
        if predicted_win:
            return 1 if actual_win else -1
        return 0

    # 判断某一方在该盘口是否"赢"
    def did_side_win(self, match: Dict[str, Any], market: str, handicap_raw: Any, side_raw: str) -> Optional[bool]:
        lose_result = self.did_side_lose(match, market, handicap_raw, side_raw)
        if lose_result is None:
            return None
        return not lose_result

    # 读取赔率过滤参数
    def read_odds_filter_params(self) -> Dict[str, Optional[float]]:
        return {
            'min': self.min_odds,
            'max': self.max_odds
        }

    # 赔率区间判断
    def is_odds_in_range(self, odds: float, min_odds: Optional[float], max_odds: Optional[float]) -> bool:
        if odds is None or odds != odds:  # 检查NaN
            return False

        # 未设置任何过滤条件
        if min_odds is None and max_odds is None:
            return True

        # 仅设置最低赔率
        if min_odds is not None and max_odds is None:
            if min_odds >= 0:
                return odds < 0 or odds >= min_odds
            else:
                return odds < 0 and odds >= min_odds

        # 仅设置最高赔率
        if max_odds is not None and min_odds is None:
            if max_odds >= 0:
                return odds >= 0 and odds <= max_odds
            else:
                return odds >= 0 or odds <= max_odds

        # 同时设置min和max
        pass_min = False
        pass_max = False

        if min_odds >= 0:
            pass_min = odds < 0 or odds >= min_odds
        else:
            pass_min = odds < 0 and odds >= min_odds

        if max_odds >= 0:
            pass_max = odds >= 0 and odds <= max_odds
        else:
            pass_max = odds >= 0 or odds <= max_odds

        return pass_min and pass_max

    # 判断两个盘口是否为上下盘关系
    def are_opposite_odds(self, a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        # 必须是同一场比赛
        if a.get('match') != b.get('match'):
            return False

        # 必须是同一类型的盘口
        if a.get('market') != b.get('market'):
            return False

        # 让分盘上下盘判断
        if a.get('market') == 'spread':
            a_handicap = self.safe_number(a.get('handicap'))
            b_handicap = self.safe_number(b.get('handicap'))

            if a_handicap is None or b_handicap is None:
                return False

            # 让分0的情况，只需队伍方向相反
            if a_handicap == 0 and b_handicap == 0:
                return self.are_opposite_sides(a.get('side'), b.get('side'))

            # 盘口绝对值相同，符号相反，且队伍方向相反
            return (abs(a_handicap) == abs(b_handicap) and
                    a_handicap == -b_handicap and
                    self.are_opposite_sides(a.get('side'), b.get('side')))

        # 大小球盘上下盘判断
        if a.get('market') == 'total':
            # 盘口值相同，方向相反(over/under)
            a_side = str(a.get('side')).lower() if a.get('side') else ''
            b_side = str(b.get('side')).lower() if b.get('side') else ''

            return (a.get('handicap') == b.get('handicap') and
                    (('over' in a_side and 'under' in b_side) or
                     ('under' in a_side and 'over' in b_side)))

        return False

    # 判断两个队伍方向是否相反
    def are_opposite_sides(self, side_a: Any, side_b: Any) -> bool:
        side_a = str(side_a).lower() if side_a else ''
        side_b = str(side_b).lower() if side_b else ''

        is_home_a = ('home' in side_a or '主' in side_a or
                     re.match(r'^h$', side_a) or side_a == '主队')

        is_away_a = ('away' in side_a or '客' in side_a or
                     re.match(r'^a$', side_a) or side_a == '客队')

        is_home_b = ('home' in side_b or '主' in side_b or
                     re.match(r'^h$', side_b) or side_b == '主队')

        is_away_b = ('away' in side_b or '客' in side_b or
                     re.match(r'^a$', side_b) or side_b == '客队')

        return (is_home_a and is_away_b) or (is_away_a and is_home_b)

    # 从上下盘中选择最优投注项
    def select_best_odd_from_opposites(self, opposites: List[Dict[str, Any]], comparison_type: str) -> List[
        Dict[str, Any]]:
        if len(opposites) <= 1:
            return opposites

        # 计算每个盘口的差值 (srcB.avgDecimal - srcA.avgDecimal)
        for odd in opposites:
            src_a_decimal = odd.get('srcA', {}).get('avgDecimal', 0)
            src_b_decimal = odd.get('srcB', {}).get('avgDecimal', 0)
            odd['diff'] = abs(src_b_decimal - src_a_decimal)  # 计算差值的绝对值

        # 按差值降序排序，差值更大的盘口优先保留
        opposites.sort(key=lambda x: x.get('diff', 0), reverse=True)

        # 返回差值最大的一个
        return [opposites[0]]

    # 从联赛名称匹配国家和大洲
    def get_continent_country_from_league(self, league_name: str) -> Dict[str, str]:
        if not league_name or not isinstance(league_name, str):
            return {'country': '未知国家', 'continent': '未知大洲'}

        lower_league = league_name.lower().strip()

        # 遍历所有大洲的国家
        for continent, countries in self.CONTINENT_COUNTRY_MAP.items():
            for country in countries:
                lower_country = country.lower()
                # 检查联赛名称是否包含国家名
                if lower_country in lower_league:
                    return {'country': country, 'continent': continent}

                # 特殊情况处理
                if country == "USA" and "usa" in lower_league:
                    return {'country': country, 'continent': continent}

                if country == "Ivory Coast" and "ivory coast" in lower_league:
                    return {'country': country, 'continent': continent}

                if country == "Cote d'Ivoire" and "cote d'ivoire" in lower_league:
                    return {'country': country, 'continent': continent}

        # 未匹配到任何已知国家/大洲
        return {'country': '未知国家', 'continent': '未知大洲'}

    # 排序工具函数
    def sort_by_field(self, a: Dict[str, Any], b: Dict[str, Any], field: str, direction: str) -> int:
        val_a = a.get(field)
        val_b = b.get(field)

        if field == 'name':
            val_a = str(val_a).lower() if val_a else ''
            val_b = str(val_b).lower() if val_b else ''
            if val_a < val_b:
                return -1 if direction == 'asc' else 1
            elif val_a > val_b:
                return 1 if direction == 'asc' else -1
            return 0

        # 数值比较
        if val_a is None:
            return 1 if direction == 'asc' else -1
        if val_b is None:
            return -1 if direction == 'asc' else 1

        if val_a < val_b:
            return -1 if direction == 'asc' else 1
        elif val_a > val_b:
            return 1 if direction == 'asc' else -1
        return 0

    # 更新排序状态
    def update_sort_state(self, current_state: Dict[str, str], new_field: str) -> Dict[str, str]:
        if current_state['field'] == new_field:
            return {**current_state,
                    'direction': 'asc' if current_state['direction'] == 'desc' else 'desc'}
        return {'field': new_field, 'direction': 'desc'}

    # 新增：同方向盘口过滤核心逻辑
    def filter_odds_by_side(self, match_odds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        按投注方向(side)过滤盘口，选择最优的同方向组并保留最多2个盘口
        """
        try:
            # 空数组或长度小于等于2直接返回
            if not isinstance(match_odds, list) or len(match_odds) <= 2:
                return match_odds or []

            # 按side分组
            side_groups = {}
            for odd in match_odds:
                if not odd or not isinstance(odd, dict):
                    continue  # 跳过无效数据

                side = (odd.get('side') or 'unknown').lower().strip()
                if side not in side_groups:
                    side_groups[side] = []
                side_groups[side].append(odd)

            # 如果没有有效分组，返回原始数据
            side_keys = list(side_groups.keys())
            if not side_keys:
                return match_odds[:2]  # 最多返回2个

            # 如果只有一个side组，直接处理该组
            if len(side_keys) == 1:
                side = side_keys[0]
                return self.filter_same_side_odds(side_groups[side])

            # 多个side组，计算每个组的平均值并选择平均值大的组
            best_side = None
            best_average = -float('inf')

            for side, odds in side_groups.items():
                # 计算该side组的平均赔率
                try:
                    valid_odds = [odd for odd in odds
                                  if odd and odd.get('srcB') and
                                  isinstance(odd['srcB'].get('avgDecimal'), (int, float))]

                    if not valid_odds:
                        continue

                    avg = sum(odd['srcB']['avgDecimal'] for odd in valid_odds) / len(valid_odds)

                    if avg > best_average:
                        best_average = avg
                        best_side = side
                except Exception as e:
                    print(f"计算side组平均值出错: {str(e)}")

            # 如果没有找到最佳side（异常情况），返回原始数据的前2个
            if not best_side:
                return match_odds[:2]

            # 对最佳side组进行过滤，保留最多2个盘口
            return self.filter_same_side_odds(side_groups.get(best_side, []))
        except Exception as e:
            print(f"side过滤逻辑出错: {str(e)}")
            return match_odds[:2] if match_odds else []  # 出错时返回原始数据的前2个

    # 新增：相同side盘口过滤辅助函数
    def filter_same_side_odds(self, odds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        对相同side的盘口进行过滤，最多保留2个最优盘口
        """
        try:
            if not isinstance(odds, list):
                return []
            if len(odds) <= 2:
                return odds

            # 复制并排序数组 - 负数保留绝对值小的，正数保留绝对值大的
            sorted_odds = sorted(odds, key=lambda x: self._sort_odds_key(x))

            # 取前两个
            return sorted_odds[:2]
        except Exception as e:
            print(f"相同side过滤出错: {str(e)}")
            return odds[:2] if odds else []  # 出错时至少返回前2个

    # 新增：排序辅助函数
    def _sort_odds_key(self, odd: Dict[str, Any]) -> float:
        """用于相同side盘口排序的键生成函数"""
        hc = self.safe_number(odd.get('handicap')) or 0

        # 负数：绝对值小的排前面（返回绝对值）
        if hc < 0:
            return abs(hc)

        # 正数：绝对值大的排前面（返回负的绝对值）
        return -abs(hc)

    # 全量计算主函数
    def do_full_scan(self, matches: List[Dict[str, Any]], comparison_type: str) -> Tuple[
        List[Dict[str, Any]], Dict[str, Any]]:
        """
        执行全量计算并返回结果

        参数:
            matches: 赛事数据列表
            comparison_type: 比较类型，'high'或'low'

        返回:
            (results, stats) - 计算结果列表和统计信息
        """
        results = []

        # 初始化统计对象
        stats = {
            'totalScore': 0,
            'totalCount': 0,
            'correctCount': 0,
            'undecidableCount': 0,
            'predictedWinCount': 0,
            'totalProfit': 0.0,
            'totalBetCount': 0,
            'totalStake': 0.0,
            'leagueStats': {},
            'continentStats': {},
            'countryStats': {}
        }

        for match in matches:
            # 获取联赛名称
            league_name = (match.get('league') or match.get('league_name') or
                           match.get('tournament') or match.get('competition') or
                           match.get('leagueTitle') or '未知联赛')

            # 初始化联赛统计
            if league_name not in stats['leagueStats']:
                stats['leagueStats'][league_name] = {
                    'score': 0, 'count': 0, 'correct': 0,
                    'profit': 0.0, 'betCount': 0, 'stake': 0.0
                }

            # 获取国家和大洲信息
            region_info = self.get_continent_country_from_league(league_name)
            country = region_info['country']
            continent = region_info['continent']

            # 初始化大洲统计
            if continent not in stats['continentStats']:
                stats['continentStats'][continent] = {
                    'score': 0, 'count': 0, 'correct': 0,
                    'profit': 0.0, 'betCount': 0, 'stake': 0.0
                }

            # 初始化国家统计
            if country not in stats['countryStats']:
                stats['countryStats'][country] = {
                    'score': 0, 'count': 0, 'correct': 0,
                    'profit': 0.0, 'betCount': 0, 'stake': 0.0
                }

            # 获取赔率列表
            odds_list_raw = self.find_odds_list(match)
            if not odds_list_raw:
                continue

            # 应用盘口级别的早收盘过滤
            odds_list_raw = self.filter_odds_by_early_close(match, odds_list_raw)
            if not odds_list_raw:  # 如果所有盘口都被过滤，则跳过这场比赛
                continue

            # 应用开赛时间过滤
            filter_active = self.filter_minutes is not None and self.filter_minutes > 0
            if filter_active:
                match_start_raw = self.get_match_start_time_raw(match)
                match_start_date = self.parse_date_time(match_start_raw)

                if not match_start_date:
                    stats['undecidableCount'] += 1
                    continue

                # 计算过滤时间范围
                filter_start = match_start_date - timedelta(minutes=self.filter_minutes)
                filter_end = match_start_date

                # 过滤赔率
                filtered_odds = []
                for r in odds_list_raw:
                    t = self.parse_date_time(r.get('time'))
                    if t and filter_start <= t <= filter_end:
                        filtered_odds.append(r)

                odds_list_raw = filtered_odds
                if not odds_list_raw:
                    stats['undecidableCount'] += 1
                    continue

            # 按盘口、让分、方向分组
            groups = {}
            for r in odds_list_raw:
                market = r.get('market')
                if market not in self.selected_markets:
                    continue

                key = f"{market}||{r.get('handicap')}||{r.get('side')}"
                if key not in groups:
                    groups[key] = {
                        'bySource': {},
                        'sample': {
                            'market': market,
                            'handicap': r.get('handicap'),
                            'side': r.get('side'),
                            'match': match
                        }
                    }

                source = r.get('source')
                if source not in groups[key]['bySource']:
                    groups[key]['bySource'][source] = []

                groups[key]['bySource'][source].append(r.get('price'))

            # 处理每个组
            match_odds = []
            for key, group_data in groups.items():
                src_list = self.selected_sources[:2]
                avg_by_source = {}

                # 计算每个来源的平均赔率
                for src in src_list:
                    prices = group_data['bySource'].get(src, [])
                    if not prices:
                        avg_by_source[src] = None
                        continue

                    # 转换为概率并计算平均值
                    probs = []
                    for price in prices:
                        prob = self.malay_to_probability(price)
                        if prob is not None:
                            probs.append(prob)

                    if not probs:
                        avg_by_source[src] = None
                        continue

                    avg_prob = sum(probs) / len(probs)
                    avg_malay = self.probability_to_malay(avg_prob)
                    avg_decimal = 1 / avg_prob if avg_prob != 0 else None

                    avg_by_source[src] = {
                        'avgProb': avg_prob,
                        'avgMalay': avg_malay,
                        'avgDecimal': avg_decimal
                    }

                # 获取两个来源的数据
                src_a, src_b = src_list[0], src_list[1] if len(src_list) > 1 else src_list[0]
                a_data = avg_by_source.get(src_a)
                b_data = avg_by_source.get(src_b)

                if not a_data or not b_data:
                    stats['undecidableCount'] += 1
                    continue

                # 应用赔率过滤
                odds_filter = self.read_odds_filter_params()
                if not self.is_odds_in_range(b_data['avgMalay'], odds_filter['min'], odds_filter['max']):
                    continue

                # 预测胜负
                predicted_win = False
                if comparison_type == 'high':
                    predicted_win = b_data['avgDecimal'] >= a_data['avgDecimal']
                else:
                    predicted_win = b_data['avgDecimal'] <= a_data['avgDecimal']

                # 只保留符合预测条件的盘口
                if predicted_win:
                    match_odds.append({
                        'market': group_data['sample']['market'],
                        'handicap': group_data['sample']['handicap'],
                        'side': group_data['sample']['side'],
                        'match': group_data['sample']['match'],
                        'srcA': {'id': src_a, **a_data},
                        'srcB': {'id': src_b, **b_data},
                        'predictedWin': predicted_win
                    })

            # 识别并处理上下盘
            processed_odds = []
            processed_indices = set()

            # 找出所有上下盘组合
            for i in range(len(match_odds)):
                if i in processed_indices:
                    continue

                current = match_odds[i]
                opposites = [current]

                for j in range(i + 1, len(match_odds)):
                    if j in processed_indices:
                        continue

                    if self.are_opposite_odds(current, match_odds[j]):
                        opposites.append(match_odds[j])
                        processed_indices.add(j)

                # 从上下盘中选择最优的一个
                best_odds = self.select_best_odd_from_opposites(opposites, comparison_type)
                processed_odds.extend(best_odds)
                processed_indices.add(i)

            # 新增：应用同方向盘口过滤（默认开启）
            if self.side_filter_enabled:
                processed_odds = self.filter_odds_by_side(processed_odds)

            # 处理筛选后的盘口
            for item in processed_odds:
                # 判断实际输赢
                actual_win = self.did_side_win(
                    item['match'], item['market'],
                    item['handicap'], item['side']
                )

                if actual_win is None:
                    stats['undecidableCount'] += 1
                    continue

                # 计算评分
                score = self.score_result(item['predictedWin'], actual_win)

                if item['predictedWin']:
                    # 更新总统计
                    stats['totalScore'] += score
                    stats['totalCount'] += 1
                    if score == 1:
                        stats['correctCount'] += 1
                    stats['predictedWinCount'] += 1

                    # 更新联赛统计
                    stats['leagueStats'][league_name]['score'] += score
                    stats['leagueStats'][league_name]['count'] += 1
                    if score == 1:
                        stats['leagueStats'][league_name]['correct'] += 1

                    # 更新大洲统计
                    stats['continentStats'][continent]['score'] += score
                    stats['continentStats'][continent]['count'] += 1
                    if score == 1:
                        stats['continentStats'][continent]['correct'] += 1

                    # 更新国家统计
                    stats['countryStats'][country]['score'] += score
                    stats['countryStats'][country]['count'] += 1
                    if score == 1:
                        stats['countryStats'][country]['correct'] += 1

                # 计算盈亏
                profit_this = None
                used_source = 'B'
                decimal_to_use = item['srcB'].get('avgDecimal')

                if decimal_to_use is None or not isinstance(decimal_to_use, (int, float)) or decimal_to_use <= 0:
                    decimal_to_use = item['srcA'].get('avgDecimal')
                    used_source = 'A(as-fallback)' if decimal_to_use else None

                if not decimal_to_use or not isinstance(decimal_to_use, (int, float)) or decimal_to_use <= 0:
                    stats['undecidableCount'] += 1
                    profit_this = None
                else:
                    # 计算盈亏
                    p = self.compute_profit_for_bet(
                        item['match'], item['market'], item['handicap'],
                        item['side'], self.STAKE_PER_BET, decimal_to_use
                    )

                    if p is None:
                        stats['undecidableCount'] += 1
                        profit_this = None
                    else:
                        profit_this = p
                        if item['predictedWin']:
                            # 更新总盈亏统计
                            stats['totalProfit'] += p
                            stats['totalBetCount'] += 1
                            stats['totalStake'] += self.STAKE_PER_BET

                            # 更新联赛盈亏统计
                            stats['leagueStats'][league_name]['profit'] += p
                            stats['leagueStats'][league_name]['betCount'] += 1
                            stats['leagueStats'][league_name]['stake'] += self.STAKE_PER_BET

                            # 更新大洲盈亏统计
                            stats['continentStats'][continent]['profit'] += p
                            stats['continentStats'][continent]['betCount'] += 1
                            stats['continentStats'][continent]['stake'] += self.STAKE_PER_BET

                            # 更新国家盈亏统计
                            stats['countryStats'][country]['profit'] += p
                            stats['countryStats'][country]['betCount'] += 1
                            stats['countryStats'][country]['stake'] += self.STAKE_PER_BET

                # 添加到结果列表
                match_obj = item['match']
                home_team = match_obj.get('home_team', '未知主队')
                away_team = match_obj.get('away_team', '未知客队')
                match_title = f"{home_team} vs {away_team}"

                results.append({
                    'match': match_title,
                    'league': league_name,
                    'country': country,
                    'continent': continent,
                    'market': item['market'],
                    'handicap': item['handicap'],
                    'side': item['side'],
                    'srcA': {
                        'id': item['srcA']['id'],
                        'avgMalay': item['srcA']['avgMalay'],
                        'avgDecimal': item['srcA']['avgDecimal']
                    },
                    'srcB': {
                        'id': item['srcB']['id'],
                        'avgMalay': item['srcB']['avgMalay'],
                        'avgDecimal': item['srcB']['avgDecimal']
                    },
                    'predictedWin': item['predictedWin'],
                    'actualWin': actual_win,
                    'score': score,
                    'profit': profit_this,
                    'profitSource': used_source
                })

        return results, stats

    # 打印结果
    def print_results(self, results: List[Dict[str, Any]], stats: Dict[str, Any], title: str) -> None:
        """打印计算结果和统计信息"""
        print(f"\n====== [{title}] 全量计算结果汇总 ======")
        print(f"预测赢为是: {stats['predictedWinCount']}")
        print(f"正确: {stats['correctCount']}")
        print(f"错误: {stats['totalCount'] - stats['correctCount']}")
        win_rate = (stats['correctCount'] / stats['totalCount'] * 100
                    if stats['totalCount'] > 0 else 0)
        print(f"胜率: {win_rate:.2f}%")
        print(f"总评分: {stats['totalScore']}")
        print(f"无法判定: {stats['undecidableCount']}")
        print(f"总投注场次: {stats['totalBetCount']}")
        print(f"总投注金额: {stats['totalStake']:.2f}")
        print(f"总盈亏: {stats['totalProfit']:.2f}")
        roi = (stats['totalProfit'] / stats['totalStake'] * 100
               if stats['totalStake'] > 0 else 0)
        print(f"ROI: {roi:.2f}%")

        # 打印联赛统计
        print(f"\n====== [{title}] 联赛统计 ======")
        league_stats = []
        for name, data in stats['leagueStats'].items():
            win_rate = (data['correct'] / data['count'] * 100 if data['count'] > 0 else 0)
            roi = (data['profit'] / data['stake'] * 100 if data['stake'] > 0 else 0)
            league_stats.append({
                'name': name,
                'count': data['count'],
                'score': data['score'],
                'winRate': win_rate,
                'profit': data['profit'],
                'roi': roi
            })

        # 按当前排序状态排序
        league_stats.sort(key=lambda x: self.sort_by_field(
            x, {}, self.league_sort_state['field'], self.league_sort_state['direction']
        ))

        # 打印表头
        print(f"{'联赛名称':<30} {'投注场次':>8} {'总评分':>6} {'胜率(%)':>8} {'总盈亏':>10} {'ROI(%)':>8}")
        print("-" * 80)
        for league in league_stats:
            print(f"{league['name']:<30} {league['count']:>8} {league['score']:>6} "
                  f"{league['winRate']:>8.2f} {league['profit']:>10.2f} {league['roi']:>8.2f}")

    # 日期范围工具函数
    def get_date_range(self, days: int) -> Tuple[str, str]:
        """获取日期范围：end_date=今天，start_date=days天前"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        return start_date, end_date

    # 内部单一模式计算
    def _perform_single_calculation(self, matches, comparison_type, is_7d=False):
        """执行单一模式计算"""
        try:
            print(f"[{'7天' if is_7d else '30天'} | {comparison_type}] 正在计算...")
            results, stats = self.do_full_scan(matches, comparison_type)
            self.print_results(results, stats, f"{'7天' if is_7d else '30天'} | {comparison_type}")

            # 保存结果
            if self.save_results_to_db(stats, stats['leagueStats'], comparison_type, is_7d):
                print(f"[{'7天' if is_7d else '30天'} | {comparison_type}] 计算成功")
                return {'success': True, 'message': f"{'7天' if is_7d else '30天'}-{comparison_type}成功",
                        'stats': stats}
            else:
                return {'success': False, 'message': f"{'7天' if is_7d else '30天'}-{comparison_type}保存失败"}
        except Exception as e:
            return {'success': False, 'message': f"{'7天' if is_7d else '30天'}-{comparison_type}错误: {str(e)}"}
        finally:
            # 确保计算完成后关闭当前线程的数据库连接
            self.close_db_connection()

    # 按天数计算
    def perform_calculation_by_days(self, days: int, max_retries=3):
        """按天数计算（30/7天通用）"""
        is_7d = days == 7
        if days not in [7, 30]:
            return {'success': False, 'message': f"不支持的时间范围：{days}天，仅支持7天和30天"}

        retries = 0
        while retries < max_retries:
            try:
                # 获取指定天数的日期范围
                start_date, end_date = self.get_date_range(days)
                print(f"[{'7天' if is_7d else '30天'}] 获取 {start_date} 至 {end_date} 数据...")

                # 一次获取数据
                success, matches, message = self.fetch_data_from_api(
                    start_date=start_date,
                    end_date=end_date,
                    source_filter=[1, 2]
                )
                if not success:
                    raise Exception(f"获取数据失败: {message}")

                # 同时计算high和low模式
                high_result = self._perform_single_calculation(matches, 'high', is_7d)
                low_result = self._perform_single_calculation(matches, 'low', is_7d)

                return {
                    'success': high_result['success'] and low_result['success'],
                    'message': f"{'7天' if is_7d else '30天'}：high={high_result['message']}; low={low_result['message']}",
                    'high': high_result,
                    'low': low_result,
                    'days': days
                }
            except Exception as e:
                retries += 1
                if retries < max_retries:
                    print(f"[{'7天' if is_7d else '30天'}] 重试({retries}/{max_retries})...")
                    time.sleep(5)
                else:
                    return {'success': False, 'message': f"{'7天' if is_7d else '30天'}重试耗尽: {str(e)}"}
            finally:
                # 确保重试循环中关闭连接
                self.close_db_connection()

    # 原有30天计算兼容
    def perform_both_calculations_with_retry(self, max_retries=3):
        """（兼容原有调用）30天数据计算"""
        return self.perform_calculation_by_days(days=30, max_retries=max_retries)


# Flask接口：触发计算（同时计算30天和7天）
@app.route('/api/calculate', methods=['POST'])
def trigger_calculation():
    """触发计算：同时计算30天和7天数据（high/low模式各算一次）"""
    global analyzer
    try:
        print("=== 开始触发30天+7天计算 ===")
        # 并行计算30天和7天（用线程提高效率）
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_30d = executor.submit(analyzer.perform_calculation_by_days, days=30)
            future_7d = executor.submit(analyzer.perform_calculation_by_days, days=7)

            result_30d = future_30d.result()
            result_7d = future_7d.result()

        # 汇总结果
        overall_success = result_30d['success'] and result_7d['success']
        return jsonify({
            'success': overall_success,
            'message': f"30天: {result_30d['message']}; 7天: {result_7d['message']}",
            'results': {
                '30d': result_30d,
                '7d': result_7d
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f"触发计算错误: {str(e)}"
        }), 500


# 新增：获取7天数据结果接口
@app.route('/api/latest-7d-results', methods=['GET'])
def get_latest_7d_results():
    """新接口：获取7天数据结果"""
    global analyzer
    try:
        results = analyzer.get_latest_7d_results_from_db()
        if results is None:
            return jsonify({
                'success': False,
                'message': '获取7天数据失败'
            }), 500

        return jsonify({
            'success': True,
            'data': results
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取7天数据过程中发生错误: {str(e)}'
        }), 500


# 原有接口：获取最新数据（30天）
@app.route('/api/latest-results', methods=['GET'])
def get_latest_results():
    """获取最新数据（30天，保持原有逻辑不变）"""
    global analyzer
    try:
        results = analyzer.get_latest_results_from_db()
        if results is None:
            return jsonify({
                'success': False,
                'message': '获取数据失败'
            }), 500

        return jsonify({
            'success': True,
            'data': results
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取数据过程中发生错误: {str(e)}'
        }), 500


# 定时任务执行函数
def run_scheduled_tasks():
    """定时任务：同时计算30天和7天数据"""
    global analyzer
    print(f"\n====== 开始执行定时任务: {datetime.now()} ======")
    print(f"使用固定早收盘过滤参数: {analyzer.early_close_filter_minutes}分钟")

    # 并行计算30天和7天数据
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(analyzer.perform_calculation_by_days, days=30)
        executor.submit(analyzer.perform_calculation_by_days, days=7)

    print(f"\n====== 定时任务执行完成: {datetime.now()} ======")


# 启动定时任务线程
def start_scheduler_thread():
    """在后台线程中启动定时任务"""

    def scheduler_loop():
        print("启动定时任务，每天北京时间12:20执行计算...")
        # 每天中午13点执行任务
        schedule.every().day.at("12:20").do(run_scheduled_tasks)

        # 保持循环运行
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    # 创建并启动线程
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()


# 启动Flask服务器线程
def start_flask_server():
    """在后台线程中启动Flask服务器"""

    def flask_server():
        print("启动API服务器，监听端口5070...")
        app.run(host='0.0.0.0', port=5070, debug=False, use_reloader=False)

    # 创建并启动线程
    flask_thread = threading.Thread(target=flask_server, daemon=True)
    flask_thread.start()


# 主函数：启动挂机模式
def main():
    global analyzer

    # 初始化分析器
    analyzer = SportsOddsAnalyzer()

    # 初始化数据库
    print("初始化数据库...")
    analyzer.init_database()

    # 设置参数
    analyzer.selected_markets = ['spread']  # 分析让分盘
    analyzer.filter_minutes = 5  # 不限制时间
    analyzer.min_odds = None  # 不限制最低赔率
    analyzer.max_odds = None  # 不限制最高赔率
    # 早收盘过滤参数已在类初始化时设置为全局固定值

    print(f"系统启动，使用早收盘过滤参数: {analyzer.early_close_filter_minutes}分钟")
    print(f"同方向盘口过滤已{'' if analyzer.side_filter_enabled else '未'}开启")

    # 启动定时任务线程
    start_scheduler_thread()

    # 启动Flask服务器线程
    start_flask_server()

    # 主进程保持运行（挂机模式）
    print("系统启动完成，进入挂机模式...")
    try:
        while True:
            time.sleep(3600)  # 每小时检查一次
    except KeyboardInterrupt:
        print("系统正在关闭...")


if __name__ == "__main__":
    main()
