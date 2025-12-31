import os
import threading
import queue
import time
import re  # 用于提取盘口数值
from flask import Flask, request, jsonify
import requests
import json
from flask_cors import CORS
import psycopg2
import datetime
from datetime import datetime, timedelta
import pytz
from psycopg2.extras import RealDictCursor

# Flask app
app = Flask(__name__)
CORS(app)

# -------------------------- 全局过滤缓存与配置 --------------------------
# 缓存结构：key = "模式|联赛名|主队|客队|比赛类型"，value = [{"spread_num": 盘口数值, "odds_name": 赔率名, "cache_time": 存入时间}]
GLOBAL_BET_CACHE = {}
# 缓存锁：保证多线程操作缓存安全
CACHE_LOCK = threading.Lock()
# 缓存有效期：24小时（单位：秒），避免长期占用内存
CACHE_EXPIRE_SEC = 86400
# 盘口类型匹配正则：提取SPREAD_FT_后的数字（支持正负、整数/小数）
SPREAD_PATTERN = re.compile(r"SPREAD_FT_([+-]?\d+\.?\d*)")

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "223658",
    "port": 5432
}

# 账号与对应的目标服务器IP映射
ACCOUNT_SERVERS = {
    "ch9aghk": "154.219.101.74",
    "ch9al7d": "45.207.210.21",
    "ch9agob": "103.106.188.8",
    "ch9a6h4": "103.106.191.104",
    "ch9a5b1": "103.242.13.77"
}
LOW_ACCOUNT_SERVERS = {
    "ch9ajDD": "154.222.29.75",
    "ch9aoDD": "154.222.29.84",
    "ch9a8DD": "154.222.29.89",
    "ch9aqDD": "154.222.29.139",
    "ch9amDD": "154.222.29.252"
}
# 修改：min模式账号与服务器映射（原all模式）
MIN_ACCOUNT_SERVERS = {
    "ch9accc11": "103.38.80.212",
    "ch9accc22": "103.106.188.150",
    "ch9accc33": "103.106.188.170",
    "ch9accc44": "103.106.191.117",
    "ch9accc55": "103.242.13.102",
    "ch9accc66": "114.134.184.87",
    "ch9accc77": "114.134.184.100",
    "ch9accc88": "103.38.80.233",
}

# 核心修改：各模式独立默认投注金额（可自行修改数值）
HIGH_DEFAULT_BET_AMOUNT = 100  # high模式默认金额
LOW_DEFAULT_BET_AMOUNT = 100  # low模式默认金额
MIN_DEFAULT_BET_AMOUNT = 1000  # min模式默认金额
TARGET_PATH = "/receive_signal"

# 请求队列
HIGH_REQUEST_QUEUE = queue.Queue()
LOW_REQUEST_QUEUE = queue.Queue()
# 修改：min模式请求队列（原all模式队列）
MIN_REQUEST_QUEUE = queue.Queue()
# 工作线程数量：包含min模式账号（原all模式）
WORKER_COUNT = len(ACCOUNT_SERVERS) + len(LOW_ACCOUNT_SERVERS) + len(MIN_ACCOUNT_SERVERS)


# -------------------------- 缓存操作工具函数 --------------------------
def clean_expired_cache():
    """清理过期的缓存记录（线程安全）"""
    current_time = time.time()
    with CACHE_LOCK:
        # 遍历并删除过期的比赛缓存
        expired_keys = [
            key for key, records in GLOBAL_BET_CACHE.items()
            if any(current_time - record["cache_time"] > CACHE_EXPIRE_SEC for record in records)
        ]
        for key in expired_keys:
            del GLOBAL_BET_CACHE[key]
            print(f"缓存清理：删除过期比赛记录 -> {key}")


def check_bet_conflict(league, home_team, away_team, match_type, spread_num, odds_name, alert_type):
    """
    检查当前投注是否与缓存中上下盘冲突（按模式区分缓存）
    :param league: 联赛名
    :param home_team: 主队
    :param away_team: 客队
    :param match_type: 比赛类型
    :param spread_num: 盘口数值（float）
    :param odds_name: 赔率名（如HomeOdds、AwayOdds）
    :param alert_type: 模式（high/low/min）
    :return: (是否冲突, 冲突原因)
    """
    # 标准化名称：去除空格、统一小写，避免因格式差异误判
    league = league.strip().replace(" ", "").lower() if league else ""
    home_team = home_team.strip().replace(" ", "").lower() if home_team else ""
    away_team = away_team.strip().replace(" ", "").lower() if away_team else ""
    match_type = match_type.strip().lower() if match_type else ""
    alert_type = alert_type.strip().lower() if alert_type else "high"  # 默认high模式

    # 1. 生成比赛唯一标识（加入模式维度，按模式区分缓存）
    match_key = f"{alert_type}|{league}|{home_team}|{away_team}|{match_type}"
    if not match_key.replace("|", ""):  # 若所有字段为空，无法识别比赛，直接放行
        return False, "比赛信息不完整，跳过冲突检查"

    # 2. 先清理过期缓存
    clean_expired_cache()

    with CACHE_LOCK:
        # 3. 检查该比赛（同模式）是否有已投注记录
        existing_records = GLOBAL_BET_CACHE.get(match_key, [])
        if not existing_records:
            # 无记录：直接通过，并存入缓存
            GLOBAL_BET_CACHE[match_key] = [
                {"spread_num": spread_num, "odds_name": odds_name, "cache_time": time.time()}]
            print(f"缓存新增：{match_key} -> 盘口{spread_num}, 赔率{odds_name}")
            return False, ""

        # 4. 有记录：按规则判断冲突（仅在同模式内检查）
        for record in existing_records:
            old_spread = record["spread_num"]
            old_odds = record["odds_name"]

            # 新增：重复盘口判断（同一模式内、同一盘口+同一赔率名，仅允许一次）
            if spread_num == old_spread and odds_name == old_odds:
                return True, f"[{alert_type}]盘口重复：已存在相同盘口{old_spread}({old_odds})，禁止重复转发"

            # 规则1：0盘口 → 仅判断赔率名是否不同（同模式内）
            if spread_num == 0 and old_spread == 0:
                if odds_name != old_odds:
                    return True, f"[{alert_type}]0盘口冲突：已存在{old_odds}，当前{odds_name}"
                continue  # 赔率名相同，无冲突

            # 规则2：非0盘口 → 数值符号相反 + 赔率名不同（同模式内）
            if (spread_num == -old_spread) and (odds_name != old_odds):
                return True, f"[{alert_type}]上下盘冲突：已存在{old_spread}({old_odds})，当前{spread_num}({odds_name})"

        # 5. 无冲突：更新缓存（添加当前记录）
        existing_records.append({"spread_num": spread_num, "odds_name": odds_name, "cache_time": time.time()})
        GLOBAL_BET_CACHE[match_key] = existing_records
        print(f"缓存更新：{match_key} -> 新增盘口{spread_num}, 赔率{odds_name}")
        return False, ""


def clean_numeric_str(num_str):
    """清洗带千分位逗号的数值字符串，转为PostgreSQL可识别的格式"""
    if isinstance(num_str, str):
        # 去除逗号、首尾空格，空值返回None
        cleaned = num_str.replace(",", "").strip()
        return cleaned if cleaned else None
    # 非字符串类型直接返回（如int/float/None）
    return num_str

# -------------------------- 数据库函数 --------------------------
def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS betting_requests (
                id SERIAL PRIMARY KEY,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                account VARCHAR(50),
                account_used VARCHAR(50),
                alert_league_name VARCHAR(255),
                alert_home_team VARCHAR(100),
                alert_away_team VARCHAR(100),
                alert_bet_type_name VARCHAR(50),
                alert_odds_name VARCHAR(50),
                alert_match_type VARCHAR(50),
                bet_amount NUMERIC(10,2),
                alert_type VARCHAR(20),
                raw_data JSONB NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS betting_responses (
                id SERIAL PRIMARY KEY,
                request_id INTEGER REFERENCES betting_requests(id),
                response_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status_code INTEGER,
                response_status VARCHAR(20),
                response_message TEXT,
                receipt_order_result TEXT,
                receipt_ior NUMERIC(5,2),
                receipt_stake NUMERIC(10,2),
                receipt_win_gold NUMERIC(10,2),
                raw_response JSONB NOT NULL
            )
        """)
    conn.commit()
    conn.close()


def save_request_data(data, account=None, account_used=None):
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO betting_requests (
                alert_league_name, alert_home_team, alert_away_team,
                alert_bet_type_name, alert_odds_name, alert_match_type,
                bet_amount, alert_type, raw_data, account, account_used
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            data.get('alert', {}).get('league_name'),
            data.get('alert', {}).get('home_team'),
            data.get('alert', {}).get('away_team'),
            data.get('alert', {}).get('bet_type_name'),
            data.get('alert', {}).get('odds_name'),
            data.get('alert', {}).get('match_type'),
            data.get('bet_amount'),
            data.get('alert_type'),
            json.dumps(data),
            account,
            account_used
        ))
        req_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return req_id


def save_response_data(request_id, status_code, response_data):
    conn = psycopg2.connect(**DB_CONFIG)
    receipt = response_data.get('receipt', {})
    # 核心修改：清洗带逗号的数值字段
    ior = clean_numeric_str(receipt.get('ior'))
    stake = clean_numeric_str(receipt.get('stake'))
    win_gold = clean_numeric_str(receipt.get('win_gold'))

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO betting_responses (
                request_id, status_code, response_status, response_message,
                receipt_order_result, receipt_ior, receipt_stake, receipt_win_gold, raw_response
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            request_id,
            status_code,
            response_data.get('status'),
            response_data.get('message'),
            receipt.get('order_result'),
            ior,  # 清洗后的值
            stake,  # 清洗后的值
            win_gold,  # 清洗后的值
            json.dumps(response_data)
        ))
    conn.commit()
    conn.close()


# -------------------------- 工作线程与API --------------------------
def worker(account, server_ip, mode_queue):
    print(f"工作线程启动，账号: {account}, 服务器IP: {server_ip}, 模式队列: {mode_queue}")
    while True:
        try:
            data, original_account, request_id = mode_queue.get(block=True)
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE betting_requests 
                        SET account = %s, account_used = %s 
                        WHERE id = %s
                    """, (account, account, request_id))
                conn.commit()
                conn.close()

                target_url = f"http://{server_ip}:5031{TARGET_PATH}"
                print(f"账号 {account} 转发请求到 {target_url}, 请求ID: {request_id}")
                response = requests.post(target_url, json=data, timeout=60)
                resp_json = response.json()
                status_code = response.status_code

                receipt = resp_json.get('receipt', {})
                order_result = receipt.get('order_result', '')
                if "SUCCESSFULLY" in str(order_result).upper():
                    print(f"账号 {account} 转发成功（业务成功）, 请求ID: {request_id}")
                    save_response_data(request_id, status_code, resp_json)
                else:
                    error_msg = f"业务处理失败: order_result='{order_result}'"
                    print(f"账号 {account} 转发失败（{error_msg}）, 请求ID: {request_id}")
                    save_response_data(
                        request_id,
                        status_code,
                        {"status": "business_failure", "message": error_msg, "raw_resp": resp_json}
                    )
            except Exception as e:
                error_msg = f"转发过程出错: {str(e)}"
                print(f"账号 {account} 转发失败（{error_msg}）, 请求ID: {request_id}")
                save_response_data(
                    request_id,
                    500,
                    {"status": "network_error", "message": error_msg}
                )
            finally:
                mode_queue.task_done()
        except Exception as e:
            print(f"工作线程 {account} 出错: {str(e)}")
            time.sleep(1)


def start_workers():
    for account, server_ip in ACCOUNT_SERVERS.items():
        thread = threading.Thread(
            target=worker,
            args=(account, server_ip, HIGH_REQUEST_QUEUE),
            daemon=True
        )
        thread.start()
    for account, server_ip in LOW_ACCOUNT_SERVERS.items():
        thread = threading.Thread(
            target=worker,
            args=(account, server_ip, LOW_REQUEST_QUEUE),
            daemon=True
        )
        thread.start()
    # 修改：启动min模式线程（原all模式线程）
    for account, server_ip in MIN_ACCOUNT_SERVERS.items():
        thread = threading.Thread(
            target=worker,
            args=(account, server_ip, MIN_REQUEST_QUEUE),  # 绑定min模式队列
            daemon=True
        )
        thread.start()


# -------------------------- 请求接口 --------------------------
@app.route("/proxy_bet_request", methods=["POST"])
def proxy_request():
    try:
        data = request.get_json(force=True)
        alert_data = data.get('alert', {})
        # 获取模式类型（high/low/min），用于按模式区分缓存
        alert_type = data.get('alert_type', '').lower()

        # 提取上下盘判断所需字段
        league = alert_data.get('league_name')
        home_team = alert_data.get('home_team')
        away_team = alert_data.get('away_team')
        match_type = alert_data.get('match_type')
        bet_type_name = alert_data.get('bet_type_name', '')
        odds_name = alert_data.get('odds_name', '')

        # 提取盘口数值（仅处理SPREAD_FT类型）
        spread_match = SPREAD_PATTERN.search(bet_type_name)
        if not spread_match:
            # 非SPREAD_FT类型，无需上下盘过滤，直接放行
            pass
        else:
            # 提取并转换盘口数值（如"SPREAD_FT_-0.25" → -0.25）
            try:
                spread_num = float(spread_match.group(1))
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": f"盘口数值解析失败: {bet_type_name}",
                    "code": 400
                }), 400

            # 检查上下盘冲突（传入alert_type，按模式区分缓存）
            conflict, conflict_msg = check_bet_conflict(
                league=league,
                home_team=home_team,
                away_team=away_team,
                match_type=match_type,
                spread_num=spread_num,
                odds_name=odds_name,
                alert_type=alert_type  # 核心修改：传入模式参数
            )
            if conflict:
                return jsonify({
                    "status": "conflict",
                    "message": f"上下盘投注冲突：{conflict_msg}",
                    "code": 403  # 403表示禁止访问（冲突）
                }), 403

        # 核心修改：根据模式选择对应默认金额
        if alert_type == 'low':
            default_amount = LOW_DEFAULT_BET_AMOUNT
        elif alert_type == 'min':
            default_amount = MIN_DEFAULT_BET_AMOUNT
        else:  # 默认high模式
            default_amount = HIGH_DEFAULT_BET_AMOUNT
        data["bet_amount"] = data.get("bet_amount", default_amount)

        print(f"添加bet_amount后的数据：{json.dumps(data, ensure_ascii=False)}")

        request_id = save_request_data(data)

        # 修改：判断min模式（原all模式）
        target_queue = MIN_REQUEST_QUEUE if alert_type == 'min' else (  # 绑定min模式队列
            LOW_REQUEST_QUEUE if alert_type == 'low' else HIGH_REQUEST_QUEUE
        )
        target_queue.put((data, None, request_id))
        queue_size = target_queue.qsize()

        return jsonify({
            "status": "success",
            "message": f"请求已接收，正在{alert_type or 'high'}模式队列处理",
            "request_id": request_id,
            "queue_position": queue_size,
            "used_bet_amount": data["bet_amount"],
            "code": 200
        }), 200

    except Exception as e:
        error_msg = f"接收数据失败：{str(e)}"
        print(error_msg)
        return jsonify({
            "status": "error",
            "message": error_msg,
            "code": 400
        }), 400


@app.route("/get_order_details", methods=["GET"])
def get_order_details():
    try:
        request_id = request.args.get('request_id', type=int)
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)

        if now.hour >= 12:
            start_beijing = now.replace(hour=12, minute=0, second=0, microsecond=0)
        else:
            start_beijing = (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        end_beijing = start_beijing + timedelta(hours=24)

        # 范围过滤仍用UTC转换（维持原逻辑）
        start_utc = start_beijing.astimezone(pytz.UTC)
        end_utc = end_beijing.astimezone(pytz.UTC)

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            base_sql = """
                SELECT 
                    r.id as request_id,
                    r.request_time as request_time_utc,  -- 直接返回原始UTC时间（无时区转换）
                    r.account_used as used_account,
                    r.alert_league_name as league,
                    r.alert_home_team as home_team,
                    r.alert_away_team as away_team,
                    r.alert_bet_type_name as bet_type,
                    r.alert_odds_name as odds,
                    r.bet_amount as bet_amount,
                    r.alert_type as mode,
                    res.response_time as response_time_utc,  -- 直接返回原始UTC时间（无时区转换）
                    res.status_code as status_code,
                    res.response_status as result_status,
                    res.receipt_order_result as order_result,
                    res.receipt_stake as stake,
                    res.receipt_win_gold as win_gold
                FROM betting_requests r
                LEFT JOIN betting_responses res ON r.id = res.request_id
                WHERE r.request_time BETWEEN %s AND %s
                AND res.receipt_order_result IS NOT NULL
                AND res.receipt_order_result ILIKE %s
            """
            success_param = '%SUCCESSFULLY%'
            if request_id:
                sql = base_sql + " AND r.id = %s"
                cursor.execute(sql, (start_utc, end_utc, success_param, request_id))
            else:
                cursor.execute(base_sql, (start_utc, end_utc, success_param))
            results = cursor.fetchall()

        # 对原始UTC时间做简单格式化（可选，保证可读性），不转换时区
        for item in results:
            # 处理request_time_utc（原始UTC时间，转ISO格式字符串）
            if item.get('request_time_utc'):
                item['request_time'] = item.pop('request_time_utc').isoformat() + 'Z'  # 标记UTC时区
            # 处理response_time_utc（原始UTC时间，转ISO格式字符串）
            if item.get('response_time_utc'):
                item['response_time'] = item.pop('response_time_utc').isoformat() + 'Z'  # 标记UTC时区

        conn.close()

        return jsonify({
            "status": "success",
            "message": f"查询到{len(results)}条成功的订单",
            "time_range": {
                "start": start_beijing.strftime("%Y-%m-%d %H:%M:%S") + " (Asia/Shanghai)",  # 保留范围说明
                "end": end_beijing.strftime("%Y-%m-%d %H:%M:%S") + " (Asia/Shanghai)"
            },
            "count": len(results),
            "data": results
        }), 200

    except Exception as e:
        error_msg = f"查询失败：{str(e)}"
        print(error_msg)
        return jsonify({
            "status": "error",
            "message": error_msg,
            "code": 500
        }), 500


if __name__ == "__main__":
    create_tables()
    start_workers()
    print(f"启动 {WORKER_COUNT} 个工作线程处理请求")
    print(f"可用账号与服务器: {ACCOUNT_SERVERS}")
    app.run(host="0.0.0.0", port=5030)