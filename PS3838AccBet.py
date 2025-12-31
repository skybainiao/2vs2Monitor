import psycopg2
import psycopg2.errors
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json
from datetime import datetime
import traceback
import urllib.parse
import threading  # 新增：导入线程模块
import os
import base64
from datetime import datetime, timedelta, timezone
from requests.auth import HTTPBasicAuth  # 新增：更可靠的Basic Auth

# 初始化Flask应用
app = Flask(__name__)
CORS(app)  # 保留CORS配置
PS3838_API_BASE_URL = "https://api.ps3838.com"  # 修正了原始代码中的空格问题
PS3838_API_ENDPOINT = "/v2/line"
PS3838_USERNAME = os.getenv("PS3838_API_USER", "H620803004")  # 使用环境变量，提供默认值
PS3838_PASSWORD = os.getenv("PS3838_API_PASS", "dddd1111")    # 使用环境变量，提供默认值
PS3838_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "PS3838-API-Client/1.0"
}
# 数据库配置（复用你的配置）
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}

# PS3838投注服务默认端口
PS3838_DEFAULT_PORT = 5041

# 盘口类型与目标接口映射
BET_TYPE_MAP = {
    "moneyline": "/api/bet/moneyline",  # 金钱线
    "spread": "/api/bet/spread",  # 让分盘
    "total": "/api/bet/total"  # 大小球（总分盘）
}

# 各盘口必填参数校验
BET_REQUIRED_PARAMS = {
    "moneyline": ["line_id", "event_id"],
    "spread": ["line_id", "event_id", "handicap"],
    "total": ["line_id", "event_id", "side", "handicap"]
}


# ------------------------ 原有数据库核心函数（保留，适配ps38accounts表） ------------------------
def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        return None


def init_account_table():
    """初始化独立表（表名ps38accounts，绝不触碰原有表）"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        # 表名改为ps38accounts，仅创建不存在的表
        create_sql = """
        CREATE TABLE IF NOT EXISTS ps38accounts (
            username VARCHAR(50) PRIMARY KEY,
            password VARCHAR(50) NOT NULL,
            link_ip VARCHAR(50) NOT NULL,
            balance NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
             rate NUMERIC(6, 4) NOT NULL,  -- 关键修改：从(5,2)改为(6,4)，支持4位小数
            single_max NUMERIC(10, 2) NOT NULL,
            total_max NUMERIC(10, 2) NOT NULL,
            group_name VARCHAR(50) NOT NULL,
            remark TEXT,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_sql)
        cursor.close()
        conn.close()
        return True
    except psycopg2.Error as e:
        print(f"新表ps38accounts初始化失败：{e}")
        return False


# ------------------------ 新增：投注记录相关数据库函数（表名ps38bet_records） ------------------------
def init_bet_records_table():
    """初始化投注记录表（表名带ps38前缀：ps38bet_records）"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        # 修正：PostgreSQL注释用--，新增字段后加逗号，修复CONSTRAINT语法错误
        create_sql = """
        CREATE TABLE IF NOT EXISTS ps38bet_records (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL REFERENCES ps38accounts(username),
            bet_type VARCHAR(20) NOT NULL,  -- moneyline/spread/total
            request_data TEXT NOT NULL,     -- 前端请求参数（JSON字符串）
            response_data TEXT,             -- 目标服务器返回数据（JSON字符串）
            target_ip VARCHAR(50) NOT NULL, -- 转发的目标IP
            target_port INT NOT NULL DEFAULT %s,
            target_path VARCHAR(100) NOT NULL, -- 转发的目标接口路径
            bet_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,    -- success/failed/pending
            error_msg TEXT,                 -- 错误信息（失败时填充）
            -- ========== 新增字段 START ==========
            league_name VARCHAR(100) NOT NULL DEFAULT '未知联赛', -- 联赛名称
            home_team VARCHAR(100) NOT NULL, -- 主队名称
            away_team VARCHAR(100) NOT NULL, -- 客队名称
            handicap_value NUMERIC(10,2) NOT NULL, -- 盘口值
            bet_direction VARCHAR(20) NOT NULL, -- 投注方向
            bet_category VARCHAR(20) NOT NULL, -- 盘口种类（spread/total）【关键：加逗号】
            -- ========== 新增字段 END ==========
            CONSTRAINT fk_ps38_bet_record_account FOREIGN KEY (username) REFERENCES ps38accounts(username)
        );
        """
        cursor.execute(create_sql, (PS3838_DEFAULT_PORT,))
        cursor.close()
        conn.close()
        print("✅ 投注记录表ps38bet_records初始化成功（含新增字段）")
        return True
    except psycopg2.Error as e:
        print(f"初始化投注记录表ps38bet_records失败：{str(e)}")
        return False


def save_bet_record(record_data):
    """保存投注记录到ps38bet_records表（含新增字段）"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        # ========== 新增字段：解析request_data中的新增字段 ==========
        request_data = json.loads(record_data["request_data"])
        league_name = request_data.get("league_name", "未知联赛")
        home_team = request_data.get("home_team", "")
        away_team = request_data.get("away_team", "")
        handicap_value = request_data.get("handicap_value", 0.0)
        bet_direction = request_data.get("bet_direction", "")
        bet_category = request_data.get("bet_category", "")

        # ========== 新增字段：修改INSERT语句 ==========
        insert_sql = """
        INSERT INTO ps38bet_records (
            username, bet_type, request_data, response_data,
            target_ip, target_port, target_path, status, error_msg,
            league_name, home_team, away_team, handicap_value, bet_direction, bet_category
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (
            record_data["username"],
            record_data["bet_type"],
            record_data["request_data"],
            record_data["response_data"],
            record_data["target_ip"],
            record_data["target_port"],
            record_data["target_path"],
            record_data["status"],
            record_data.get("error_msg", ""),
            # ========== 新增字段：传递值 ==========
            league_name,
            home_team,
            away_team,
            handicap_value,
            bet_direction,
            bet_category
        ))
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"保存投注记录到ps38bet_records失败：{str(e)}")
        return False


# ------------------------ 新增：余额查询与更新函数 ------------------------
def get_single_account_balance(link_ip):
    """
    调用目标服务器余额接口，提取availableBalance
    :param link_ip: 账号绑定的link_ip（支持http://IP:PORT格式）
    :return: availableBalance（float）或None
    """
    try:
        # 复用原有IP/端口解析逻辑，保证兼容性
        parsed_url = urllib.parse.urlparse(link_ip)
        scheme = parsed_url.scheme if parsed_url.scheme else "http"
        netloc = parsed_url.netloc if parsed_url.netloc else link_ip

        if ":" in netloc:
            target_ip, target_port = netloc.split(":", 1)
            target_port = int(target_port)
        else:
            target_ip = netloc
            target_port = PS3838_DEFAULT_PORT

        # 构造余额查询URL（对接目标服务器的/api/account/balance接口）
        balance_url = f"{scheme}://{target_ip}:{target_port}/api/account/balance"

        # 发送GET请求获取余额（短超时，避免阻塞）
        response = requests.get(
            url=balance_url,
            timeout=5,  # 5秒超时，不影响主流程
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )

        # 解析响应，提取availableBalance
        if response.status_code == 200:
            response_data = response.json()
            official_resp = response_data.get("data", {}).get("official_api_response", {})
            available_balance = official_resp.get("availableBalance")
            if available_balance is not None:
                return float(available_balance)
        return None
    except Exception as e:
        print(f"❌ 获取账号余额失败（link_ip：{link_ip}）：{str(e)}")
        return None


def update_all_accounts_balance():
    """
    遍历所有账号，查询最新余额并更新到ps38accounts的balance字段
    （仅更新availableBalance，不修改其他字段，异常不影响主流程）
    """
    conn = get_db_connection()
    if not conn:
        print("❌ 数据库连接失败，跳过余额更新")
        return

    try:
        cursor = conn.cursor()
        # 查询所有账号的用户名和绑定IP
        cursor.execute("SELECT username, link_ip FROM ps38accounts;")
        accounts = cursor.fetchall()

        # 逐个更新余额
        for username, link_ip in accounts:
            new_balance = get_single_account_balance(link_ip)
            if new_balance is not None:
                cursor.execute(
                    "UPDATE ps38accounts SET balance = %s WHERE username = %s;",
                    (new_balance, username)
                )
                print(f"✅ 账号[{username}]余额更新成功：{new_balance}")
            else:
                print(f"⚠️  账号[{username}]余额获取失败，跳过更新")

        cursor.close()
        conn.close()
        print("✅ 所有账号余额更新流程执行完成")
    except Exception as e:
        print(f"❌ 批量更新余额失败：{str(e)}")
        conn.close()


def update_single_account_balance(username):
    """
    仅更新指定账号的余额（投注成功后调用，替代批量更新）
    :param username: 参与投注的账号名
    :return: 无（仅打印日志，异常不影响主流程）
    """
    conn = get_db_connection()
    if not conn:
        print(f"❌ 数据库连接失败，跳过账号[{username}]余额更新")
        return

    try:
        cursor = conn.cursor()
        # 1. 查询该账号的link_ip
        cursor.execute("SELECT link_ip FROM ps38accounts WHERE username = %s;", (username,))
        result = cursor.fetchone()
        if not result:
            print(f"⚠️  账号[{username}]不存在，跳过余额更新")
            cursor.close()
            conn.close()
            return
        link_ip = result[0]

        # 2. 调用原有函数获取最新余额
        new_balance = get_single_account_balance(link_ip)
        if new_balance is not None:
            # 3. 仅更新该账号的余额
            cursor.execute(
                "UPDATE ps38accounts SET balance = %s WHERE username = %s;",
                (new_balance, username)
            )
            print(f"✅ 账号[{username}]余额更新成功：{new_balance}")
        else:
            print(f"⚠️  账号[{username}]余额获取失败，跳过更新")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 更新账号[{username}]余额失败：{str(e)}")
        conn.close()

# ------------------------ 新增：投注转发核心函数 ------------------------
def forward_bet_request(username, bet_type, bet_params):
    """
    转发投注请求到对应账号的目标服务器
    :param username: 账号名（关联ps38accounts表的link_ip）
    :param bet_type: 盘口类型（moneyline/spread/total）
    :param bet_params: 前端传入的投注参数
    :return: 转发结果（dict）
    """
    # ========== 新增：接收到转发请求时打印简洁的原始信息 ==========
    print(f"\n📥 收到投注转发请求 | 账号：{username} | 盘口类型：{bet_type}")
    print(f"   原始请求参数：{json.dumps(bet_params, ensure_ascii=False, indent=2)}")

    # 1. 校验盘口类型合法性
    if bet_type not in BET_TYPE_MAP:
        return {
            "success": False,
            "msg": f"无效盘口类型，仅支持：{list(BET_TYPE_MAP.keys())}",
            "data": None
        }

    # 2. 校验该盘口的必填参数
    required_params = BET_REQUIRED_PARAMS[bet_type]
    missing_params = [p for p in required_params if p not in bet_params]
    if missing_params:
        return {
            "success": False,
            "msg": f"缺少{bet_type}盘口必填参数：{','.join(missing_params)}",
            "data": None
        }

    # 3. 查询账号对应的目标IP（从ps38accounts表查询）
    conn = get_db_connection()
    if not conn:
        return {
            "success": False,
            "msg": "数据库连接失败，无法获取账号绑定的IP",
            "data": None
        }

    try:
        cursor = conn.cursor()
        select_sql = "SELECT link_ip FROM ps38accounts WHERE username = %s;"
        cursor.execute(select_sql, (username,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            return {
                "success": False,
                "msg": f"账号{username}不存在（ps38accounts表）",
                "data": None
            }
        raw_link_ip = result[0]
        # ========== 移除：账号IP信息打印 ==========

    except Exception as e:
        return {
            "success": False,
            "msg": f"查询账号IP失败：{str(e)}",
            "data": None
        }

    # 4. 解析link_ip（适配 http://45.204.212.58:5041 格式）
    try:
        parsed_url = urllib.parse.urlparse(raw_link_ip)
        scheme = parsed_url.scheme if parsed_url.scheme else "http"
        netloc = parsed_url.netloc if parsed_url.netloc else raw_link_ip

        if ":" in netloc:
            target_ip_clean, target_port_clean = netloc.split(":", 1)
            target_port_clean = int(target_port_clean)
        else:
            target_ip_clean = netloc
            target_port_clean = PS3838_DEFAULT_PORT

        target_path = BET_TYPE_MAP[bet_type]
        target_url = f"{scheme}://{target_ip_clean}:{target_port_clean}{target_path}"

        # ========== 移除：解析后的IP/端口等打印 ==========

    except Exception as e:
        error_msg = f"解析link_ip失败（格式应为http://IP:PORT）：{str(e)}，原始link_ip：{raw_link_ip}"
        print(f"❌ {error_msg}")
        bet_record = {
            "username": username,
            "bet_type": bet_type,
            "request_data": json.dumps(bet_params, ensure_ascii=False),
            "response_data": "",
            "target_ip": raw_link_ip,
            "target_port": PS3838_DEFAULT_PORT,
            "target_path": BET_TYPE_MAP[bet_type],
            "status": "failed",
            "error_msg": error_msg
        }
        save_bet_record(bet_record)
        return {
            "success": False,
            "msg": "解析账号绑定的IP格式失败（请检查link_ip是否为http://IP:PORT格式）",
            "data": {
                "raw_link_ip": raw_link_ip,
                "error_detail": str(e)
            }
        }

    # 5. 构造投注记录基础数据
    bet_record = {
        "username": username,
        "bet_type": bet_type,
        "request_data": json.dumps(bet_params, ensure_ascii=False),
        "response_data": "",
        "target_ip": target_ip_clean,
        "target_port": target_port_clean,
        "target_path": target_path,
        "status": "pending",
        "error_msg": ""
    }

    # 6. 转发请求到目标服务器
    try:
        forward_params = bet_params.copy()
        forward_params["accept_better_line"] = bet_params.get("accept_better_line", False)
        response = requests.post(
            url=target_url,
            json=forward_params,
            timeout=10,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )
        response_data = response.json()
        bet_record["response_data"] = json.dumps(response_data, ensure_ascii=False)

        # ========== 修改：精简目标服务器返回信息打印（仅保留关键） ==========
        print("\n📡 目标服务器响应 | 状态码：{}".format(response.status_code))
        print(f"   响应内容：{json.dumps(response_data, ensure_ascii=False, indent=2)}")

        if response.status_code == 200 and response_data.get("code") == 200:
            bet_record["status"] = "success"
            save_result = save_bet_record(bet_record)
            try:
                update_single_account_balance(username)  # 新增：仅更新当前投注账号
            except Exception as e:
                print(f"⚠️  投注转发成功但余额更新失败（不影响主流程）：{str(e)}")
            return {
                "success": True,
                "msg": "投注请求转发成功",
                "data": {
                    "target_ip": target_ip_clean,
                    "target_port": target_port_clean,
                    "target_url": target_url,
                    "bet_record_saved": save_result,
                    "target_response": response_data
                }
            }
        else:
            bet_record["status"] = "failed"
            bet_record[
                "error_msg"] = f"目标服务器返回错误：{response_data.get('msg', '未知错误')}（状态码：{response.status_code}）"
            save_result = save_bet_record(bet_record)
            return {
                "success": False,
                "msg": f"投注请求转发失败（目标服务器返回错误）：{response_data.get('msg', '未知错误')}",
                "data": {
                    "target_ip": target_ip_clean,
                    "target_port": target_port_clean,
                    "target_url": target_url,
                    "bet_record_saved": save_result,
                    "target_response": response_data,
                    "status_code": response.status_code
                }
            }

    except requests.exceptions.Timeout:
        bet_record["status"] = "failed"
        bet_record["error_msg"] = f"目标服务器连接超时（URL：{target_url}，超时时间：10秒）"
        save_bet_record(bet_record)
        return {
            "success": False,
            "msg": "目标服务器连接超时",
            "data": {
                "target_ip": target_ip_clean,
                "target_port": target_port_clean,
                "target_url": target_url,
                "error_detail": "超时（10秒），请检查目标服务器是否监听该端口，或网络是否通畅"
            }
        }
    except requests.exceptions.ConnectionError as e:
        error_detail = str(e)
        bet_record["status"] = "failed"
        bet_record["error_msg"] = f"目标服务器连接失败（URL：{target_url}）：{error_detail}"
        save_bet_record(bet_record)
        return {
            "success": False,
            "msg": "目标服务器连接失败（IP/端口错误或服务未启动）",
            "data": {
                "target_ip": target_ip_clean,
                "target_port": target_port_clean,
                "target_url": target_url,
                "error_detail": error_detail
            }
        }
    except requests.exceptions.JSONDecodeError:
        raw_response = response.text if 'response' in locals() else '无响应'
        # ========== 修改：精简非JSON响应打印 ==========
        print("\n📡 目标服务器响应（非JSON格式）| 状态码：{}".format(
            response.status_code if 'response' in locals() else '无'))
        print(f"   原始响应：{raw_response}")

        bet_record["status"] = "failed"
        bet_record["error_msg"] = f"目标服务器返回非JSON格式数据：{raw_response}"
        save_bet_record(bet_record)
        return {
            "success": False,
            "msg": "目标服务器返回无效数据（非JSON格式）",
            "data": {
                "target_ip": target_ip_clean,
                "target_port": target_port_clean,
                "target_url": target_url,
                "raw_response": raw_response
            }
        }
    except Exception as e:
        error_msg = f"转发请求异常：{str(e)}\n{traceback.format_exc()}"
        bet_record["status"] = "failed"
        bet_record["error_msg"] = error_msg
        save_bet_record(bet_record)
        return {
            "success": False,
            "msg": f"转发请求异常：{str(e)}",
            "data": {
                "target_ip": target_ip_clean,
                "target_port": target_port_clean,
                "target_url": target_url,
                "error_stack": traceback.format_exc()
            }
        }


# ======================== 新增：Get Straight Line 核心函数 ========================
def get_straight_line_api(
        league_id,
        event_id,
        bet_type,
        handicap,
        team=None,
        side=None,
        sport_id=29,  # 固定足球
        odds_format="Malay",  # 固定马来赔率
        period_number=0  # 固定全场
):
    """
    调用PS3838 Get Straight Line v2 API (独立实现，不影响原有功能)

    参数说明:
    - 必填: league_id, event_id, bet_type, handicap
    - 条件参数:
        * bet_type="SPREAD" 时需提供 team (Team1/Team2/Draw)
        * bet_type="TOTAL_POINTS" 时需提供 side (OVER/UNDER)
    - 固定参数: sport_id=29, odds_format="Malay", period_number=0

    返回: (status_code, response_data)
    """
    params = {
        "sportId": sport_id,
        "leagueId": league_id,
        "eventId": event_id,
        "periodNumber": period_number,
        "betType": bet_type,
        "handicap": handicap,
        "oddsFormat": odds_format
    }

    # 根据投注类型添加条件参数
    if bet_type in ["SPREAD", "TEAM_TOTAL_POINTS"] and team:
        params["team"] = team
    if bet_type in ["TOTAL_POINTS", "TEAM_TOTAL_POINTS"] and side:
        params["side"] = side

    try:
        response = requests.get(
            url=PS3838_API_BASE_URL + PS3838_API_ENDPOINT,
            params=params,
            auth=(PS3838_USERNAME, PS3838_PASSWORD),
            headers=PS3838_HEADERS,
            timeout=10
        )
        return response.status_code, response.json()
    except Exception as e:
        return 500, {"error": str(e)}

# ======================================================================================

# ------------------------ 最终版：前端传啥账号就查啥账号的PS3838注单 ------------------------
def get_ps3838_bets_24h(username):
    """
    核心逻辑：
    1. 从ps38accounts表读取前端传入账号对应的PS3838账号（username字段）和密码（password字段）
    2. 用该账号密码调用PS3838 /v3/bets接口，查询对应注单
    3. 日期范围29天（符合PS3838限制），兼容所有Python版本
    """
    # 1. 从数据库获取该账号对应的PS3838认证信息（username=PS3838账号，password=PS3838密码）
    conn = get_db_connection()
    if not conn:
        return {"success": False, "msg": "数据库连接失败", "data": None}

    ps3838_auth_username = None
    ps3838_auth_password = None
    try:
        cursor = conn.cursor()
        # 查询该账号的PS3838认证信息（ps38accounts表的username=PS3838账号，password=PS3838密码）
        cursor.execute("""
            SELECT username, password 
            FROM ps38accounts 
            WHERE username = %s
        """, (username,))
        result = cursor.fetchone()

        if not result:
            cursor.close()
            conn.close()
            return {"success": False, "msg": f"数据库中无账号【{username}】的信息", "data": None}

        # 提取PS3838认证账号和密码
        ps3838_auth_username, ps3838_auth_password = result
        cursor.close()
        conn.close()
    except Exception as e:
        conn.close()
        return {"success": False, "msg": f"查询数据库失败：{str(e)}", "data": None}

    # 2. 构造UTC时间（兼容Python 3.10及以下版本，日期范围29天）
    to_date = datetime.now(timezone.utc)
    from_date = to_date - timedelta(days=1)
    from_date_str = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date_str = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 3. 构造PS3838接口请求参数
    params = {
        "betlist": "ALL",
        "fromDate": from_date_str,
        "toDate": to_date_str,
        "sortDir": "DESC",
        "pageSize": 1000
    }

    # 打印调试信息（明确当前查询的账号）
    print(f"\n🔍 查询PS3838 1天注单 | 目标账号：{username} | PS3838认证账号：{ps3838_auth_username}")
    print(f"   时间范围：{from_date_str} 至 {to_date_str}")
    print(f"   请求参数：{json.dumps(params, indent=2)}")

    # 4. 用该账号的PS3838认证信息调用接口
    try:
        response = requests.get(
            url=f"{PS3838_API_BASE_URL}/v3/bets",
            params=params,
            auth=HTTPBasicAuth(ps3838_auth_username, ps3838_auth_password),  # 动态认证！
            headers={"Accept": "application/json"},
            timeout=20,
            verify=False
        )

        print(f"📡 PS3838 /v3/bets 响应状态码：{response.status_code}")
        print(f"   原始响应内容：{response.text}")

        # 处理400错误（参数/日期范围错误）
        if response.status_code == 400:
            try:
                error_data = response.json()
                error_msg = error_data.get("message", "无效请求参数")
            except:
                error_msg = response.text
            return {
                "success": False,
                "msg": f"PS3838接口返回错误：{error_msg}",
                "data": {
                    "status_code": 400,
                    "error_detail": error_msg,
                    "request_params": params
                }
            }

        # 处理非200错误
        if response.status_code != 200:
            return {
                "success": False,
                "msg": f"PS3838接口请求失败，状态码：{response.status_code}",
                "data": {
                    "status_code": response.status_code,
                    "raw_response": response.text
                }
            }

        # 解析200响应
        api_data = response.json()
        all_bets = []

        # 解析所有类型注单
        bet_categories = ["straightBets", "parlayBets", "teaserBets", "specialBets", "manualBets"]
        for category in bet_categories:
            bets = api_data.get(category, [])
            if isinstance(bets, list) and len(bets) > 0:
                for bet in bets:
                    bet["betCategory"] = category[:-4]
                    all_bets.append(bet)
                print(f"✅ 解析到{category}类型注单 {len(bets)} 条")

        # 构造成功返回
        return {
            "success": True,
            "msg": f"查询成功！账号【{username}】（PS3838账号：{ps3838_auth_username}）近1天有 {len(all_bets)} 条注单",
            "data": {
                "fromRecord": api_data.get("fromRecord", 0),
                "moreAvailable": api_data.get("moreAvailable", False),
                "pageSize": api_data.get("pageSize", 1000),
                "toRecord": api_data.get("toRecord", -1),
                "bets": all_bets,
                "totalBets": len(all_bets),
                "ps3838_auth_username": ps3838_auth_username,
                "time_range": {
                    "from": from_date_str,
                    "to": to_date_str,
                    "days": 29
                }
            }
        }

    except requests.exceptions.Timeout:
        return {"success": False, "msg": "PS3838 API请求超时（20秒）", "data": None}
    except requests.exceptions.ConnectionError:
        return {"success": False, "msg": "无法连接到PS3838 API服务器（检查网络/域名）", "data": None}
    except Exception as e:
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        print(f"❌ PS3838接口调用异常：{error_detail}")
        return {"success": False, "msg": f"接口调用失败：{str(e)}", "data": {"error_detail": error_detail}}

# ------------------------ 新增：对外接口路由 ------------------------
@app.route('/api/bet/ps3838/24h', methods=['GET'])
def query_ps3838_bets_24h():
    """对外接口：查询指定账号24小时内所有注单"""
    username = request.args.get("username")
    if not username:
        return jsonify({"code": 400, "msg": "缺少必填参数username", "data": None})

    result = get_ps3838_bets_24h(username)
    return jsonify({
        "code": 200 if result["success"] else 500,
        "msg": result["msg"],
        "data": result["data"]
    })




# ------------------------ 原有API接口（完全保留，适配ps38accounts表） ------------------------
@app.route('/api/account', methods=['POST'])
def add_account():
    try:
        data = request.get_json()
        required_fields = ["username", "password", "link_ip", "balance", "rate", "single_max", "total_max",
                           "group_name"]
        for field in required_fields:
            if field not in data:
                return jsonify({"code": 400, "msg": f"缺少必填参数：{field}", "data": None})

        numeric_fields = ["balance", "rate", "single_max", "total_max"]
        for field in numeric_fields:
            try:
                data[field] = float(data[field])
            except ValueError:
                return jsonify({"code": 400, "msg": f"{field}必须为数字类型", "data": None})

        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()
        # 插入ps38accounts表
        insert_sql = """
        INSERT INTO ps38accounts (username, password, link_ip, balance, rate, single_max, total_max, group_name, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (
            data["username"], data["password"], data["link_ip"], data["balance"],
            data["rate"], data["single_max"], data["total_max"], data["group_name"],
            data.get("remark", "")
        ))
        cursor.close()
        conn.close()

        return jsonify({"code": 200, "msg": "账号添加成功", "data": {"username": data["username"]}})

    except psycopg2.errors.UniqueViolation:
        return jsonify({"code": 400, "msg": "用户名已存在", "data": None})
    except Exception as e:
        return jsonify({"code": 500, "msg": f"添加失败：{str(e)}", "data": None})


@app.route('/api/account/<username>', methods=['DELETE'])
def delete_account(username):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()
        # 删除ps38accounts表数据
        delete_sql = "DELETE FROM ps38accounts WHERE username = %s;"
        cursor.execute(delete_sql, (username,))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({"code": 400, "msg": "用户名不存在", "data": None})

        cursor.close()
        conn.close()
        return jsonify({"code": 200, "msg": "账号删除成功", "data": {"username": username}})

    except Exception as e:
        return jsonify({"code": 500, "msg": f"删除失败：{str(e)}", "data": None})


@app.route('/api/account/<username>', methods=['GET'])
def query_account(username):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()
        # 查询ps38accounts表
        select_sql = """
        SELECT username, password, link_ip, balance, rate, single_max, total_max, group_name, remark, create_time
        FROM ps38accounts WHERE username = %s;
        """
        cursor.execute(select_sql, (username,))
        result = cursor.fetchone()

        if not result:
            cursor.close()
            conn.close()
            return jsonify({"code": 400, "msg": "用户名不存在", "data": None})

        account_data = {
            "username": result[0],
            "password": result[1],
            "link_ip": result[2],
            "balance": float(result[3]),
            "rate": float(result[4]),
            "single_max": float(result[5]),
            "total_max": float(result[6]),
            "group_name": result[7],
            "remark": result[8] if result[8] else "",
            "create_time": result[9].strftime("%Y-%m-%d %H:%M:%S")
        }

        cursor.close()
        conn.close()
        return jsonify({"code": 200, "msg": "查询成功", "data": account_data})

    except Exception as e:
        return jsonify({"code": 500, "msg": f"查询失败：{str(e)}", "data": None})


@app.route('/api/account/<username>', methods=['PUT'])
def modify_account(username):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()
        # 校验ps38accounts表中是否存在该用户
        check_sql = "SELECT 1 FROM ps38accounts WHERE username = %s;"
        cursor.execute(check_sql, (username,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({"code": 400, "msg": "用户名不存在", "data": None})

        data = request.get_json()
        if not data:
            return jsonify({"code": 400, "msg": "无修改字段", "data": None})

        field_config = {
            "password": str,
            "link_ip": str,
            "balance": float,
            "rate": float,
            "single_max": float,
            "total_max": float,
            "group_name": str,
            "remark": str
        }
        valid_fields = []
        update_sql_parts = []
        update_params = []
        for field, value in data.items():
            if field not in field_config:
                continue
            try:
                converted_val = field_config[field](value)
                update_sql_parts.append(f"{field} = %s")
                update_params.append(converted_val)
                valid_fields.append(field)
            except ValueError:
                return jsonify({"code": 400, "msg": f"{field}必须为{field_config[field].__name__}类型", "data": None})

        if not valid_fields:
            return jsonify({"code": 400, "msg": "无有效修改字段", "data": None})

        # 更新ps38accounts表
        update_sql = f"UPDATE ps38accounts SET {', '.join(update_sql_parts)} WHERE username = %s;"
        update_params.append(username)
        cursor.execute(update_sql, tuple(update_params))

        cursor.close()
        conn.close()
        return jsonify({
            "code": 200,
            "msg": "账号修改成功",
            "data": {"username": username, "updated_fields": valid_fields}
        })

    except Exception as e:
        return jsonify({"code": 500, "msg": f"修改失败：{str(e)}", "data": None})


@app.route('/api/accounts', methods=['GET'])
def query_all_accounts():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()
        # 查询ps38accounts表所有数据
        select_sql = """
        SELECT username, password, link_ip, balance, rate, single_max, total_max, group_name, remark, create_time
        FROM ps38accounts ORDER BY create_time DESC;
        """
        cursor.execute(select_sql)
        results = cursor.fetchall()

        account_list = []
        for result in results:
            account_data = {
                "username": result[0],
                "password": result[1],
                "link_ip": result[2],
                "balance": float(result[3]),
                "rate": float(result[4]),
                "single_max": float(result[5]),
                "total_max": float(result[6]),
                "group_name": result[7],
                "remark": result[8] if result[8] else "",
                "create_time": result[9].strftime("%Y-%m-%d %H:%M:%S")
            }
            account_list.append(account_data)

        cursor.close()
        conn.close()

        return jsonify({
            "code": 200,
            "msg": "查询成功",
            "data": {
                "total": len(account_list),
                "accounts": account_list
            }
        })

    except Exception as e:
        return jsonify({"code": 500, "msg": f"查询所有账号失败：{str(e)}", "data": None})


# ------------------------ 新增：投注转发API接口 ------------------------
@app.route('/api/bet/forward', methods=['POST'])
def bet_forward():
    try:
        # 获取前端请求参数
        req_data = request.get_json()
        if not req_data:
            return jsonify({
                "code": 400,
                "msg": "请求参数不能为空",
                "data": None
            })

        # 校验核心必填参数
        core_required = ["username", "bet_type"]
        missing_core = [p for p in core_required if p not in req_data]
        if missing_core:
            return jsonify({
                "code": 400,
                "msg": f"缺少核心必填参数：{','.join(missing_core)}",
                "data": None
            })

        # 执行转发逻辑
        forward_result = forward_bet_request(
            username=req_data["username"],
            bet_type=req_data["bet_type"],
            bet_params=req_data
        )

        # 构造统一响应格式
        if forward_result["success"]:
            return jsonify({
                "code": 200,
                "msg": forward_result["msg"],
                "data": forward_result["data"]
            })
        else:
            return jsonify({
                "code": 500,
                "msg": forward_result["msg"],
                "data": forward_result.get("data")
            })

    except Exception as e:
        return jsonify({
            "code": 500,
            "msg": f"投注转发接口异常：{str(e)}",
            "data": None
        })


# ------------------------ 新增：投注记录查询接口（适配ps38bet_records表） ------------------------
@app.route('/api/bet/records', methods=['GET'])
def get_bet_records():
    """查询投注记录（含新增字段，修复total未定义问题）"""
    try:
        # 原有参数获取逻辑（保留）
        username = request.args.get("username")
        bet_type = request.args.get("bet_type")
        status = request.args.get("status")
        page = int(request.args.get("page", 1))
        size = int(request.args.get("size", 20))
        offset = (page - 1) * size

        # 原有条件构造逻辑（保留）
        where_conditions = []
        query_params = []
        if username:
            where_conditions.append("username = %s")
            query_params.append(username)
        if bet_type:
            where_conditions.append("bet_type = %s")
            query_params.append(bet_type)
        if status:
            where_conditions.append("status = %s")
            query_params.append(status)

        where_sql = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # 执行查询
        conn = get_db_connection()
        if not conn:
            return jsonify({"code": 500, "msg": "数据库连接失败", "data": None})

        cursor = conn.cursor()

        # ========== 修复：补全总数查询逻辑 ==========
        # 查询总数（ps38bet_records表）
        count_sql = f"SELECT COUNT(*) FROM ps38bet_records {where_sql};"
        cursor.execute(count_sql, tuple(query_params))
        total = cursor.fetchone()[0]  # 赋值total变量，解决未定义问题

        # ========== 新增字段：修改SELECT语句 ==========
        select_sql = f"""
        SELECT id, username, bet_type, request_data, response_data,
               target_ip, target_port, target_path, bet_time, status, error_msg,
               league_name, home_team, away_team, handicap_value, bet_direction, bet_category
        FROM ps38bet_records {where_sql}
        ORDER BY bet_time DESC
        LIMIT %s OFFSET %s;
        """
        query_params.extend([size, offset])
        cursor.execute(select_sql, tuple(query_params))
        results = cursor.fetchall()

        # 构造返回数据（新增字段）
        record_list = []
        for row in results:
            record_list.append({
                "id": row[0],
                "username": row[1],
                "bet_type": row[2],
                "request_data": json.loads(row[3]) if row[3] else {},
                "response_data": json.loads(row[4]) if row[4] else {},
                "target_ip": row[5],
                "target_port": row[6],
                "target_path": row[7],
                "bet_time": row[8].strftime("%Y-%m-%d %H:%M:%S"),
                "status": row[9],
                "error_msg": row[10] or "",
                # ========== 新增字段：返回 ==========
                "league_name": row[11],
                "home_team": row[12],
                "away_team": row[13],
                "handicap_value": float(row[14]),
                "bet_direction": row[15],
                "bet_category": row[16]
            })

        cursor.close()
        conn.close()

        return jsonify({
            "code": 200,
            "msg": "查询成功",
            "data": {
                "total": total,  # 现在total已定义，无报错
                "page": page,
                "size": size,
                "records": record_list
            }
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "msg": f"查询投注记录失败：{str(e)}",
            "data": None
        })


@app.route('/api/get_line', methods=['GET'])
def get_straight_line_route():
    """
    获取盘口线路信息 (独立路由，不影响原有功能)

    查询参数:
    - league_id (必填): 联赛ID
    - event_id (必填): 赛事ID
    - bet_type (必填): 投注类型 (SPREAD/TOTAL_POINTS)
    - handicap (必填): 盘口值 (数字)
    - team (条件): SPREAD类型时必填 (Team1/Team2/Draw)
    - side (条件): TOTAL_POINTS类型时必填 (OVER/UNDER)

    固定参数:
    - sport_id=29 (足球)
    - odds_format=Malay
    - period_number=0 (全场)
    """
    try:
        # 获取并校验必填参数
        league_id = request.args.get('league_id')
        event_id = request.args.get('event_id')
        bet_type = request.args.get('bet_type')
        handicap = request.args.get('handicap')

        if not all([league_id, event_id, bet_type, handicap]):
            return jsonify({
                "code": 400,
                "msg": "缺少必填参数: league_id, event_id, bet_type, handicap",
                "data": None
            })

        # 验证handicap为数字
        try:
            float(handicap)
        except ValueError:
            return jsonify({
                "code": 400,
                "msg": "handicap必须为数字",
                "data": None
            })

        # 条件参数校验
        team = request.args.get('team')
        side = request.args.get('side')

        if bet_type == "SPREAD" and not team:
            return jsonify({
                "code": 400,
                "msg": "SPREAD类型必须提供team参数 (Team1/Team2/Draw)",
                "data": None
            })

        if bet_type == "TOTAL_POINTS" and not side:
            return jsonify({
                "code": 400,
                "msg": "TOTAL_POINTS类型必须提供side参数 (OVER/UNDER)",
                "data": None
            })

        # 调用核心API函数
        status_code, api_response = get_straight_line_api(
            league_id=league_id,
            event_id=event_id,
            bet_type=bet_type,
            handicap=handicap,
            team=team,
            side=side
        )

        # 构造统一响应
        if status_code == 200 and api_response.get("status") == "SUCCESS":
            return jsonify({
                "code": 200,
                "msg": "获取盘口成功",
                "data": {
                    # 保留原有字段（不修改）
                    "price": api_response.get("price"),
                    "line_id": api_response.get("lineId"),
                    "max_risk_stake": api_response.get("maxRiskStake"),
                    "min_risk_stake": api_response.get("minRiskStake"),
                    # 新增剩余的所有原始数据字段（仅添加，不修改其他）
                    "alt_line_id": api_response.get("altLineId"),
                    "effective_as_of": api_response.get("effectiveAsOf"),
                    "max_win_stake": api_response.get("maxWinStake"),
                    "min_win_stake": api_response.get("minWinStake"),
                    "period_team1_red_cards": api_response.get("periodTeam1RedCards"),
                    "period_team1_score": api_response.get("periodTeam1Score"),
                    "period_team2_red_cards": api_response.get("periodTeam2RedCards"),
                    "period_team2_score": api_response.get("periodTeam2Score"),
                    "team1_red_cards": api_response.get("team1RedCards"),
                    "team1_score": api_response.get("team1Score"),
                    "team2_red_cards": api_response.get("team2RedCards"),
                    "team2_score": api_response.get("team2Score"),
                    "status": api_response.get("status"),
                    # 保留原始响应供调试（不修改）
                    "raw_response": api_response
                }
            })
        else:
            return jsonify({
                "code": status_code if status_code != 200 else 500,
                "msg": f"获取盘口失败: {api_response.get('message', '未知错误')}",
                "data": {
                    "raw_response": api_response,
                    "request_params": {
                        "league_id": league_id,
                        "event_id": event_id,
                        "bet_type": bet_type,
                        "handicap": handicap,
                        "team": team,
                        "side": side
                    }
                }
            })

    except Exception as e:
        return jsonify({
            "code": 500,
            "msg": f"内部服务器错误: {str(e)}",
            "data": None
        })


# ------------------------ 启动配置（增量修改，添加ps38bet_records表初始化） ------------------------
if __name__ == "__main__":
    # 启动时初始化数据库表（先初始化ps38accounts，再初始化ps38bet_records）
    account_table_ok = init_account_table()
    bet_table_ok = init_bet_records_table()

    if account_table_ok and bet_table_ok:
        print("✅ 数据库表初始化成功（ps38accounts + ps38bet_records），API服务启动中...")
        app.run(host="0.0.0.0", port=5034, debug=False)
    else:
        print("❌ 数据库表初始化失败（ps38accounts或ps38bet_records），服务启动失败！")