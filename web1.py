import requests
import base64
import json
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
from flask import Flask, jsonify, request
from concurrent.futures import ThreadPoolExecutor  # 新增多线程库

app = Flask(__name__)

# 配置信息
USERNAME = "H620803004"
PASSWORD = "dddd1111"
SPORT_ID = 29  # 足球运动 ID
FIXTURES_API_URL = "https://api.ps3838.com/v3/fixtures"  # 修复URL末尾空格
ODDS_API_URL = "https://api.ps3838.com/v3/odds"  # 修复URL末尾空格
ODDS_FORMAT = "Malay"  # 赔率格式（支持 American/Decimal/HongKong/Indonesian/Malay）
JAVA_CHECK_API = "http://160.25.20.18:8080/api/bindings/check-existing"  # Java API地址

# 生成 Basic 认证头
auth_credentials = f"{USERNAME}:{PASSWORD}"
auth_header = base64.b64encode(auth_credentials.encode()).decode()


# ------------------------------ 获取赛事列表 ------------------------------
def get_fixtures():
    params = {
        "sportId": SPORT_ID,
    }
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(FIXTURES_API_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"获取赛事列表失败 - HTTP 错误 {response.status_code}: {response.json().get('error', '未知错误')}")
    except requests.exceptions.RequestException as e:
        print(f"获取赛事列表请求失败: {e}")
    return None


# ------------------------------ 获取赛事赔率（保持单例接口，供多线程调用）------------------------------
def get_odds(event_ids):
    params = {
        "sportId": SPORT_ID,
        "oddsFormat": ODDS_FORMAT,
        "eventIds": event_ids  # 接受逗号分隔字符串
    }
    headers = {"Authorization": f"Basic {auth_header}"}  # 移除错误的Content-Type头
    try:
        response = requests.get(ODDS_API_URL, params=params, headers=headers, timeout=(5, 10))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取赔率失败: {e}")
        return None


# 格式化时间差
def format_time_delta(delta):
    total_seconds = int(delta.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        return f"{days}天{hours}小时{minutes}分"
    elif hours > 0:
        return f"{hours}小时{minutes}分"
    else:
        return f"{minutes}分{seconds}秒"


# ------------------------------ 检查比赛是否存在于数据库 ------------------------------
def check_matches_in_db(matches):
    """
    调用Java API检查比赛是否存在于数据库
    :param matches: 比赛列表，每个元素包含league, home_team, away_team
    :return: 存在于数据库的比赛列表
    """
    payload = {
        "source": 1,  # 检查source1
        "matches": matches
    }

    try:
        response = requests.post(JAVA_CHECK_API, json=payload)
        response.raise_for_status()
        result = response.json()

        # 返回存在的比赛
        existing_matches = []
        for match in matches:
            league = match["league"]
            home_team = match["homeTeam"]
            away_team = match["awayTeam"]

            # 如果联赛、主队、客队都存在，则保留此比赛
            if league in result["leagues"] and home_team in result["teams"] and away_team in result["teams"]:
                existing_matches.append(match)

        return existing_matches

    except Exception as e:
        print(f"检查比赛存在性失败: {e}")
        return []


def reformat_odds_data(original_odds):
    """
    将赔率数据从原格式转换为指定的新格式，添加altLineId支持
    :param original_odds: 原始赔率数据
    :return: 重新格式化后的赔率数据
    """
    reformatted_odds = {
        "spreads": {},
        "totals": {}
    }

    # 新增：统一处理盘口数值的格式化函数（适用于让球盘和大小球）
    def format_handicap(hc):
        if hc is None:
            return None
        # 处理整数盘口去尾零（如1.0→1，-2.0→-2；保留0.0→0的原有逻辑）
        if isinstance(hc, float) and hc.is_integer():
            hc = int(hc)
        return str(hc)

    # 处理让球盘数据（添加altLineId）
    if "spreads" in original_odds:
        for spread in original_odds["spreads"]:
            home_handicap = spread.get("home_handicap")
            away_handicap = spread.get("away_handicap")
            home_odds = spread.get("home_odds")
            away_odds = spread.get("away_odds")
            alt_line_id = spread.get("altLineId")  # 提取altLineId

            if home_handicap is not None:
                home_key = format_handicap(home_handicap)
                if home_key not in reformatted_odds["spreads"]:
                    reformatted_odds["spreads"][home_key] = {}
                if home_odds is not None:
                    reformatted_odds["spreads"][home_key]["home"] = str(home_odds)
                # 添加altLineId到该盘口
                if alt_line_id is not None:
                    reformatted_odds["spreads"][home_key]["altLineId"] = alt_line_id

            if away_handicap is not None:
                away_key = format_handicap(away_handicap)
                if away_key not in reformatted_odds["spreads"]:
                    reformatted_odds["spreads"][away_key] = {}
                if away_odds is not None:
                    reformatted_odds["spreads"][away_key]["away"] = str(away_odds)
                # 添加altLineId到该盘口
                if alt_line_id is not None:
                    reformatted_odds["spreads"][away_key]["altLineId"] = alt_line_id

    # 处理大小球数据（添加altLineId）
    if "totals" in original_odds:
        for total in original_odds["totals"]:
            points = total.get("points")
            over_odds = total.get("over_odds")
            under_odds = total.get("under_odds")
            alt_line_id = total.get("altLineId")  # 提取altLineId

            if points is not None:
                # 对大小球点数应用相同的格式化逻辑
                points_formatted = format_handicap(points)  # 复用统一函数
                reformatted_odds["totals"][points_formatted] = {}
                if over_odds is not None:
                    reformatted_odds["totals"][points_formatted]["over"] = str(over_odds)
                if under_odds is not None:
                    reformatted_odds["totals"][points_formatted]["under"] = str(under_odds)
                # 添加altLineId到该盘口
                if alt_line_id is not None:
                    reformatted_odds["totals"][points_formatted]["altLineId"] = alt_line_id

    return reformatted_odds


# ------------------------------ 主逻辑 ------------------------------
@app.route('/get_odds1', methods=['GET'])
def get_sports_data():
    # 1. 获取并筛选赛事
    fixtures_data = get_fixtures()
    if not fixtures_data:
        return jsonify({"message": "未获取到赛事列表"}), 500

    beijing_timezone = timezone(timedelta(hours=8))
    current_time_beijing = datetime.now(beijing_timezone)
    valid_fixtures = []
    event_ids = []  # 收集符合条件的赛事 ID

    # 准备发送给Java API的比赛列表
    matches_to_check = []

    for league in fixtures_data.get("league", []):
        for event in league.get("events", []):
            starts_str = event.get("starts")
            if not starts_str:
                continue
            starts_utc = parse(starts_str).astimezone(timezone.utc)
            starts_beijing = starts_utc.astimezone(beijing_timezone)  # 转换为北京时间
            status = event.get("status", "")
            live_status = event.get("liveStatus", 0)
            home = event.get("home", "")
            away = event.get("away", "")
            league_name = league.get("name", "")

            # 移除原有时间过滤条件，仅保留非数据相关过滤
            if ("Corners" not in league_name and "Bookings" not in league_name and
                    "Corners" not in home and "Bookings" not in home and
                    "Corners" not in away and "Bookings" not in away and
                    status == 'O' and live_status in {2}):
                valid_fixtures.append({
                    "event_id": event.get("id"),
                    "start_time_beijing": starts_beijing.strftime("%Y-%m-%d %H:%M:%S"),
                    "home_team": home,
                    "away_team": away,
                    "league_name": league_name
                })

                matches_to_check.append({
                    "league": league_name,
                    "homeTeam": home,
                    "awayTeam": away
                })

    if not valid_fixtures:
        return jsonify({"message": "未找到符合条件的赛事"}), 404

    # 2. 检查比赛是否存在于数据库
    existing_matches = check_matches_in_db(matches_to_check)

    if not existing_matches:
        return jsonify({"message": "未找到在数据库中存在的赛事"}), 404

    # 3. 筛选出存在于数据库的赛事
    valid_fixtures_in_db = []
    for fixture in valid_fixtures:
        for match in existing_matches:
            if (fixture["home_team"] == match["homeTeam"] and
                    fixture["away_team"] == match["awayTeam"] and
                    fixture["league_name"] == match["league"]):
                valid_fixtures_in_db.append(fixture)
                event_ids.append(fixture["event_id"])
                break

    if not valid_fixtures_in_db:
        return jsonify({"message": "未找到在数据库中存在的赛事"}), 404

    # 4. 根据赛事 ID 获取赔率
    if event_ids:
        # 1. 计算批次
        batch_size = (len(event_ids) // 5) + 1
        batches = [event_ids[i:i + batch_size] for i in range(0, len(event_ids), batch_size)]
        valid_batches = [batch for batch in batches if batch]  # 过滤空批次
        if not valid_batches:
            return jsonify({"message": "无有效赛事ID可请求赔率"}), 404

        # 2. 并行请求
        with ThreadPoolExecutor(max_workers=len(valid_batches)) as executor:
            futures = [
                executor.submit(get_odds, ",".join(map(str, batch)))
                for batch in valid_batches
            ]
        results = [future.result() for future in futures]

        # 3. 合并结果
        odds_data = {"leagues": []}
        for result in results:
            if result and "leagues" in result:
                odds_data["leagues"].extend(result["leagues"])
    else:
        odds_data = None

    # 5. 解析赔率并过滤24小时内开赛的比赛
    valid_fixtures_with_odds = []
    for fixture in valid_fixtures_in_db:
        event_id = fixture["event_id"]
        odds_info = {}
        line_id = ""
        league_id = None  # 新增：存储联赛ID
        period_number = None  # 新增：存储盘口期号

        if odds_data:
            for league in odds_data.get("leagues", []):
                current_league_id = league.get("id")  # 从联赛对象获取ID
                for event in league.get("events", []):
                    if event.get("id") == event_id:
                        for period in event.get("periods", []):
                            # 关键修改: 检查 number=0 且获取 lineId、leagueId 和 periodNumber
                            if period.get("number") == 0:
                                # 获取关键字段
                                line_id = str(period.get("lineId", ""))
                                league_id = current_league_id  # 保存联赛ID
                                period_number = period.get("number", 0)  # 保存盘口期号

                                # 解析让球赔率 (新增altLineId提取)
                                spreads = period.get("spreads", [])
                                if spreads:
                                    odds_info["spreads"] = []
                                    for spread in spreads:
                                        handicap = spread.get("hdp")
                                        home_odds = spread.get("home")
                                        away_odds = spread.get("away")
                                        alt_line_id = spread.get("altLineId")  # 提取altLineId

                                        # 让球盘口尾零处理
                                        if isinstance(handicap, float) and handicap.is_integer():
                                            home_handicap = int(handicap)
                                        else:
                                            home_handicap = handicap
                                        away_handicap = -home_handicap if home_handicap is not None else None

                                        spread_info = {
                                            "home_odds": home_odds,
                                            "away_odds": away_odds,
                                            "home_handicap": home_handicap,
                                            "away_handicap": away_handicap,
                                            "altLineId": alt_line_id  # 添加altLineId
                                        }
                                        odds_info["spreads"].append(spread_info)

                                # 解析总进球赔率 (新增altLineId提取)
                                totals = period.get("totals", [])
                                if totals:
                                    odds_info["totals"] = []
                                    for total in totals:
                                        points = total.get("points")
                                        over_odds = total.get("over")
                                        under_odds = total.get("under")
                                        alt_line_id = total.get("altLineId")  # 提取altLineId

                                        # 大小球盘口尾零处理
                                        if isinstance(points, float) and points.is_integer():
                                            points = int(points)

                                        total_info = {
                                            "points": points,
                                            "over_odds": over_odds,
                                            "under_odds": under_odds,
                                            "altLineId": alt_line_id  # 添加altLineId
                                        }
                                        odds_info["totals"].append(total_info)
                        # 找到对应赛事后跳出内层循环
                        break

        if odds_info:
            start_time_beijing = datetime.strptime(fixture["start_time_beijing"], "%Y-%m-%d %H:%M:%S")
            start_time_beijing = start_time_beijing.replace(tzinfo=beijing_timezone)
            time_until_start = start_time_beijing - current_time_beijing

            # 核心过滤条件：剩余时间在0到24小时内
            if 0 < time_until_start.total_seconds() <= 24 * 3600:
                fixture["time_until_start"] = format_time_delta(time_until_start)
                formatted_odds_info = reformat_odds_data(odds_info)
                valid_fixtures_with_odds.append((
                    fixture,
                    formatted_odds_info,
                    line_id,
                    league_id,          # 新增：传递联赛ID
                    period_number       # 新增：传递盘口期号
                ))

    if not valid_fixtures_with_odds:
        return jsonify({"message": "未找到24小时内有赔率信息的赛事"}), 404

    # 6. 构造最终返回数据 (新增leagueId和periodNumber字段)
    data = []
    for fixture, odds_info, line_id, league_id, period_number in valid_fixtures_with_odds:
        data.append({
            "league_name": fixture["league_name"],
            "home_team": fixture["home_team"],
            "away_team": fixture["away_team"],
            "start_time_beijing": fixture["start_time_beijing"],
            "time_until_start": fixture["time_until_start"],
            "odds": odds_info,
            "event_id": fixture["event_id"],
            "line_id": line_id,
            "league_id": league_id,          # 新增字段
            "period_number": period_number   # 新增字段
        })

    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)