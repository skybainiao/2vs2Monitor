import requests
import base64
import json
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
from flask import Flask, jsonify

app = Flask(__name__)

# 配置信息
USERNAME = "GA1A713S00"
PASSWORD = "dddd1111DD"
SPORT_ID = 29  # 足球运动 ID
FIXTURES_API_URL = "https://api.ps3838.com/v3/fixtures"  # 赛事列表 API
ODDS_API_URL = "https://api.ps3838.com/v3/odds"  # 赔率 API
ODDS_FORMAT = "Decimal"  # 赔率格式（支持 American/Decimal/HongKong/Indonesian/Malay）

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


# ------------------------------ 获取赛事赔率 ------------------------------
def get_odds(event_ids):
    params = {
        "sportId": SPORT_ID,
        "oddsFormat": ODDS_FORMAT,
        "eventIds": event_ids  # 按文档支持的逗号分隔格式传递
    }
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(ODDS_API_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_data = response.json()
        print(
            f"获取赔率失败 - HTTP 错误 {response.status_code}: {error_data.get('error', {}).get('message', '未知错误')}")
    except requests.exceptions.RequestException as e:
        print(f"获取赔率请求失败: {e}")
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


# ------------------------------ 主逻辑 ------------------------------
@app.route('/get_sports_data', methods=['GET'])
def get_sports_data():
    # 1. 获取并筛选赛事
    fixtures_data = get_fixtures()
    if not fixtures_data:
        return jsonify({"message": "未获取到赛事列表"}), 500

    current_time_utc = datetime.now(timezone.utc)
    today_start_utc = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_utc = current_time_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
    beijing_timezone = timezone(timedelta(hours=8))
    current_time_beijing = datetime.now(beijing_timezone)
    valid_fixtures = []
    event_ids = []  # 收集符合条件的赛事 ID

    for league in fixtures_data.get("league", []):
        for event in league.get("events", []):
            starts_str = event.get("starts")
            if not starts_str:
                continue
            starts_utc = parse(starts_str).astimezone(timezone.utc)
            status = event.get("status", "")
            live_status = event.get("liveStatus", 0)
            home = event.get("home", "")
            away = event.get("away", "")
            league_name = league.get("name", "")

            # 筛选条件（沿用用户原有逻辑）
            if ("Corners" not in league_name and "Bookings" not in league_name and
                    "Corners" not in home and "Bookings" not in home and
                    "Corners" not in away and "Bookings" not in away and
                    today_start_utc <= starts_utc <= today_end_utc and
                    starts_utc > current_time_utc and
                    status == 'O' and live_status in {2}):
                valid_fixtures.append({
                    "event_id": event.get("id"),
                    "start_time_beijing": starts_utc.astimezone(beijing_timezone).strftime("%Y-%m-%d %H:%M:%S"),
                    "home_team": home,
                    "away_team": away,
                    "league_name": league_name
                })
                event_ids.append(event.get("id"))  # 收集赛事 ID

    if not valid_fixtures:
        return jsonify({"message": "未找到符合条件的赛事"}), 404

    # 2. 根据赛事 ID 获取赔率（需转换为逗号分隔字符串）
    if event_ids:
        params_event_ids = ",".join(map(str, event_ids))
        odds_data = get_odds(params_event_ids)
    else:
        odds_data = None

    # 解析并打印有赔率信息的比赛结果
    valid_fixtures_with_odds = []
    for fixture in valid_fixtures:
        event_id = fixture["event_id"]
        odds_info = {}

        # 匹配赔率数据（假设赛事 ID 唯一）
        if odds_data:
            for league in odds_data.get("leagues", []):
                for event in league.get("events", []):
                    if event.get("id") == event_id:
                        for period in event.get("periods", []):
                            if period.get("number") == 0:  # 0 代表全场比赛（根据文档）
                                # 解析让球赔率
                                spreads = period.get("spreads", [])
                                if spreads:
                                    odds_info["spreads"] = []
                                    for spread in spreads:
                                        spread_info = {
                                            "handicap": spread.get("hdp"),
                                            "home_odds": spread.get("home"),
                                            "away_odds": spread.get("away")
                                        }
                                        odds_info["spreads"].append(spread_info)

                                # 解析胜平负赔率
                                moneyline = period.get("moneyline", {})
                                odds_info["moneyline"] = {
                                    "home": moneyline.get("home"),
                                    "away": moneyline.get("away"),
                                    "draw": moneyline.get("draw")
                                }

                                # 解析总进球赔率
                                totals = period.get("totals", [])
                                if totals:
                                    odds_info["totals"] = []
                                    for total in totals:
                                        total_info = {
                                            "points": total.get("points"),
                                            "over_odds": total.get("over"),
                                            "under_odds": total.get("under")
                                        }
                                        odds_info["totals"].append(total_info)

        if odds_info:
            start_time_beijing = datetime.strptime(fixture["start_time_beijing"], "%Y-%m-%d %H:%M:%S")
            start_time_beijing = start_time_beijing.replace(tzinfo=beijing_timezone)
            time_until_start = start_time_beijing - current_time_beijing
            fixture["time_until_start"] = format_time_delta(time_until_start)
            valid_fixtures_with_odds.append((fixture, odds_info))

    if not valid_fixtures_with_odds:
        return jsonify({"message": "未找到有赔率信息的符合条件赛事"}), 404

    data = []
    for fixture, odds_info in valid_fixtures_with_odds:
        data.append({
            "event_id": fixture["event_id"],
            "league_name": fixture["league_name"],
            "home_team": fixture["home_team"],
            "away_team": fixture["away_team"],
            "start_time_beijing": fixture["start_time_beijing"],
            "time_until_start": fixture["time_until_start"],
            "odds_info": odds_info
        })

    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
