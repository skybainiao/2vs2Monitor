import requests
import base64
import json
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse

# 配置信息
USERNAME = "GA1A711S00"
PASSWORD = "dddd1111"
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


# ------------------------------ 主逻辑 ------------------------------
def main():
    # 1. 获取并筛选赛事
    fixtures_data = get_fixtures()
    if not fixtures_data:
        return

    current_time_utc = datetime.now(timezone.utc)
    today_start_utc = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_utc = current_time_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
    beijing_timezone = timezone(timedelta(hours=8))
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
        print("未找到符合条件的赛事")
        return

    # 2. 根据赛事 ID 获取赔率（需转换为逗号分隔字符串）
    if event_ids:
        params_event_ids = ",".join(map(str, event_ids))
        odds_data = get_odds(params_event_ids)
    else:
        odds_data = None

    # ------------------------------ 解析并打印结果 ------------------------------
    print(f"获取到 {len(valid_fixtures)} 场符合条件的比赛及其赔率：")
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

        # 打印赛事信息
        print(f"\n------------------------ 赛事：{fixture['league_name']} ------------------------")
        print(f"赛事 ID：{event_id}")
        print(f"主客队：{fixture['home_team']} vs {fixture['away_team']}")
        print(f"开始时间（北京时间）：{fixture['start_time_beijing']}")

        # 打印赔率信息（如果有）
        if odds_info:
            print("\n赔率信息（Decimal 格式）：")
            # 打印让球赔率
            if "spreads" in odds_info:
                print("让球赔率：")
                for spread in odds_info["spreads"]:
                    print(f"  盘口：{spread['handicap']} | 主胜：{spread['home_odds']} | 客胜：{spread['away_odds']}")
            # 打印胜平负赔率
            if "moneyline" in odds_info:
                moneyline = odds_info["moneyline"]
                print(f"胜平负：主胜 {moneyline['home']} | 平局 {moneyline['draw']} | 客胜 {moneyline['away']}")
            # 打印总进球赔率
            if "totals" in odds_info:
                print("总进球赔率：")
                for total in odds_info["totals"]:
                    print(f"  盘口：{total['points']} | 大球：{total['over_odds']} | 小球：{total['under_odds']}")
        else:
            print("\n暂无该赛事的赔率信息")


if __name__ == "__main__":
    main()
