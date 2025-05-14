from flask import Flask, jsonify
import requests
from datetime import datetime  # 新增：可能用于时间处理（如果需要）

app = Flask(__name__)

# API 基础路径（保持不变）
BASE_URL = "https://www.isn99.com/membersite-api/api"

# 认证信息（保持不变）
auth_endpoint = "/member/authenticate"
username = "C6850DMA01"
password = "dddd1111DD"

# 新增：Java接口配置（需根据实际情况修改URL）
JAVA_API_URL = "http://160.25.20.18:8080/api/bindings/check-existing"  # 请替换为实际Java服务地址


def get_jwt_token():
    """获取JWT令牌（保持不变）"""
    headers = {"locale": "en_US"}
    data = {"username": username, "password": password}
    while True:
        response = requests.post(f"{BASE_URL}{auth_endpoint}", json=data, headers=headers)
        response.raise_for_status()
        result = response.json()
        token = result.get("token")
        next_action = result.get("nextAction")
        if not next_action:
            return token
        headers["Authorization"] = f"Bearer {token}"
        if next_action == "/member/acceptAgreement":
            requests.post(f"{BASE_URL}{next_action}", headers=headers)


def get_member_odds_group(token):
    """获取会员赔率组ID（保持不变）"""
    headers = {
        "Authorization": f"Bearer {token}",
        "locale": "en_US"
    }
    response = requests.get(f"{BASE_URL}/member/info", headers=headers)
    response.raise_for_status()
    member_info = response.json()
    return member_info["oddsGroupId"]


def get_soccer_events(token, odds_group_id):
    """获取足球比赛信息（保持不变）"""
    sport_id = 1
    schedule_id = 2
    league_id = 0
    odds_format_id = 4

    endpoint = f"/data/events/{sport_id}/{schedule_id}/{league_id}/{odds_format_id}/{odds_group_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "locale": "en_US"
    }
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()


def simplify_to_required_structure(markets):
    """赔率结构化（保持不变）"""
    simplified = {"spreads": {}, "totals": {}}
    for market in markets:
        market_name = market.get('name', '')
        if market_name == "HDP":
            for line in market.get('lines', []):
                for selection in line.get('marketSelections', []):
                    handicap = selection.get('handicap', '0')
                    decimal_odds = selection.get('decimalOdds', '0')
                    indicator = selection.get('indicator', '').lower()
                    if handicap not in simplified["spreads"]:
                        simplified["spreads"][handicap] = {}
                    if indicator == "home":
                        simplified["spreads"][handicap]["home"] = decimal_odds
                    elif indicator == "away":
                        simplified["spreads"][handicap]["away"] = decimal_odds
        elif market_name == "OU":
            for line in market.get('lines', []):
                for selection in line.get('marketSelections', []):
                    handicap = selection.get('handicap', '0')
                    decimal_odds = selection.get('decimalOdds', '0')
                    indicator = selection.get('indicator', '').lower()
                    if handicap not in simplified["totals"]:
                        simplified["totals"][handicap] = {}
                    if indicator == "over":
                        simplified["totals"][handicap]["over"] = decimal_odds
                    elif indicator == "under":
                        simplified["totals"][handicap]["under"] = decimal_odds
    return simplified


@app.route('/get_odds3', methods=['GET'])
def api_get_soccer_events():
    try:
        token = get_jwt_token()
        odds_group_id = get_member_odds_group(token)
        raw_data = get_soccer_events(token, odds_group_id)

        # ------------------------ 新增过滤逻辑 ------------------------
        # 1. 整理待检查的比赛数据（需与Java接口的CheckDuplicateRequest结构一致）
        check_request = {
            "source": 3,  # 数据源编号（需与Java接口约定一致，通常为1/2/3）
            "matches": []
        }
        for league in raw_data.get('schedule', {}).get('leagues', []):
            league_name = league.get('name', '')
            for event in league.get('events', []):
                home_team = event.get('homeTeam', '')
                away_team = event.get('awayTeam', '')
                check_request["matches"].append({
                    "league": league_name,
                    "homeTeam": home_team,
                    "awayTeam": away_team
                })

        # 2. 调用Java接口检查比赛存在性
        java_response = requests.post(
            JAVA_API_URL,
            json=check_request,
            headers={"Content-Type": "application/json"}
        )
        java_response.raise_for_status()
        check_result = java_response.json()

        # 3. 解析Java接口返回结果（假设返回存在的联赛和球队列表）
        existing_leagues = set(check_result.get("leagues", []))
        existing_teams = set(check_result.get("teams", []))

        # 4. 过滤原始数据，仅保留存在于数据库中的比赛
        filtered_matches = []
        for league in raw_data.get('schedule', {}).get('leagues', []):
            league_name = league.get('name', '')
            if league_name not in existing_leagues:
                continue  # 跳过不存在的联赛

            for event in league.get('events', []):
                home_team = event.get('homeTeam', '')
                away_team = event.get('awayTeam', '')

                # 检查主/客队是否存在于数据库中（根据业务需求调整逻辑）
                if home_team in existing_teams and away_team in existing_teams:
                    # 处理时间格式（保持原有逻辑）
                    utc_time_str = event.get('startTimeServer', '')
                    time_format = "00:00"
                    if utc_time_str:
                        try:
                            time_parts = utc_time_str.split(" ")[1].split(":")[:2]
                            time_format = ":".join(time_parts)
                        except IndexError:
                            pass

                    # 提取赔率
                    odds = simplify_to_required_structure(
                        event.get('odds', []) or event.get('markets', [])
                    )

                    filtered_matches.append({
                        "league_name": league_name,
                        "home_team": home_team,
                        "away_team": away_team,
                        "time": time_format,
                        "odds": odds
                    })

        return jsonify(filtered_matches)

    except requests.exceptions.HTTPError as e:
        return jsonify({
            "error": f"HTTP Error: {e.response.status_code}, Details: {e.response.text}"
        }), 500
    except requests.exceptions.RequestException as e:  # 新增：处理Java接口调用异常
        return jsonify({
            "error": f"Java接口调用失败: {str(e)}"
        }), 503
    except Exception as e:
        return jsonify({"error": f"Error: {str(e)}"}), 500


@app.after_request
def add_cors_headers(response):
    """跨域支持（保持不变）"""
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)