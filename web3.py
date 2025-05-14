from flask import Flask, jsonify
import requests

app = Flask(__name__)

# API 基础路径
BASE_URL = "https://www.isn99.com/membersite-api/api"

# 认证信息
auth_endpoint = "/member/authenticate"
username = "C6850DMA01"
password = "dddd1111DD"


def get_jwt_token():
    """获取 JWT 令牌，并处理 nextAction（如有）"""
    headers = {"locale": "en_US"}  # 设置英文语言
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
        # 其他 nextAction 处理逻辑...


def get_member_odds_group(token):
    """获取会员赔率组 ID（oddsGroupId）"""
    headers = {
        "Authorization": f"Bearer {token}",
        "locale": "en_US"  # 设置英文语言
    }
    response = requests.get(f"{BASE_URL}/member/info", headers=headers)
    response.raise_for_status()
    member_info = response.json()
    return member_info["oddsGroupId"]


def get_soccer_events(token, odds_group_id):
    """获取足球比赛信息（马来盘赔率，英文输出）"""
    sport_id = 1  # Soccer 的 sportId
    schedule_id = 2  # scheduleId 枚举值：1, 2, 3（示例选 1）
    league_id = 0  # leagueId 0 表示所有联赛
    odds_format_id = 4  # 马来盘对应的 oddsFormatId（根据文档枚举值，4 为马来盘）

    endpoint = f"/data/events/{sport_id}/{schedule_id}/{league_id}/{odds_format_id}/{odds_group_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "locale": "en_US"  # 设置英文语言
    }
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()


@app.route('/get_odds3', methods=['GET'])
def api_get_soccer_events():
    try:
        token = get_jwt_token()
        odds_group_id = get_member_odds_group(token)
        raw_data = get_soccer_events(token, odds_group_id)

        matches = []  # 使用英文数组名（matches/items等均可）

        for league in raw_data.get('schedule', {}).get('leagues', []):
            for event in league.get('events', []):
                home_team = event.get('homeTeam', '')
                away_team = event.get('awayTeam', '')
                league_name = league.get('name', '')
                utc_time_str = event.get('startTimeServer', '')

                # 处理时间（保持原有逻辑）
                time_format = "00:00"
                if utc_time_str:
                    try:
                        time_parts = utc_time_str.split(" ")[1].split(":")[:2]
                        time_format = ":".join(time_parts)
                    except IndexError:
                        pass

                # 提取赔率（使用英文结构）
                odds = simplify_to_required_structure(
                    event.get('odds', []) or event.get('markets', [])
                )

                matches.append({
                    "league_name": league_name,  # 联赛名称（英文）
                    "home_team": home_team,  # 主队
                    "away_team": away_team,  # 客队
                    "time": time_format,  # 时间（英文）
                    "odds": odds  # 赔率（英文结构）
                })

        return jsonify(matches)  # 直接返回英文键名的数组

    except requests.exceptions.HTTPError as e:
        return jsonify({
            "error": f"HTTP Error: {e.response.status_code}, Details: {e.response.text}"
        }), 500
    except Exception as e:
        return jsonify({"error": f"Error: {str(e)}"}), 500


def simplify_to_required_structure(markets):
    """按指定结构提取让球盘（HDP）和大小球（OU）数据"""
    simplified = {
        "spreads": {},  # 让球盘（主/客队赔率）
        "totals": {}  # 大小球（Over/Under赔率）
    }

    for market in markets:
        market_name = market.get('name', '')

        # 处理让球盘（HDP）市场
        if market_name == "HDP":
            for line in market.get('lines', []):
                for selection in line.get('marketSelections', []):
                    handicap = selection.get('handicap', '0')
                    decimal_odds = selection.get('decimalOdds', '0')
                    indicator = selection.get('indicator', '').lower()  # 获取主/客队标识（home/away）

                    # 按盘口分组，合并主/客队赔率（同盘口可能有多个赔率，取最后一个或覆盖）
                    if handicap not in simplified["spreads"]:
                        simplified["spreads"][handicap] = {}
                    if indicator == "home":
                        simplified["spreads"][handicap]["home"] = decimal_odds
                    elif indicator == "away":
                        simplified["spreads"][handicap]["away"] = decimal_odds

        # 处理大小球（OU）市场
        elif market_name == "OU":
            for line in market.get('lines', []):
                for selection in line.get('marketSelections', []):
                    handicap = selection.get('handicap', '0')
                    decimal_odds = selection.get('decimalOdds', '0')
                    indicator = selection.get('indicator', '').lower()  # 获取Over/Under标识（over/under）

                    # 按盘口分组，合并Over/Under赔率（同盘口可能有多个赔率，取最后一个或覆盖）
                    if handicap not in simplified["totals"]:
                        simplified["totals"][handicap] = {}
                    if indicator == "over":
                        simplified["totals"][handicap]["over"] = decimal_odds
                    elif indicator == "under":
                        simplified["totals"][handicap]["under"] = decimal_odds

    return simplified


@app.after_request
def add_cors_headers(response):
    # 允许所有来源的跨域请求
    response.headers['Access-Control-Allow-Origin'] = '*'
    # 允许的请求方法
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    # 允许的请求头
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)