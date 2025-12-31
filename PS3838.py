import requests
import uuid
import json
import os
import logging
from datetime import datetime
import base64
from flask import Flask, request, jsonify
from flask_cors import CORS

# ======================
# 配置初始化
# ======================
app = Flask(__name__)
CORS(app)  # 解决跨域问题

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 从环境变量读取账号信息（生产环境推荐）
PS3838_USERNAME = os.getenv("PS3838_USERNAME", "E02A22Q006")
PS3838_PASSWORD = os.getenv("PS3838_PASSWORD", "dddd1111")


# ======================
# 原有投注类（未修改核心逻辑，仅保留）
# ======================
class PS3838BetPlacer:
    """
    PS3838 API v4 直投投注程序
    """

    BASE_URL = "https://api.ps3838.com"

    # 赔率格式选项
    ODDS_FORMATS = {
        "AMERICAN": "AMERICAN",  # 美式赔率
        "DECIMAL": "DECIMAL",  # 欧式赔率
        "HONGKONG": "HONGKONG",  # 香港赔率
        "INDONESIAN": "INDONESIAN",  # 印尼赔率
        "MALAY": "MALAY"  # 马来赔率
    }

    # 金额类型选项
    STAKE_TYPES = {
        "WIN": "WIN",  # 赢取金额
        "RISK": "RISK"  # 风险金额
    }

    # 填充类型选项
    FILL_TYPES = {
        "NORMAL": "NORMAL",  # 按指定金额投注
        "FILLANDKILL": "FILLANDKILL",  # 超过限额则按最大限额投注
        "FILLMAXLIMIT": "FILLMAXLIMIT"  # 始终按最大限额投注
    }

    # 投注类型选项
    BET_TYPES = {
        "MONEYLINE": "MONEYLINE",  # 金钱线
        "SPREAD": "SPREAD",  # 让分
        "TOTAL_POINTS": "TOTAL_POINTS",  # 总分
        "TEAM_TOTAL_POINTS": "TEAM_TOTAL_POINTS"  # 队伍总分
    }

    # 队伍选项
    TEAMS = {
        "TEAM1": "TEAM1",  # 队伍1
        "TEAM2": "TEAM2",  # 队伍2
        "DRAW": "DRAW"  # 平局
    }

    # 侧面选项
    SIDES = {
        "OVER": "OVER",  # 大于
        "UNDER": "UNDER"  # 小于
    }

    def __init__(self, username, password):
        """
        初始化投注器

        Args:
            username (str): API用户名
            password (str): API密码
        """
        self.username = username
        self.password = password
        self.auth_header = self._generate_auth_header()

    def _generate_auth_header(self):
        """
        生成Basic认证头

        Returns:
            dict: 包含认证头的字典
        """
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode('utf-8')
        encoded_auth = base64.b64encode(auth_bytes).decode('utf-8')
        return {"Authorization": f"Basic {encoded_auth}"}

    def place_straight_bet(self, bet_params):
        """
        放置直投投注

        Args:
            bet_params (dict): 投注参数字典

        Returns:
            dict: API响应（即使失败也返回官方原始信息，无响应时返回None）
        """
        url = f"{self.BASE_URL}/v4/bets/place"
        headers = {
            **self.auth_header,
            "Content-Type": "application/json"
        }

        # 确保必填参数存在
        required_params = [
            "oddsFormat", "uniqueRequestId", "acceptBetterLine", "stake",
            "winRiskStake", "lineId", "sportId", "eventId", "periodNumber",
            "betType", "team"
        ]

        for param in required_params:
            if param not in bet_params:
                raise ValueError(f"缺少必填参数: {param}")
        logger.info(
            f"发送给PS3838的handicap参数：值={bet_params.get('handicap')}，类型={type(bet_params.get('handicap'))}")

        # 将参数转换为JSON
        payload = json.dumps(bet_params)

        try:
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            # 无论状态码是否成功，都解析官方响应内容
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                # 若返回非JSON格式，封装原始文本
                response_data = {"raw_response": response.text, "status_code": response.status_code}

            # 打印官方响应到控制台
            logger.info(f"PS3838官方投注响应: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
            return response_data
        except requests.exceptions.RequestException as e:
            logger.error(f"请求失败: {e}")
            error_response = {"error": str(e)}
            # 捕获官方返回的错误信息
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_response["official_error"] = e.response.json()
                except json.JSONDecodeError:
                    error_response["official_error"] = e.response.text
                error_response["status_code"] = e.response.status_code
            # 打印错误信息到控制台
            logger.info(f"PS3838官方投注响应（异常）: {json.dumps(error_response, ensure_ascii=False, indent=2)}")
            return error_response

    def get_account_balance(self):
        """
        查询账户余额（PS3838 v1 API）

        Returns:
            dict: API响应（成功返回余额信息，失败返回官方原始错误，无响应时返回None）
        """
        url = f"{self.BASE_URL}/v1/client/balance"
        headers = {
            **self.auth_header,
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            # 解析响应（兼容非JSON格式）
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {
                    "raw_response": response.text,
                    "status_code": response.status_code
                }

            # 打印官方响应到控制台
            logger.info(f"PS3838官方余额查询响应: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
            return response_data
        except requests.exceptions.RequestException as e:
            logger.error(f"余额查询请求失败: {e}")
            error_response = {"error": str(e)}
            # 捕获官方返回的错误信息
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_response["official_error"] = e.response.json()
                except json.JSONDecodeError:
                    error_response["official_error"] = e.response.text
                error_response["status_code"] = e.response.status_code
            # 打印错误信息到控制台
            logger.info(f"PS3838官方余额查询响应（异常）: {json.dumps(error_response, ensure_ascii=False, indent=2)}")
            return error_response

    def create_bet_params(
            self,
            odds_format="DECIMAL",
            stake=10.0,
            win_risk_stake="RISK",
            line_id=None,
            alt_line_id=None,
            accept_better_line=True,
            pitcher1_must_start=True,
            pitcher2_must_start=True,
            fill_type="NORMAL",
            sport_id=29,  # 默认足球
            event_id=None,
            period_number=0,
            bet_type="MONEYLINE",
            team="TEAM1",
            side=None,
            handicap=None
    ):
        """
        创建投注参数字典，结构清晰，便于配置

        Args:
            odds_format (str): 赔率格式
            stake (float): 投注金额
            win_risk_stake (str): 金额类型 (WIN/RISK)
            line_id (int): 线ID (必填)
            alt_line_id (int): 备用线ID (可选)
            accept_better_line (bool): 是否接受更有利的赔率
            pitcher1_must_start (bool): 投手1必须首发 (仅棒球)
            pitcher2_must_start (bool): 投手2必须首发 (仅棒球)
            fill_type (str): 填充类型
            sport_id (int): 运动ID
            event_id (int): 事件ID (必填)
            period_number (int): 时期编号
            bet_type (str): 投注类型
            team (str): 队伍
            side (str): 侧面 (OVER/UNDER)
            handicap (float): 让分数

        Returns:
            dict: 投注参数字典
        """
        if line_id is None:
            raise ValueError("line_id 是必填参数")

        if event_id is None:
            raise ValueError("event_id 是必填参数")

        # 校验枚举参数合法性
        if odds_format not in self.ODDS_FORMATS.values():
            raise ValueError(f"无效赔率格式，可选值：{list(self.ODDS_FORMATS.values())}")
        if win_risk_stake not in self.STAKE_TYPES.values():
            raise ValueError(f"无效金额类型，可选值：{list(self.STAKE_TYPES.values())}")
        if fill_type not in self.FILL_TYPES.values():
            raise ValueError(f"无效填充类型，可选值：{list(self.FILL_TYPES.values())}")
        if bet_type not in self.BET_TYPES.values():
            raise ValueError(f"无效投注类型，可选值：{list(self.BET_TYPES.values())}")
        if team not in self.TEAMS.values() and bet_type != "TOTAL_POINTS":
            raise ValueError(f"无效队伍，可选值：{list(self.TEAMS.values())}")
        if side and side not in self.SIDES.values():
            raise ValueError(f"无效侧面，可选值：{list(self.SIDES.values())}")
        if stake <= 0:
            raise ValueError("投注金额必须大于0")

        # 创建唯一请求ID
        unique_request_id = str(uuid.uuid4())

        # 构建参数字典
        params = {
            # 基础参数
            "oddsFormat": odds_format,
            "uniqueRequestId": unique_request_id,
            "acceptBetterLine": accept_better_line,
            "stake": stake,
            "winRiskStake": win_risk_stake,

            # 线ID参数
            "lineId": line_id,
            "altLineId": alt_line_id,

            # 棒球专用参数
            "pitcher1MustStart": pitcher1_must_start,
            "pitcher2MustStart": pitcher2_must_start,

            # 填充类型
            "fillType": fill_type,

            # 比赛参数
            "sportId": sport_id,
            "eventId": event_id,
            "periodNumber": period_number,

            # 投注类型参数
            "betType": bet_type,
            "team": team,

            # 可选参数
            "side": side,
            "handicap": handicap
        }

        # 移除值为None的参数
        params = {k: v for k, v in params.items() if v is not None}

        return params


# ======================
# 初始化投注器实例
# ======================
bet_placer = PS3838BetPlacer(PS3838_USERNAME, PS3838_PASSWORD)


# ======================
# 核心新增：两次投注尝试的通用逻辑（修复重复RequestID问题）
# ======================
def place_bet_with_retry(bet_placer, base_bet_params, alt_line_id):
    """
    两次投注尝试逻辑：
    1. 第一次：移除alt_line_id，仅用line_id投注（独立RequestID）
    2. 若失败，立即第二次尝试：添加alt_line_id + 全新RequestID投注
    3. 返回最终结果（成功则终止，失败则返回最后一次结果）
    """
    # 第一次尝试：移除alt_line_id，仅用line_id，使用初始RequestID
    first_params = {k: v for k, v in base_bet_params.items() if k != "altLineId"}
    logger.info("开始第一次投注尝试（仅line_id）")
    first_result = bet_placer.place_straight_bet(first_params)

    # 第一次成功：直接返回
    if isinstance(first_result, dict) and first_result.get("status") == "ACCEPTED":
        logger.info("第一次投注尝试成功，终止重试")
        return first_result, 1  # 1表示第一次成功

    # 第一次失败：立即执行第二次尝试（核心修复：生成全新的uniqueRequestId）
    logger.info("第一次投注尝试失败，立即执行第二次尝试（添加alt_line_id + 全新RequestID）")
    second_params = base_bet_params.copy()
    # 关键修复：重新生成唯一请求ID，避免重复
    second_params["uniqueRequestId"] = str(uuid.uuid4())
    if alt_line_id is not None:
        second_params["altLineId"] = alt_line_id
    # 执行第二次投注
    second_result = bet_placer.place_straight_bet(second_params)
    return second_result, 2  # 2表示第二次尝试（无论成败）


# ======================
# API接口定义（修改核心投注逻辑）
# ======================
@app.route('/api/bet/moneyline', methods=['POST'])
def bet_moneyline():
    """
    接口1：足球金钱线投注（对应示例1）
    请求参数（JSON）：
    {
        "line_id": 420921914,          // 必填 线ID
        "event_id": 7575042611,        // 必填 赛事ID
        "stake": 10.0,                 // 可选 投注金额，默认10.0
        "odds_format": "MALAY",        // 可选 赔率格式，默认MALAY
        "team": "TEAM1",               // 可选 投注队伍，默认TEAM1
        "fill_type": "NORMAL",         // 可选 填充类型，默认NORMAL
        "alt_line_id": 123456          // 可选 备用线ID（第二次尝试用）
    }
    """
    try:
        # 解析请求参数
        req_data = request.get_json() or {}

        # 校验必填参数
        if not req_data.get("line_id"):
            return jsonify({
                "code": 400,
                "msg": "缺少必填参数：line_id",
                "data": None
            }), 400
        if not req_data.get("event_id"):
            return jsonify({
                "code": 400,
                "msg": "缺少必填参数：event_id",
                "data": None
            }), 400

        # 提取alt_line_id（第二次尝试用）
        alt_line_id = req_data.get("alt_line_id")
        if alt_line_id is not None:
            alt_line_id = int(alt_line_id)

        # 构建基础投注参数（含alt_line_id，第一次尝试时会移除）
        bet_params = bet_placer.create_bet_params(
            odds_format=req_data.get("odds_format", "MALAY"),
            stake=float(req_data.get("stake", 10.0)),
            win_risk_stake=bet_placer.STAKE_TYPES["RISK"],
            line_id=int(req_data.get("line_id")),
            alt_line_id=alt_line_id,  # 先传入，第一次尝试时移除
            event_id=int(req_data.get("event_id")),
            sport_id=29,  # 足球
            bet_type=bet_placer.BET_TYPES["MONEYLINE"],
            team=req_data.get("team", "TEAM1"),
            accept_better_line=True,
            fill_type=req_data.get("fill_type", "NORMAL")
        )

        # 执行两次投注尝试
        final_result, attempt_num = place_bet_with_retry(bet_placer, bet_params, alt_line_id)

        # 构造响应
        response = {
            "code": 200 if (isinstance(final_result, dict) and final_result.get("status") == "ACCEPTED") else 500,
            "msg": f"第{attempt_num}次投注{'成功' if (isinstance(final_result, dict) and final_result.get('status') == 'ACCEPTED') else '失败'}：{final_result.get('errorMessage', '未知错误') if isinstance(final_result, dict) else '请求异常'}",
            "data": {
                "attempt_number": attempt_num,  # 标记是第几次成功/失败
                "bet_params": bet_params,
                "official_api_response": final_result,
                "bet_status": final_result.get("status") if isinstance(final_result, dict) else "FAILED"
            }
        }

        return jsonify(response)

    except ValueError as e:
        logger.error(f"参数错误：{str(e)}")
        return jsonify({
            "code": 400,
            "msg": f"参数错误：{str(e)}",
            "data": None
        }), 400
    except Exception as e:
        logger.error(f"服务器异常：{str(e)}", exc_info=True)
        return jsonify({
            "code": 500,
            "msg": f"服务器异常：{str(e)}",
            "data": None
        }), 500


@app.route('/api/bet/spread', methods=['POST'])
def bet_spread():
    """
    接口2：足球让分盘投注（对应示例2）
    请求参数（JSON）：
    {
        "line_id": 3389737511,         // 必填 线ID
        "event_id": 1621265360,        // 必填 赛事ID
        "stake": 150.0,                // 可选 投注金额，默认150.0
        "odds_format": "MALAY",        // 可选 赔率格式，默认MALAY
        "team": "TEAM2",               // 可选 投注队伍，默认TEAM2
        "handicap": 0.75,              // 必填 让分数
        "fill_type": "NORMAL",         // 可选 填充类型，默认NORMAL
        "alt_line_id": 123456          // 可选 备用线ID（第二次尝试用）
    }
    """
    try:
        # 解析请求参数
        req_data = request.get_json() or {}
        logger.info(f"接收到投注请求参数: {json.dumps(req_data, ensure_ascii=False)}")

        # 校验必填参数
        required = ["line_id", "event_id", "handicap"]
        for param in required:
            if param not in req_data or req_data[param] is None:
                return jsonify({"code": 400, "msg": f"缺少必填参数：{param}"})

            # 特别针对handicap的数字验证
            if param == "handicap":
                try:
                    float(req_data[param])  # 确保可转换为浮点数
                except (TypeError, ValueError):
                    return jsonify({"code": 400, "msg": f"handicap参数必须是有效数字"})

        # 处理参数类型
        accept_better_line = req_data.get("accept_better_line", True)
        handicap_val = req_data.get("handicap")
        try:
            handicap_float = float(handicap_val)
            handicap = int(handicap_float) if handicap_float.is_integer() else handicap_float
        except (TypeError, ValueError):
            return jsonify({"code": 400, "msg": f"handicap参数必须是有效数字"})

        # 提取alt_line_id（第二次尝试用）
        alt_line_id = req_data.get("alt_line_id")
        if alt_line_id is not None:
            alt_line_id = int(alt_line_id)

        # 构建基础投注参数
        bet_params = bet_placer.create_bet_params(
            odds_format=req_data.get("odds_format", "MALAY"),
            stake=float(req_data.get("stake", 150.0)),
            win_risk_stake=bet_placer.STAKE_TYPES["RISK"],
            line_id=int(req_data.get("line_id")),
            alt_line_id=alt_line_id,  # 先传入，第一次尝试时移除
            event_id=int(req_data.get("event_id")),
            sport_id=29,  # 足球
            bet_type=bet_placer.BET_TYPES["SPREAD"],
            team=req_data.get("team", "TEAM2"),
            handicap=handicap,
            accept_better_line=accept_better_line,
            fill_type=req_data.get("fill_type", "NORMAL")
        )

        # 执行两次投注尝试
        final_result, attempt_num = place_bet_with_retry(bet_placer, bet_params, alt_line_id)

        # 构造响应
        response = {
            "code": 200 if (isinstance(final_result, dict) and final_result.get("status") == "ACCEPTED") else 500,
            "msg": f"第{attempt_num}次投注{'成功' if (isinstance(final_result, dict) and final_result.get('status') == 'ACCEPTED') else '失败'}：{final_result.get('errorMessage', '未知错误') if isinstance(final_result, dict) else '请求异常'}",
            "data": {
                "attempt_number": attempt_num,
                "bet_params": bet_params,
                "official_api_response": final_result,
                "bet_status": final_result.get("status") if isinstance(final_result, dict) else "FAILED"
            }
        }

        return jsonify(response)

    except ValueError as e:
        logger.error(f"参数错误：{str(e)}")
        return jsonify({
            "code": 400,
            "msg": f"参数错误：{str(e)}",
            "data": None
        }), 400
    except Exception as e:
        logger.error(f"服务器异常：{str(e)}", exc_info=True)
        return jsonify({
            "code": 500,
            "msg": f"服务器异常：{str(e)}",
            "data": None
        }), 500


@app.route('/api/bet/total', methods=['POST'])
def bet_total():
    """
    接口3：足球总分盘投注（对应示例3）
    请求参数（JSON）：
    {
        "line_id": 3389737511,         // 必填 线ID
        "event_id": 1621265360,        // 必填 赛事ID
        "stake": 150.0,                // 可选 投注金额，默认150.0
        "odds_format": "MALAY",        // 可选 赔率格式，默认MALAY
        "side": "UNDER",               // 必填 投注方向（OVER/UNDER）
        "handicap": 2.5,               // 必填 总分值
        "fill_type": "NORMAL",         // 可选 填充类型，默认NORMAL
        "alt_line_id": 123456          // 可选 备用线ID（第二次尝试用）
    }
    """
    try:
        # 解析请求参数
        req_data = request.get_json() or {}
        logger.info(f"接收到投注请求参数: {json.dumps(req_data, ensure_ascii=False)}")

        # 校验必填参数
        required = ["line_id", "event_id", "side", "handicap"]
        for param in required:
            if param not in req_data or req_data[param] is None:
                return jsonify({"code": 400, "msg": f"缺少必填参数：{param}"})

            # 特别针对handicap的数字验证
            if param == "handicap":
                try:
                    float(req_data[param])  # 确保可转换为浮点数
                except (TypeError, ValueError):
                    return jsonify({"code": 400, "msg": f"handicap参数必须是有效数字"})

        # 处理参数类型
        accept_better_line = req_data.get("accept_better_line", True)
        handicap_val = req_data.get("handicap")
        try:
            handicap_float = float(handicap_val)
            handicap = int(handicap_float) if handicap_float.is_integer() else handicap_float
        except (TypeError, ValueError):
            return jsonify({"code": 400, "msg": f"handicap参数必须是有效数字"})

        # 提取alt_line_id（第二次尝试用）
        alt_line_id = req_data.get("alt_line_id")
        if alt_line_id is not None:
            alt_line_id = int(alt_line_id)

        # 构建基础投注参数
        bet_params = bet_placer.create_bet_params(
            odds_format=req_data.get("odds_format", "MALAY"),
            stake=float(req_data.get("stake", 150.0)),
            win_risk_stake=bet_placer.STAKE_TYPES["RISK"],
            line_id=int(req_data.get("line_id")),
            alt_line_id=alt_line_id,  # 先传入，第一次尝试时移除
            event_id=int(req_data.get("event_id")),
            sport_id=29,  # 足球
            bet_type=bet_placer.BET_TYPES["TOTAL_POINTS"],
            team="TEAM1",  # 总分盘team字段无实际意义
            side=req_data.get("side"),
            handicap=handicap,
            accept_better_line=accept_better_line,
            fill_type=req_data.get("fill_type", "NORMAL")
        )

        # 执行两次投注尝试
        final_result, attempt_num = place_bet_with_retry(bet_placer, bet_params, alt_line_id)

        # 构造响应
        response = {
            "code": 200 if (isinstance(final_result, dict) and final_result.get("status") == "ACCEPTED") else 500,
            "msg": f"第{attempt_num}次投注{'成功' if (isinstance(final_result, dict) and final_result.get('status') == 'ACCEPTED') else '失败'}：{final_result.get('errorMessage', '未知错误') if isinstance(final_result, dict) else '请求异常'}",
            "data": {
                "attempt_number": attempt_num,
                "bet_params": bet_params,
                "official_api_response": final_result,
                "bet_status": final_result.get("status") if isinstance(final_result, dict) else "FAILED"
            }
        }

        return jsonify(response)

    except ValueError as e:
        logger.error(f"参数错误：{str(e)}")
        return jsonify({
            "code": 400,
            "msg": f"参数错误：{str(e)}",
            "data": None
        }), 400
    except Exception as e:
        logger.error(f"服务器异常：{str(e)}", exc_info=True)
        return jsonify({
            "code": 500,
            "msg": f"服务器异常：{str(e)}",
            "data": None
        }), 500


@app.route('/api/account/balance', methods=['GET'])
def get_balance():
    """
    接口4：账户余额查询（PS3838 v1 API）
    """
    try:
        # 执行余额查询
        balance_result = bet_placer.get_account_balance()

        # 构造标准化响应
        response = {
            "code": 200,
            "msg": "请求成功",
            "data": {
                "official_api_response": balance_result,
                "balance_info": {}  # 友好化余额信息（可选）
            }
        }

        # 处理成功响应（解析余额字段）
        if isinstance(balance_result, dict):
            # 校验是否为合法的余额响应
            if all(key in balance_result for key in
                   ["availableBalance", "outstandingTransactions", "givenCredit", "currency"]):
                response["data"]["balance_info"] = {
                    "可用投注余额": balance_result["availableBalance"],
                    "未结算投注金额": balance_result["outstandingTransactions"],
                    "信用额度": balance_result["givenCredit"],
                    "币种": balance_result["currency"]
                }
            # 处理官方错误响应
            elif "code" in balance_result or "message" in balance_result:
                response["code"] = 500
                response["msg"] = f"余额查询失败：{balance_result.get('message', '未知错误')}"
        # 处理请求异常
        elif balance_result is None:
            response["code"] = 500
            response["msg"] = "余额查询失败：无API响应"

        return jsonify(response)

    except Exception as e:
        logger.error(f"余额查询接口异常：{str(e)}", exc_info=True)
        return jsonify({
            "code": 500,
            "msg": f"服务器异常：{str(e)}",
            "data": None
        }), 500


# ======================
# 健康检查接口
# ======================
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "code": 200,
        "msg": "服务正常",
        "data": {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "service": "ps3838-bet-api"
        }
    })


# ======================
# 启动服务
# ======================
if __name__ == "__main__":
    # 生产环境建议使用 Gunicorn/uWSGI 部署，此处为测试用
    app.run(host="0.0.0.0", port=5041, debug=False)