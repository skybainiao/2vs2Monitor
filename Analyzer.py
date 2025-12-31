import time
import requests
import asyncio
import websockets
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional, Any
from flask import Flask, jsonify, request
import threading
from flask_cors import CORS

app = Flask(__name__)
calculator_instance = None  # 全局计算器实例引用
CORS(app)  # 允许跨域请求


class HighFrequencyOddsCalculator:
    def __init__(self):
        # 核心配置参数
        self.HIGH_FREQ_API_URL = "http://160.25.20.18:8766/api/upcoming-odds-full"  # 高频计算API地址
        self.CHECK_INTERVAL = 10  # 检查间隔时间(秒)
        self.running = False  # 运行状态标志
        self.beijing_tz = timezone(timedelta(hours=8))  # 北京时区(UTC+8)

        # 旧程序WebSocket配置
        self.OLD_PROGRAM_WS_URL = "ws://160.25.20.18:8765"  # 旧程序WebSocket地址
        self.old_program_connected = False  # 连接状态
        self.old_program_ws = None  # WebSocket连接实例
        self.old_data_cache = {}  # 旧程序数据缓存
        self.ws_running = False  # WebSocket运行标志（新增）
        self.ws_client_thread = None  # WebSocket线程引用（调整）

        # 发送配置（所有模式固定0分钟阈值）
        self.SEND_MIN_THRESHOLD_MINUTES = 0  # 全局发送时间阈值（0分钟）
        self.SEND_HIGH_ENABLED = False  # high模式发送开关
        self.SEND_LOW_ENABLED = False  # low模式发送开关
        self.SEND_MIN_ENABLED = False  # min模式发送开关
        self.TARGET_API_URL = "http://154.222.29.200:5030/proxy_bet_request"  # 目标服务器接口
        self.sent_items = set()  # 已发送盘口唯一标识（防重复）
        # 核心修改：按模式区分发送记录 {"high": {"主队_客队": {...}}, "low": {...}, "min": {...}}
        self.match_sent_records = {
            "high": {},
            "low": {},
            "min": {}
        }

        # low模式指定盘口值（固定12个）
        self.LOW_MODE_ALLOWED_HANDICAPS = {
            "-2.25", "-2", "-1.75", "-1.25", "-0.5",
            "0.5", "0.75", "1", "1.75", "2", "2.25", "2.5"
        }

        # 最新计算结果存储
        self.latest_results = {
            "high": [],
            "low": [],
            "min": [],
            "calculation_time": None
        }

        # API控制相关
        self.calculation_active = False  # 计算激活状态
        self.calculation_thread = None  # 计算线程引用

        # 移除：初始化时不再自动启动WebSocket
        # self.ws_client_thread = threading.Thread(target=self.start_ws_client, daemon=True)
        # self.ws_client_thread.start()

    # ---------------------- WebSocket客户端功能（核心修改） ----------------------
    def start_ws_client(self):
        """启动WebSocket客户端线程（仅在监控启动时调用）"""
        if self.ws_running:
            print("⚠️ WebSocket客户端已在运行中")
            return

        self.ws_running = True
        print(f"启动WebSocket客户端，尝试连接旧程序: {self.OLD_PROGRAM_WS_URL}")
        # 创建新的事件循环并运行WebSocket客户端
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.ws_client_loop())
        loop.close()

    def stop_ws_client(self):
        """停止WebSocket客户端（监控停止时调用）"""
        if not self.ws_running:
            print("⚠️ WebSocket客户端未运行")
            return

        self.ws_running = False
        print("🛑 正在关闭WebSocket连接...")

        # 主动关闭现有WebSocket连接
        if self.old_program_ws and not self.old_program_ws.closed:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.old_program_ws.close())
            loop.close()

        # 等待WebSocket线程结束
        if self.ws_client_thread and self.ws_client_thread.is_alive():
            self.ws_client_thread.join(timeout=5)

        self.old_program_connected = False
        self.old_program_ws = None
        print("✅ WebSocket连接已关闭")

    async def ws_client_loop(self):
        """WebSocket客户端主循环（自动重连，受ws_running控制）"""
        while self.ws_running:  # 核心修改：仅在ws_running为True时运行
            try:
                async with websockets.connect(self.OLD_PROGRAM_WS_URL, max_size=10 * 1024 * 1024) as websocket:
                    self.old_program_ws = websocket
                    self.old_program_connected = True
                    print(f"✅ 成功连接到旧程序: {self.OLD_PROGRAM_WS_URL}")

                    # 持续接收数据（同时检查运行状态）
                    async for message in websocket:
                        if not self.ws_running:  # 检测到停止信号，立即退出
                            break
                        await self.process_old_program_data(message)

            except Exception as e:
                if self.ws_running:  # 仅在运行中时才提示重连
                    self.old_program_connected = False
                    self.old_program_ws = None
                    print(f"❌ 与旧程序的连接断开: {str(e)}，5秒后重连")
                    await asyncio.sleep(5)
                else:
                    break  # 非运行状态，直接退出循环

        # 循环结束后清理状态
        self.old_program_connected = False
        self.old_program_ws = None
        print("🔌 WebSocket客户端已停止运行")

    async def process_old_program_data(self, message: str):
        """处理从旧程序接收到的数据"""
        try:
            message_data = json.loads(message)
            self.old_data_cache = {
                "cache_update_time": datetime.now(self.beijing_tz).strftime("%Y-%m-%d %H:%M:%S"),
                "connected": self.old_program_connected,
                "data": message_data
            }
            match_count = len(message_data.get("matches", []))
            print(f"📥 WebSocket接收数据：{match_count}场比赛，缓存已更新")
        except Exception as e:
            print(f"❌ WebSocket数据处理错误：{str(e)}，原始消息：{message[:200]}...")

    # ---------------------- source2盘口过滤 ----------------------
    def get_source2_spreads(self) -> Dict[str, Dict[str, List[str]]]:
        """从缓存中提取source2盘口数据"""
        source2_spreads = {}
        print(f"\n🔍 开始提取source2盘口数据（缓存状态：{'有数据' if 'data' in self.old_data_cache else '无数据'}）")

        if not isinstance(self.old_data_cache, dict) or "data" not in self.old_data_cache:
            print("❌ 缓存缺少data字段，跳过source2提取")
            return source2_spreads

        data = self.old_data_cache["data"]
        if not isinstance(data, dict) or "matches" not in data or not isinstance(data["matches"], list):
            print("❌ 缓存中无有效matches列表，跳过source2提取")
            return source2_spreads

        matches = data["matches"]
        source2_found = 0
        source2_has_spread = 0

        for match_idx, match in enumerate(matches):
            if not isinstance(match, dict):
                continue

            # 标准化主客队名称
            home = match.get("home_team", "").strip().replace(" ", "").lower()
            away = match.get("away_team", "").strip().replace(" ", "").lower()
            if not home or not away:
                continue
            match_key = f"{home}vs{away}"
            source2_spreads[match_key] = {"home": [], "away": []}

            # 查找source=2的数据源
            source2 = None
            for src in match.get("sources", []):
                if isinstance(src, dict) and src.get("source") == 2:
                    source2 = src
                    source2_found += 1
                    break

            if not source2:
                del source2_spreads[match_key]
                continue

            # 提取盘口数据
            spreads = source2.get("odds", {}).get("spreads", {})
            if not isinstance(spreads, dict) or len(spreads) == 0:
                del source2_spreads[match_key]
                continue

            for spread_str, side_data in spreads.items():
                if not isinstance(side_data, dict):
                    continue
                normalized_spread = str(spread_str).strip()
                if "home" in side_data:
                    source2_spreads[match_key]["home"].append(normalized_spread)
                if "away" in side_data:
                    source2_spreads[match_key]["away"].append(normalized_spread)

            if len(source2_spreads[match_key]["home"]) + len(source2_spreads[match_key]["away"]) > 0:
                source2_has_spread += 1
            else:
                del source2_spreads[match_key]

        print(
            f"✅ source2提取完成：{len(matches)}场比赛 → {source2_found}个source2数据源 → {source2_has_spread}个有效盘口")
        print(f"📊 可匹配比赛数：{len(source2_spreads)}场")
        return source2_spreads

    def filter_by_source2_spreads(self, results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """过滤高频计算结果（仅保留source2中存在的盘口）"""
        source2_spreads = self.get_source2_spreads()
        if not source2_spreads:
            print("⚠️ 无有效source2数据，跳过过滤")
            return results

        filtered = {"high": [], "low": [], "min": []}
        total_checked = 0
        total_filtered = 0

        for result_type, target_list in filtered.items():
            for item in results[result_type]:
                total_checked += 1
                # 标准化匹配键
                home = item.get("home_team", "").strip().replace(" ", "").lower()
                away = item.get("away_team", "").strip().replace(" ", "").lower()
                match_key = f"{home}vs{away}"
                calc_handicap = str(item.get("handicap", "")).strip()
                side = item.get("side", "").lower().strip()

                # 过滤逻辑
                if match_key not in source2_spreads or side not in ["home", "away"]:
                    total_filtered += 1
                    continue

                if calc_handicap in source2_spreads[match_key][side]:
                    target_list.append(item)
                    print(f"✅ {result_type}保留：{match_key} [{calc_handicap}/{side}]（source2存在）")
                else:
                    total_filtered += 1
                    print(f"❌ {result_type}过滤：{match_key} [{calc_handicap}/{side}]（source2无此盘口）")

        print(
            f"\n📊 source2过滤统计：检查{total_checked}个盘口 → 保留high:{len(filtered['high'])} | low:{len(filtered['low'])} | min:{len(filtered['min'])} → 过滤{total_filtered}个")
        return filtered

    # ---------------------- 模式专属盘口过滤 ----------------------
    def filter_high_mode_handicap(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """high模式：仅保留盘口值≥0的项"""
        filtered = []
        for item in items:
            handicap = item.get("handicap")
            try:
                handicap_num = float(handicap) if handicap is not None else -1
                if handicap_num >= 0:
                    filtered.append(item)
                    print(f"✅ high保留：{item.get('home_team')} vs {item.get('away_team')} → 盘口{handicap}（≥0）")
                else:
                    print(f"❌ high过滤：{item.get('home_team')} vs {item.get('away_team')} → 盘口{handicap}（<0）")
            except (ValueError, TypeError):
                print(f"❌ high过滤：{item.get('home_team')} vs {item.get('away_team')} → 盘口{handicap}（非数字）")
        return filtered

    def filter_low_mode_handicap(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """low模式：仅保留指定12个盘口值"""
        filtered = []
        for item in items:
            handicap = str(item.get("handicap", "")).strip()
            if handicap in self.LOW_MODE_ALLOWED_HANDICAPS:
                filtered.append(item)
                print(f"✅ low保留：{item.get('home_team')} vs {item.get('away_team')} → 盘口{handicap}（指定列表内）")
            else:
                print(f"❌ low过滤：{item.get('home_team')} vs {item.get('away_team')} → 盘口{handicap}（不在指定列表）")
        return filtered

    # ---------------------- 通用工具方法 ----------------------
    def safe_number(self, v: Any) -> Optional[float]:
        """安全转换为数字"""
        try:
            v = float(v)
            return v if float('-inf') < v < float('inf') else None
        except (ValueError, TypeError):
            return None

    def get_time_remaining(self, start_time_str: str) -> str:
        """计算比赛剩余时间"""
        if not start_time_str:
            return "时间未知"
        try:
            start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=self.beijing_tz)
            current_time = datetime.now(self.beijing_tz)
            time_diff = start_time - current_time

            if time_diff.total_seconds() < 0:
                return "已开赛"

            hours, remainder = divmod(int(time_diff.total_seconds()), 3600)
            minutes = remainder // 60
            return f"{hours}小时{minutes}分钟" if hours > 0 else f"{minutes}分钟"
        except ValueError:
            return "时间格式错误"

    def malay_to_probability(self, m: float) -> Optional[float]:
        """马来赔率转概率"""
        if m is None or not isinstance(m, (int, float)) or m != m:
            return None
        if m >= 0:
            return 1 / (1 + m)
        return (-m) / (1 - m)

    def probability_to_malay(self, p: float) -> Optional[float]:
        """概率转马来赔率"""
        if p is None or p <= 0:
            return None
        decimal = 1 / p
        if decimal > 2:
            return -1 / (decimal - 1)
        return decimal - 1

    def find_odds_list(self, match: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取比赛赔率列表"""
        odds_list = []
        if not isinstance(match, dict) or not isinstance(match.get('spread_odds'), list):
            return odds_list

        for item in match['spread_odds']:
            if not isinstance(item, dict):
                continue

            handicap = item.get('spread_value')
            side = item.get('side')
            sources = item.get('sources', {})

            for src, odds_items in sources.items():
                if not isinstance(odds_items, list):
                    continue
                for o in odds_items:
                    price_num = self.safe_number(o.get('odds'))
                    if price_num is not None:
                        odds_list.append({
                            'market': "spread",
                            'handicap': handicap,
                            'side': side,
                            'source': str(src),
                            'price': price_num,
                            'time': o.get('time')
                        })
        return odds_list

    def fetch_data_from_high_freq_api(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """从高频API获取比赛数据"""
        try:
            response = requests.get(self.HIGH_FREQ_API_URL, timeout=240)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "success":
                return False, [], f"API返回错误: {data.get('message', '未知错误')}"

            match_count = data.get("count", 0)
            return True, data.get("data", []), f"成功获取{match_count}场比赛数据"
        except requests.exceptions.RequestException as e:
            return False, [], f"请求失败: {str(e)}"

    # ---------------------- 发送逻辑 ----------------------
    def parse_remaining_time(self, time_str: str) -> Optional[int]:
        """解析剩余时间为分钟数"""
        if not time_str or time_str in ["时间未知", "时间格式错误", "已开赛"]:
            return None

        try:
            if "小时" in time_str and "分钟" in time_str:
                hours = int(time_str.split("小时")[0])
                minutes = int(time_str.split("小时")[1].split("分钟")[0])
                return hours * 60 + minutes
            elif "分钟" in time_str:
                return int(time_str.split("分钟")[0])
            elif "小时" in time_str:
                return int(time_str.split("小时")[0]) * 60
            return None
        except (ValueError, IndexError):
            return None

    def create_unique_identifier(self, item: Dict[str, Any], mode: str) -> str:
        """生成盘口唯一标识（防重复发送）"""
        return f"{mode}_{item.get('league', '')}_{item.get('home_team', '')}_{item.get('away_team', '')}_" \
               f"{item.get('handicap', '')}_{item.get('side', '')}"

    def send_to_target_server(self, item: Dict[str, Any], alert_type: str) -> bool:
        """发送数据到目标服务器"""
        try:
            send_data = {
                'alert': {
                    'league_name': item.get('league', '未知联赛'),
                    'home_team': item.get('home_team', '未知主队'),
                    'away_team': item.get('away_team', '未知客队'),
                    'bet_type_name': f"SPREAD_FT_{item.get('handicap', '')}",
                    'odds_name': 'HomeOdds' if item.get('side', '').lower() == 'home' else 'AwayOdds',
                    'match_type': '',
                    'cancel_on_odds_change': False
                },
                'alert_type': alert_type
            }

            response = requests.post(self.TARGET_API_URL, json=send_data, timeout=10)
            response.raise_for_status()
            result = response.json()

            if result.get("status") == "success":
                print(f"✅ 发送成功 [{alert_type}]：{item.get('home_team')} vs {item.get('away_team')}")
                return True
            else:
                print(f"❌ 发送失败 [{alert_type}]：服务器返回 {result.get('message', '未知错误')}")
                return False
        except Exception as e:
            print(f"❌ 发送异常 [{alert_type}]：{str(e)}")
            return False

    def check_and_send_eligible_items(self):
        """检查并发送符合条件的盘口（所有模式固定0分钟阈值，按模式区分发送限制）"""
        for result_type in ["high", "low", "min"]:
            # 检查模式开关
            if not getattr(self, f"SEND_{result_type.upper()}_ENABLED"):
                continue

            items_to_keep = []
            for item in self.latest_results[result_type]:
                item_id = self.create_unique_identifier(item, result_type)
                home = item.get("home_team", "未知主队")
                away = item.get("away_team", "未知客队")
                match_key = f"{home}_{away}"
                current_side = item.get("side", "").lower()

                # 跳过已发送的盘口
                if item_id in self.sent_items:
                    items_to_keep.append(item)
                    continue

                # 核心修改：按模式获取发送记录（不再全局共享）
                match_record = self.match_sent_records[result_type].get(match_key, {"sent_count": 0, "side": None})
                send_allowed = False

                if match_record["sent_count"] == 0:
                    send_allowed = True
                elif match_record["sent_count"] == 1 and match_record["side"] == current_side:
                    send_allowed = True
                else:
                    print(f"❌ 发送限制 [{result_type}]：{match_key} 已发送{match_record['sent_count']}条，拒绝发送")
                    items_to_keep.append(item)
                    continue

                # 检查时间条件（固定0分钟）
                remaining_time = self.parse_remaining_time(item.get('time_remaining', ''))
                if remaining_time is None or remaining_time > self.SEND_MIN_THRESHOLD_MINUTES:
                    items_to_keep.append(item)
                    continue

                # 发送数据
                if send_allowed and self.send_to_target_server(item, result_type):
                    self.sent_items.add(item_id)
                    # 核心修改：按模式更新发送记录
                    self.match_sent_records[result_type][match_key] = {
                        "sent_count": match_record["sent_count"] + 1,
                        "side": current_side
                    }
                    print(
                        f"📤 已发送 [{result_type}]：{match_key}（剩余{remaining_time}分钟，{result_type}模式累计{match_record['sent_count'] + 1}条）")
                else:
                    items_to_keep.append(item)

            # 更新保留的未发送项
            self.latest_results[result_type] = items_to_keep

    # ---------------------- 对手盘过滤 ----------------------
    def filter_opposite_handicaps(self, filtered_results: Dict[str, List[Dict[str, Any]]]) -> Dict[
        str, List[Dict[str, Any]]]:
        """过滤同一比赛的对手盘（保留差值绝对值更大的）"""
        final_results = {"high": [], "low": [], "min": []}

        for mode in ["high", "low", "min"]:
            # 按比赛分组
            match_groups = {}
            for handicap in filtered_results[mode]:
                match_key = f"{handicap.get('home_team')} vs {handicap.get('away_team')}"
                if match_key not in match_groups:
                    match_groups[match_key] = []
                match_groups[match_key].append(handicap)

            # 处理每组比赛的对手盘
            for match_key, handicaps in match_groups.items():
                if len(handicaps) <= 1:
                    final_results[mode].extend(handicaps)
                    continue

                # 查找对手盘对
                used = set()
                opposite_pairs = []

                for i, h1 in enumerate(handicaps):
                    if i in used:
                        continue
                    h1_handi = self.safe_number(h1.get("handicap", 0)) or 0.0
                    h1_side = h1.get("side", "")

                    for j, h2 in enumerate(handicaps[i + 1:], i + 1):
                        if j in used:
                            continue
                        h2_handi = self.safe_number(h2.get("handicap", 0)) or 0.0
                        h2_side = h2.get("side", "")

                        # 判断是否为对手盘
                        is_opposite = False
                        if h1_handi == 0.0 and h2_handi == 0.0 and h1_side != h2_side:
                            is_opposite = True
                        elif abs(h1_handi) == abs(h2_handi) and h1_handi * h2_handi < 0 and h1_side != h2_side:
                            is_opposite = True

                        if is_opposite:
                            opposite_pairs.append((h1, h2))
                            used.add(i)
                            used.add(j)
                            break

                # 处理对手盘对（保留差值更大的）
                for h1, h2 in opposite_pairs:
                    h1_diff = abs(h1.get("difference", 0.0))
                    h2_diff = abs(h2.get("difference", 0.0))

                    if h1_diff > h2_diff:
                        final_results[mode].append(h1)
                        print(
                            f"❌ 对手盘过滤 [{mode}]：{match_key} 过滤{h2.get('handicap')}/{h2.get('side')}（差值{h2_diff}<{h1_diff}）")
                    else:
                        final_results[mode].append(h2)
                        print(
                            f"❌ 对手盘过滤 [{mode}]：{match_key} 过滤{h1.get('handicap')}/{h1.get('side')}（差值{h1_diff}<{h2_diff}）")

                # 保留非对手盘项
                for idx, handicap in enumerate(handicaps):
                    if idx not in used:
                        final_results[mode].append(handicap)

        return final_results

    # ---------------------- 核心计算逻辑 ----------------------
    def high_frequency_calculation(self):
        """高频计算主逻辑（所有模式对齐min逻辑）"""
        # 1. 获取高频API数据
        success, matches, message = self.fetch_data_from_high_freq_api()
        if not success:
            print(f"❌ 高频计算失败：{message}")
            return

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n========== 高频计算 [{current_time}] ==========")
        print(f"📥 高频API数据：{message}")

        # 2. 筛选赛前5分钟内/已开赛的比赛
        filtered_matches = []
        for match in matches:
            start_time = match.get('start_time_beijing', '')
            time_remaining_str = self.get_time_remaining(start_time)
            time_remaining_min = self.parse_remaining_time(time_remaining_str)

            if time_remaining_min is not None and time_remaining_min <= 5 or time_remaining_str == "已开赛":
                filtered_matches.append(match)
            else:
                print(
                    f"⏰ 时间过滤：{match.get('home_team')} vs {match.get('away_team')}（剩余{time_remaining_str}，超过5分钟）")

        print(f"📊 时间过滤后：{len(filtered_matches)}场比赛（赛前5分钟/已开赛）")

        # 3. 计算基础结果（source2>source1）
        base_results = []
        for match in filtered_matches:
            home = match.get('home_team', '未知主队')
            away = match.get('away_team', '未知客队')
            league = match.get('league_name', '未知联赛')
            start_time = match.get('start_time_beijing', '')
            time_remaining = self.get_time_remaining(start_time)

            print(f"\n🔍 处理比赛：{home} vs {away}（剩余{time_remaining}）")

            # 解析开赛时间
            try:
                start_time_date = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=self.beijing_tz)
            except ValueError:
                print(f"❌ 时间解析失败：{home} vs {away}，跳过")
                continue

            # 筛选赛前5分钟内的赔率
            odds_time_threshold = start_time_date - timedelta(minutes=5)
            raw_odds = self.find_odds_list(match)
            filtered_odds = [o for o in raw_odds if datetime.strptime(o.get('time'), "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=self.beijing_tz) >= odds_time_threshold]

            if not filtered_odds:
                print(f"⏰ 赔率时间过滤：{home} vs {away} 无赛前5分钟内赔率，跳过")
                continue

            print(f"📊 赔率数量：{len(filtered_odds)}条赛前5分钟内赔率")

            # 按盘口+side分组
            groups = {}
            for odds in filtered_odds:
                key = f"{odds.get('handicap')}||{odds.get('side')}"
                if key not in groups:
                    groups[key] = {'handicap': odds.get('handicap'), 'side': odds.get('side'),
                                   'sources': {'1': [], '2': []}}
                source = odds.get('source')
                if source in ['1', '2']:
                    groups[key]['sources'][source].append(odds.get('price'))

            # 计算均值并筛选source2>source1的盘口
            for group_data in groups.values():
                src1_prices = group_data['sources']['1']
                src2_prices = group_data['sources']['2']

                if not (src1_prices and src2_prices):
                    print(f"❌ 数据源不全：{group_data['handicap']}/{group_data['side']}，跳过")
                    continue

                # 计算平均概率
                src1_probs = [self.malay_to_probability(p) for p in src1_prices if
                              self.malay_to_probability(p) is not None]
                src2_probs = [self.malay_to_probability(p) for p in src2_prices if
                              self.malay_to_probability(p) is not None]

                if not src1_probs or not src2_probs:
                    print(f"❌ 概率计算失败：{group_data['handicap']}/{group_data['side']}，跳过")
                    continue

                # 计算平均赔率
                src1_avg_prob = sum(src1_probs) / len(src1_probs)
                src2_avg_prob = sum(src2_probs) / len(src2_probs)
                src1_avg_decimal = 1 / src1_avg_prob if src1_avg_prob != 0 else None
                src2_avg_decimal = 1 / src2_avg_prob if src2_avg_prob != 0 else None

                if src1_avg_decimal is None or src2_avg_decimal is None:
                    continue

                # 仅保留source2>source1的盘口
                if src2_avg_decimal > src1_avg_decimal:
                    result_item = {
                        'home_team': home,
                        'away_team': away,
                        'league': league,
                        'time_remaining': time_remaining,
                        'handicap': group_data['handicap'],
                        'side': group_data['side'],
                        'src1_avg_decimal': round(src1_avg_decimal, 4),
                        'src1_avg_malay': round(self.probability_to_malay(src1_avg_prob),
                                                4) if src1_avg_prob != 0 else None,
                        'src2_avg_decimal': round(src2_avg_decimal, 4),
                        'src2_avg_malay': round(self.probability_to_malay(src2_avg_prob),
                                                4) if src2_avg_prob != 0 else None,
                        'difference': round(src2_avg_decimal - src1_avg_decimal, 4)
                    }
                    base_results.append(result_item)
                    print(
                        f"✅ 保留盘口：{group_data['handicap']}/{group_data['side']} → 差值：{result_item['difference']}")

        # 4. 应用模式专属盘口过滤
        results = {
            "min": base_results.copy(),
            "high": self.filter_high_mode_handicap(base_results.copy()),
            "low": self.filter_low_mode_handicap(base_results.copy())
        }

        # 5. source2盘口过滤
        filtered_results = self.filter_by_source2_spreads(results)

        # 6. 对手盘过滤
        filtered_results = self.filter_opposite_handicaps(filtered_results)

        # 7. 更新结果
        self.latest_results.update({
            "high": filtered_results["high"],
            "low": filtered_results["low"],
            "min": filtered_results["min"],
            "calculation_time": current_time
        })

        # 8. 检查并发送数据
        self.check_and_send_eligible_items()

        # 9. 最终统计
        print(
            f"\n📊 最终结果统计：High={len(filtered_results['high'])} | Low={len(filtered_results['low'])} | Min={len(filtered_results['min'])}")

    # ---------------------- 循环控制（核心修改） ----------------------
    def start_calculation_loop(self):
        """启动计算循环（先启动WebSocket）"""
        if self.calculation_active:
            return "计算已在运行中"

        if self.calculation_thread and self.calculation_thread.is_alive():
            self.calculation_thread.join()

        # 核心修改：启动计算前先启动WebSocket
        self.ws_client_thread = threading.Thread(target=self.start_ws_client, daemon=True)
        self.ws_client_thread.start()
        # 短暂等待WebSocket线程启动
        time.sleep(1)

        self.calculation_active = True
        self.calculation_thread = threading.Thread(target=self._calculation_loop, daemon=True)
        self.calculation_thread.start()
        return "✅ 计算已启动（后台运行），WebSocket已连接"

    def _calculation_loop(self):
        """计算循环主体"""
        print(f"🔄 高频计算程序启动，检查间隔：{self.CHECK_INTERVAL}秒")
        while self.calculation_active:
            try:
                self.high_frequency_calculation()
            except Exception as e:
                print(f"❌ 计算出错：{str(e)}")

            # 分段sleep，支持快速停止
            for _ in range(self.CHECK_INTERVAL):
                if not self.calculation_active:
                    break
                time.sleep(1)

        print("🛑 计算已停止")

    def stop_calculation_loop(self):
        """停止计算循环（后停止WebSocket）"""
        if not self.calculation_active:
            return "计算已停止"

        self.calculation_active = False
        # 等待计算线程结束
        if self.calculation_thread and self.calculation_thread.is_alive():
            self.calculation_thread.join(timeout=5)

        # 核心修改：停止计算后关闭WebSocket
        self.stop_ws_client()
        return "🛑 计算已停止，WebSocket已关闭"

    # ---------------------- 状态/配置管理 ----------------------
    def get_status(self):
        """获取当前状态（新增各模式发送记录统计）"""
        # 统计各模式发送记录数
        sent_stats = {}
        for mode in ["high", "low", "min"]:
            total_sent = sum([v["sent_count"] for v in self.match_sent_records[mode].values()])
            sent_stats[mode] = {
                "match_count": len(self.match_sent_records[mode]),
                "total_sent": total_sent
            }

        return {
            "running": self.calculation_active,
            "websocket_running": self.ws_running,  # 新增WebSocket状态
            "websocket_connected": self.old_program_connected,  # WebSocket连接状态
            "last_calculation_time": self.latest_results["calculation_time"],
            "high_count": len(self.latest_results["high"]),
            "low_count": len(self.latest_results["low"]),
            "min_count": len(self.latest_results["min"]),
            "send_config": {
                "threshold_minutes": self.SEND_MIN_THRESHOLD_MINUTES,
                "high_enabled": self.SEND_HIGH_ENABLED,
                "low_enabled": self.SEND_LOW_ENABLED,
                "min_enabled": self.SEND_MIN_ENABLED,
                "sent_count": len(self.sent_items),
                "sent_stats_by_mode": sent_stats  # 新增模式发送统计
            },
            "old_program_connection": {
                "connected": self.old_program_connected,
                "url": self.OLD_PROGRAM_WS_URL,
                "last_cache_update": self.old_data_cache.get("cache_update_time")
            }
        }

    def set_send_config(self, high_enabled: Optional[bool] = None,
                        low_enabled: Optional[bool] = None,
                        min_enabled: Optional[bool] = None) -> Dict[str, Any]:
        """更新发送配置（仅开关，阈值固定0分钟）"""
        if high_enabled is not None:
            self.SEND_HIGH_ENABLED = high_enabled
        if low_enabled is not None:
            self.SEND_LOW_ENABLED = low_enabled
        if min_enabled is not None:
            self.SEND_MIN_ENABLED = min_enabled

        return {
            "threshold_minutes": self.SEND_MIN_THRESHOLD_MINUTES,
            "high_enabled": self.SEND_HIGH_ENABLED,
            "low_enabled": self.SEND_LOW_ENABLED,
            "min_enabled": self.SEND_MIN_ENABLED
        }


# ---------------------- API接口 ----------------------
@app.route('/start', methods=['GET'])
def start_calculation():
    """启动计算"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    result = calculator_instance.start_calculation_loop()
    return jsonify({"status": "success", "message": result})


@app.route('/stop', methods=['GET'])
def stop_calculation():
    """停止计算"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    result = calculator_instance.stop_calculation_loop()
    return jsonify({"status": "success", "message": result})


@app.route('/status', methods=['GET'])
def get_calculation_status():
    """获取状态（包含各模式发送统计）"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    status = calculator_instance.get_status()
    return jsonify({"status": "success", "data": status})


@app.route('/results', methods=['GET'])
def get_results():
    """获取计算结果"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    return jsonify({
        "status": "success",
        "calculation_time": calculator_instance.latest_results["calculation_time"],
        "high": calculator_instance.latest_results["high"],
        "low": calculator_instance.latest_results["low"],
        "min": calculator_instance.latest_results["min"],
        "counts": {
            "high": len(calculator_instance.latest_results["high"]),
            "low": len(calculator_instance.latest_results["low"]),
            "min": len(calculator_instance.latest_results["min"]),
            "sent_total": len(calculator_instance.sent_items)
        }
    })


@app.route('/send-config', methods=['GET'])
def get_send_config():
    """获取发送配置"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    return jsonify({
        "status": "success",
        "data": {
            "threshold_minutes": calculator_instance.SEND_MIN_THRESHOLD_MINUTES,
            "high_enabled": calculator_instance.SEND_HIGH_ENABLED,
            "low_enabled": calculator_instance.SEND_LOW_ENABLED,
            "min_enabled": calculator_instance.SEND_MIN_ENABLED
        },
        "note": "所有模式阈值固定为0分钟，不可修改"
    })


@app.route('/send-config', methods=['POST'])
def update_send_config():
    """更新发送配置（仅开关）"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    try:
        data = request.json
        high_enabled = data.get('high_enabled')
        low_enabled = data.get('low_enabled')
        min_enabled = data.get('min_enabled')

        # 类型转换
        if high_enabled is not None:
            high_enabled = bool(high_enabled)
        if low_enabled is not None:
            low_enabled = bool(low_enabled)
        if min_enabled is not None:
            min_enabled = bool(min_enabled)

        config = calculator_instance.set_send_config(high_enabled, low_enabled, min_enabled)

        return jsonify({
            "status": "success",
            "message": "发送配置已更新（阈值固定0分钟）",
            "data": config
        })
    except Exception as e:
        return jsonify({"status": "error", "message": f"更新失败：{str(e)}"}), 400


@app.route('/old-program-data', methods=['GET'])
def get_old_program_data():
    """获取旧程序数据"""
    if not calculator_instance:
        return jsonify({"status": "error", "message": "计算器未初始化"}), 500

    return jsonify({
        "status": "success",
        "connected": calculator_instance.old_program_connected,
        "websocket_running": calculator_instance.ws_running,  # 新增WebSocket运行状态
        "cache_update_time": calculator_instance.old_data_cache.get("cache_update_time"),
        "data": calculator_instance.old_data_cache
    })


# ---------------------- 启动函数 ----------------------
def run_api_server():
    """运行API服务器"""
    app.run(host='0.0.0.0', port=5010, debug=False, use_reloader=False)


if __name__ == "__main__":
    calculator_instance = HighFrequencyOddsCalculator()

    # 启动API服务线程
    api_thread = threading.Thread(target=run_api_server, daemon=True)
    api_thread.start()

    # 启动提示
    print("=" * 50)
    print("🎯 高频赔率计算器已启动（未开始计算）")
    print("🌐 API服务地址：http://localhost:5010")
    print("📋 可用接口：")
    print("  GET  /start          - 开始计算（同时启动WebSocket）")
    print("  GET  /stop           - 停止计算（同时关闭WebSocket）")
    print("  GET  /status         - 查看状态（含WebSocket状态）")
    print("  GET  /results        - 获取计算结果")
    print("  GET  /send-config    - 获取发送配置")
    print("  POST /send-config    - 更新发送开关（JSON）")
    print("  GET  /old-program-data - 获取旧程序数据（含WebSocket状态）")
    print("=" * 50)
    print("💡 提示：按Ctrl+C可退出程序")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 收到退出信号，正在停止程序...")
        if calculator_instance:
            calculator_instance.stop_calculation_loop()
        print("✅ 程序已安全退出")