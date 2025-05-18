import json
import threading
import time

import requests
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from bs4 import BeautifulSoup
import re

app = Flask(__name__)

ACCOUNTS = [
    {'username': 'caafbb33', 'password': 'dddd1111DD'},
]
BASE_URL = 'https://125.252.69.206/'

MARKET_TYPES = {
    'HDP_OU': 'tab_rnou',
}

driver = None  # 全局浏览器对象


def init_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    d = webdriver.Chrome(options=chrome_options)
    # 隐藏webdriver属性以防被检测
    d.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
        '''
    })
    return d


def login(d, username, password):
    print("Starting login")
    d.get(BASE_URL)
    wait = WebDriverWait(d, 150)
    try:
        lang_field = wait.until(EC.visibility_of_element_located((By.ID, 'lang_en')))
        lang_field.click()
        print("已选择语言")

        username_field = wait.until(EC.visibility_of_element_located((By.ID, 'usr')))
        password_field = wait.until(EC.visibility_of_element_located((By.ID, 'pwd')))
        username_field.clear()
        username_field.send_keys(username)
        password_field.clear()
        password_field.send_keys(password)

        login_button = wait.until(EC.element_to_be_clickable((By.ID, 'btn_login')))
        login_button.click()
        print("已点击登录按钮")

        try:
            popup_wait = WebDriverWait(d, 10)
            no_button = popup_wait.until(EC.element_to_be_clickable((By.ID, 'C_no_btn')))
            no_button.click()
            print("已点击弹窗")
        except:
            pass
        wait.until(EC.visibility_of_element_located((By.ID, 'today_page')))
        print(f"{username} 登录成功")
        return True
    except Exception as e:
        print(f"{username} 登录失败 or 未找到比赛: {e}")
        return False


def navigate_to_football(d):
    wait = WebDriverWait(d, 150)
    try:
        football_btn = wait.until(EC.element_to_be_clickable((By.ID, 'today_page')))
        football_btn.click()
        wait.until(EC.visibility_of_element_located((By.ID, 'div_show')))
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'btn_title_le')))
        print("导航到足球页面成功")
        return True
    except Exception as e:
        print(f"导航到足球页面失败: {e}")
        return False


def get_market_data(d):
    try:
        page_source = d.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        return soup
    except Exception as e:
        print(f"获取页面数据失败: {e}")
        return None


def parse_market_data(soup, market_type):
    data = []
    league_sections = soup.find_all('div', class_='btn_title_le')
    for league_section in league_sections:
        league_name_tag = league_section.find('tt', id='lea_name')
        league_name = league_name_tag.get_text(strip=True) if league_name_tag else 'Unknown League'
        match_container = league_section.find_next_sibling()
        while match_container and 'box_lebet' in match_container.get('class', []):
            info = extract_match_info(match_container, league_name, market_type)
            if info:
                data.append(info)
            match_container = match_container.find_next_sibling()
    return data


def extract_match_info(match_container, league_name, market_type):
    try:
        home_div = match_container.find('div', class_='box_team teamH')
        away_div = match_container.find('div', class_='box_team teamC')
        home_team = (home_div.find('span', class_='text_team').get_text(strip=True)
                     if home_div and home_div.find('span', class_='text_team') else 'Unknown')
        away_team = (away_div.find('span', class_='text_team').get_text(strip=True)
                     if away_div and away_div.find('span', class_='text_team') else 'Unknown')

        match_time_tag = match_container.find('tt', class_='text_time')
        match_time = match_time_tag.find('i', id='icon_info').get_text(strip=True) if match_time_tag else 'Unknown Time'

        match_info = {
            'league': league_name,
            'match_time': match_time,
            'home_team': home_team,  # 主客队名称提升到顶级字段
            'away_team': away_team,  # 主客队名称提升到顶级字段
            'odds': {'handicap': {}, 'total_points': {}}  # 初始化赔率结构
        }

        if market_type == 'HDP_OU':
            ft_sections = match_container.find_all('div', class_='form_lebet_hdpou')
            for sec in ft_sections:
                bet_type_tag = sec.find('div', class_='head_lebet').find('span')
                bet_type = bet_type_tag.get_text(strip=True) if bet_type_tag else ''
                if bet_type in ['Handicap', 'Goals O/U']:
                    # 合并不同盘口类型的赔率
                    odds_data = extract_odds_hdp_ou(sec, bet_type)
                    if bet_type == 'Handicap':
                        match_info['odds']['handicap'].update(odds_data['handicap'])
                    elif bet_type == 'Goals O/U':
                        match_info['odds']['total_points'].update(odds_data['total_points'])

        return match_info
    except Exception as e:
        print(f"提取比赛信息失败: {e}")
        return None


def extract_odds_hdp_ou(odds_section, bet_type):
    odds = {'handicap': {}, 'total_points': {}}

    for col in odds_section.find_all('div', class_='col_hdpou'):
        bet_items = col.find_all('div', class_='btn_hdpou_odd')
        if len(bet_items) != 2:
            continue

        if bet_type == 'Handicap':
            current_odds = odds['handicap']
            for idx, item in enumerate(bet_items):
                handicap_tag = item.find('tt', class_='text_ballhead')
                odds_tag = item.find('span', class_='text_odds')
                if not handicap_tag or not odds_tag:
                    continue

                cleaned_handicap = clean_handicap(handicap_tag.get_text(strip=True))
                cleaned_odds = clean_odds_value(odds_tag.get_text(strip=True))
                if not cleaned_handicap or not cleaned_odds:
                    continue

                current_odds.setdefault(cleaned_handicap, {})
                current_odds[cleaned_handicap]['home' if idx == 0 else 'away'] = cleaned_odds

        elif bet_type == 'Goals O/U':
            current_odds = odds['total_points']
            for item in bet_items:
                team_info = item.find('tt', class_='text_ballou')
                handicap_tag = item.find('tt', class_='text_ballhead')
                odds_tag = item.find('span', class_='text_odds')
                if not all([team_info, handicap_tag, odds_tag]):
                    continue

                cleaned_handicap = clean_handicap(handicap_tag.get_text(strip=True))
                cleaned_odds = clean_odds_value(odds_tag.get_text(strip=True))
                over_under = 'over' if team_info.get_text() == 'O' else 'under'

                if not cleaned_handicap or not cleaned_odds:
                    continue

                current_odds.setdefault(cleaned_handicap, {})
                current_odds[cleaned_handicap][over_under] = cleaned_odds

    return odds


def clean_odds_value(value):
    """清洗赔率值，移除无效符号并处理特殊情况"""
    cleaned = value.strip().strip('*')
    if not cleaned or any(c not in '0123456789./-+' for c in cleaned):
        return None  # 无效数据
    return cleaned


def clean_handicap(handicap):
    """清洗盘口值，仅保留有效格式"""
    cleaned = handicap.strip().strip('*')
    if not re.match(r'^[+-]?(\d+(\.\d+)?|(\d+(\.\d+)?)/(\d+(\.\d+)?))$', cleaned):
        return None  # 无效盘口
    return cleaned if cleaned != '0' else '0'


def remove_plus_signs_from_handicap(data):
    """
    处理赔率数据，移除盘口值中的加号(+)，但保留负号(-)和其他内容不变。

    参数:
    data (list): 包含比赛信息和赔率的列表，每个元素是一个字典

    返回:
    list: 处理后的赔率数据，盘口值中的加号被移除
    """
    processed_data = []

    for match in data:
        # 复制原始数据，避免修改原对象
        processed_match = match.copy()
        processed_odds = processed_match.get('odds', {})

        # 处理让球盘(handicap)数据
        if 'handicap' in processed_odds:
            processed_handicap = {}
            for handicap, odds in processed_odds['handicap'].items():
                # 仅移除盘口值开头的加号，保留其他位置的符号
                new_handicap = handicap.lstrip('+') if isinstance(handicap, str) else handicap
                processed_handicap[new_handicap] = odds
            processed_odds['handicap'] = processed_handicap

        # 处理大小球(total_points)数据（如果需要的话）
        if 'total_points' in processed_odds:
            processed_total = {}
            for total, odds in processed_odds['total_points'].items():
                # 同样处理大小球盘口（如果存在加号的话）
                new_total = total.lstrip('+') if isinstance(total, str) else total
                processed_total[new_total] = odds
            processed_odds['total_points'] = processed_total

        processed_match['odds'] = processed_odds
        processed_data.append(processed_match)

    return processed_data


def convert_fraction_handicap_to_decimal(data):
    """
    将赔率数据中的分数形式盘口值（如 `-0/0.5`）转换为小数形式（如 `-0.25`），使用预定义的替换表。

    参数:
    data (list): 包含比赛信息和赔率的列表，每个元素是一个字典

    返回:
    list: 处理后的赔率数据，分数盘口被转换为小数形式
    """
    # 分数形式到小数形式的完整映射表（覆盖常见盘口）
    FRACTION_TO_DECIMAL_MAPPING = {
        # 负向分数盘口（让分盘）
        '-0/0.5': '-0.25',  # 平/半（主队让0.25球）
        '-0.5/1': '-0.75',  # 半/一（主队让0.75球）
        '-1/1.5': '-1.25',  # 一/球半（主队让1.25球）
        '-1.5/2': '-1.75',  # 球半/两球（主队让1.75球）
        '-2/2.5': '-2.25',  # 两球/两球半（主队让2.25球）
        '-2.5/3': '-2.75',  # 两球半/三球（主队让2.75球）
        '-3/3.5': '-3.25',  # 三球/三球半（主队让3.25球）
        '-3.5/4': '-3.75',  # 三球半/四球（主队让3.75球）
        '-4/4.5': '-4.25',  # 四球/四球半（主队让4.25球）
        '-4.5/5': '-4.75',  # 四球半/五球（主队让4.75球）

        # 正向分数盘口（受让盘，客队让球）
        '0/0.5': '0.25',  # 平/半（客队让0.25球，即主队受让0.25球）
        '0.5/1': '0.75',  # 半/一（客队让0.75球）
        '1/1.5': '1.25',  # 一/球半（客队让1.25球）
        '1.5/2': '1.75',  # 球半/两球（客队让1.75球）
        '2/2.5': '2.25',  # 两球/两球半（客队让2.25球）
        '2.5/3': '2.75',  # 两球半/三球（客队让2.75球）
        '3/3.5': '3.25',  # 三球/三球半（客队让3.25球）
        '3.5/4': '3.75',  # 三球半/四球（客队让3.75球）
        '4/4.5': '4.25',  # 四球/四球半（客队让4.25球）
        '4.5/5': '4.75',  # 四球半/五球（客队让4.75球）

        # 特殊固定值（无需转换，直接映射以确保兼容性）
        '0': '0',  # 平手盘
        '0.5': '0.5',  # 半球盘
        '1': '1',  # 一球盘
        '1.5': '1.5',  # 球半盘
        '2': '2',  # 两球盘
        '2.5': '2.5',  # 两球半盘
        '3': '3',  # 三球盘
        '3.5': '3.5',  # 三球半盘
        '4': '4',  # 四球盘
        '4.5': '4.5',  # 四球半盘
        '5': '5',  # 五球盘
        '-0.5': '-0.5',  # 受让半球（客队让0.5球）
        '-1': '-1',  # 受让一球（客队让1球）
        '-1.5': '-1.5',  # 受让球半（客队让1.5球）
        '-2': '-2',  # 受让两球（客队让2球）
        '-2.5': '-2.5',  # 受让两球半（客队让2.5球）
        '-3': '-3',  # 受让三球（客队让3球）
        '-3.5': '-3.5',  # 受让三球半（客队让3.5球）
        '-4': '-4',  # 受让四球（客队让4球）
        '-4.5': '-4.5',  # 受让四球半（客队让4.5球）
        '-5': '-5',  # 受让五球（客队让5球）
    }

    processed_data = []

    for match in data:
        processed_match = match.copy()
        processed_odds = processed_match.get('odds', {})

        # 处理让球盘(handicap)
        if 'handicap' in processed_odds:
            processed_handicap = {}
            for handicap, odds in processed_odds['handicap'].items():
                # 从映射表获取对应的小数值，不存在则保留原值
                new_handicap = FRACTION_TO_DECIMAL_MAPPING.get(handicap, handicap)
                processed_handicap[new_handicap] = odds
            processed_odds['handicap'] = processed_handicap

        # 处理大小球(total_points，逻辑相同，因大小球盘口可能包含分数形式，如1.5/2需转换为1.75）
        if 'total_points' in processed_odds:
            processed_total = {}
            for total, odds in processed_odds['total_points'].items():
                new_total = FRACTION_TO_DECIMAL_MAPPING.get(total, total)
                processed_total[new_total] = odds
            processed_odds['total_points'] = processed_total

        processed_match['odds'] = processed_odds
        processed_data.append(processed_match)

    return processed_data


def rename_fields(data):
    """
    重命名赔率数据中的特定字段名称，保持数据结构和内容不变。

    参数:
    data (list): 包含比赛信息和赔率的列表

    返回:
    list: 处理后的赔率数据，字段名已修改
    """
    processed_data = []

    for match in data:
        # 复制原始数据，避免修改原对象
        processed_match = match.copy()

        # 重命名 league 为 league_name
        if 'league' in processed_match:
            processed_match['league_name'] = processed_match.pop('league')

        # 处理赔率数据
        if 'odds' in processed_match:
            processed_odds = processed_match['odds'].copy()

            # 重命名 handicap 为 spreads
            if 'handicap' in processed_odds:
                processed_odds['spreads'] = processed_odds.pop('handicap')

            # 重命名 total_points 为 totals
            if 'total_points' in processed_odds:
                processed_odds['totals'] = processed_odds.pop('total_points')

            processed_match['odds'] = processed_odds

        processed_data.append(processed_match)

    return processed_data

@app.route("/get_odds2", methods=["GET"])
def get_odds():
    soup = get_market_data(driver)
    if soup:
        data_list = parse_market_data(soup, 'HDP_OU')
        data_list = remove_plus_signs_from_handicap(data_list)
        processed_data = convert_fraction_handicap_to_decimal(data_list)
        processed_data = rename_fields(processed_data)  # 此时数据格式与1网一致

        # ===================== 新增：Java数据验证逻辑 =====================
        # 1. 整理待验证的赛事数据（转换为Java接口需要的格式）
        matches_to_check = []
        for match in processed_data:
            matches_to_check.append({
                "league": match["league_name"],
                "homeTeam": match["home_team"],
                "awayTeam": match["away_team"]
            })

        # 2. 发送验证请求（2网固定source=2）
        JAVA_CHECK_API = "http://160.25.20.18:8080/api/bindings/check-existing"  # 假设Java接口地址不变
        payload = {
            "source": 2,  # 2网对应source2
            "matches": matches_to_check
        }

        try:
            response = requests.post(JAVA_CHECK_API, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()
            existing_leagues = result.get("leagues", [])
            existing_teams = result.get("teams", [])
        except Exception as e:
            print(f"Java验证接口调用失败: {e}")
            return jsonify({"error": "数据验证失败"}), 500

        # 3. 过滤出数据库中存在的赛事
        valid_data = []
        for match in processed_data:
            league = match["league_name"]
            home_team = match["home_team"]
            away_team = match["away_team"]
            # 必须同时存在联赛和主客队（与1网逻辑一致）
            if league in existing_leagues and home_team in existing_teams and away_team in existing_teams:
                valid_data.append(match)

        if not valid_data:
            return jsonify({"message": "未找到数据库中存在的赛事"}), 404

        return jsonify(valid_data)  # 返回验证后的有效数据
        # ===================== 原有代码删除，替换为上述逻辑 =====================
    else:
        return jsonify({"error": "未获取到数据"}), 500


# ====================== 弹窗处理模块 ======================
def handle_any_popup(driver):
    """通用弹窗处理函数，检测所有可能的弹窗类型并关闭"""
    try:
        wait = WebDriverWait(driver, 2, poll_frequency=0.5)  # 快速检测

        # 定义弹窗处理规则（按检测优先级排序）
        popup_rules = [
            # 1. 踢人登出弹窗（最高优先级，需重新登录）
            {
                'detector': (By.ID, "alert_kick"),
                'closer': (By.ID, "kick_ok_btn"),
                're_login': True  # 标记需要重新登录
            },
            # 2. 系统超时弹窗
            {
                'detector': (By.ID, "system_show"),
                'closer': (By.ID, "C_ok_btn_system")
            },
            # 3. 双按钮确认弹窗（Yes/No）
            {
                'detector': (By.ID, "alert_confirm"),
                'closer': (By.ID, "yes_btn")
            },
            # 4. C类型双按钮弹窗（C_Yes/C_No）
            {
                'detector': (By.ID, "C_alert_confirm"),
                'closer': (By.ID, "C_yes_btn")
            },
            # 5. 单按钮OK弹窗
            {
                'detector': (By.ID, "alert_ok"),
                'closer': (By.ID, "ok_btn")
            },
            # 6. C类型单按钮弹窗
            {
                'detector': (By.ID, "C_alert_ok"),
                'closer': (By.ID, "C_ok_btn")
            },
            # 7. 信息弹窗（带关闭按钮）
            {
                'detector': (By.ID, "info_pop"),
                'closer': (By.ID, "info_close")
            },
            # 8. 吐司消息弹窗
            {
                'detector': (By.ID, "message_pop"),
                'closer': (By.ID, "message_ok")
            }
        ]

        for rule in popup_rules:
            detector_by, detector_val = rule['detector']
            closer_by, closer_val = rule['closer']

            try:
                # 检测弹窗是否存在
                wait.until(EC.presence_of_element_located((detector_by, detector_val)))
                # 点击关闭按钮
                close_btn = wait.until(EC.element_to_be_clickable((closer_by, closer_val)))
                close_btn.click()
                print(f"已关闭 {detector_val} 类型弹窗")

                # 处理登出后的重新登录
                if rule.get('re_login', False):
                    print("检测到登出，正在重新登录...")
                    acc = ACCOUNTS[0]
                    driver.get(BASE_URL)
                    login(driver, acc['username'], acc['password'])
                    navigate_to_football(driver)
                return  # 处理一个弹窗后立即返回
            except:
                continue  # 继续检查下一个弹窗类型

    except Exception as e:
        print(f"弹窗处理异常: {str(e)[:50]}")


def start_popup_monitor(driver):
    """后台线程持续监控弹窗（每5秒检查一次）"""

    def monitor_loop():
        while True:
            try:
                handle_any_popup(driver)
                time.sleep(5)  # 控制检测频率
            except Exception as e:
                print(f"监控线程异常: {str(e)[:50]}")
                time.sleep(5)

    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    print("弹窗监控线程已启动（每5秒检查一次）")


if __name__ == "__main__":
    driver = init_driver()
    acc = ACCOUNTS[0]

    if not login(driver, acc['username'], acc['password']):
        driver.quit()
        raise Exception("登录失败")

    if not navigate_to_football(driver):
        driver.quit()
        raise Exception("导航失败")

    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, MARKET_TYPES['HDP_OU']))
        )
        button.click()
        print("已点击 HDP_OU 按钮")
    except Exception as e:
        print("找不到 HDP_OU 按钮或点击失败", e)

    # 启动弹窗监控线程
    start_popup_monitor(driver)

    # 运行Flask服务
    app.run(host="0.0.0.0", port=5002, debug=False)

    # 程序结束时关闭浏览器（实际中可能需要添加关闭逻辑）
    driver.quit()
