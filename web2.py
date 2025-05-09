import json
import threading
import time
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
    # chrome_options.add_argument('--headless')
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


@app.route("/get_odds", methods=["GET"])
def get_odds():
    soup = get_market_data(driver)
    if soup:
        data_list = parse_market_data(soup, 'HDP_OU')
        return jsonify(data_list)
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
    app.run(host="0.0.0.0", port=5001, debug=False)

    # 程序结束时关闭浏览器（实际中可能需要添加关闭逻辑）
    driver.quit()