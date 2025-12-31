import json
import random
import threading
import time
import os
import sys
import requests
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

app = Flask(__name__)

# 配置参数
ACCOUNTS = [
    {'username': 'czzvaa00', 'password': 'dddd1111DD'},
]
BASE_URL = 'https://205.201.0.120/'
MARKET_TYPES = {
    'HDP_OU': 'tab_rnou',
}

# 新增配置：连续空数据阈值
MAX_EMPTY_COUNT = 5  # 连续5次抓取数据为空则停止
EMPTY_COUNT = 0  # 空数据计数器

# 全局变量
driver = None  # 全局浏览器对象
CACHE_DATA = None  # 全局缓存数据
CACHE_LOCK = threading.Lock()  # 缓存锁
ERROR_COUNT = 0  # 错误计数器
MAX_ERRORS = 5  # 最大连续错误数
RESTART_DELAY = 5  # 重启延迟（秒）
IS_RUNNING = True  # 控制抓取线程的运行状态
# 场景2最大重启次数配置
MAX_RESTART_ATTEMPTS = 3  # 场景2最多重启3次
RESTART_ATTEMPT_COUNT = 0  # 场景2重启计数器

# 每日定时任务最大启动尝试次数
DAILY_START_MAX_ATTEMPTS = 3  # 每日任务最多尝试3次启动
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]


def init_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 启用无头模式
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument(f'--user-data-dir={os.path.join(os.getcwd(), "temp_chrome_data")}')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # 防止被检测为自动化工具
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
        '''
    })
    print("WebDriver 初始化完成")
    return driver


def login(d, username, password):
    """登录到目标网站"""
    print(f"尝试登录: {username}")
    d.get(BASE_URL)
    time.sleep(random.uniform(2, 4))  # 随机延迟2-4秒
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
    """导航到足球页面"""
    try:
        # 尝试关闭系统消息弹窗
        ok_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, 'close_btn1'))
        )
        ok_button.click()
        time.sleep(1)
        print("系统消息弹窗已关闭")
    except TimeoutException:
        pass

    print("导航到足球页面")
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
    """获取当前页面的市场数据"""
    try:
        page_source = d.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        return soup
    except Exception as e:
        print(f"获取页面数据失败: {e}")
        return None


def parse_market_data(soup, market_type):
    """解析市场数据"""
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
    """从比赛容器中提取比赛信息"""
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
            'home_team': home_team,
            'away_team': away_team,
            'odds': {'handicap': {}, 'total_points': {}}
        }

        if market_type == 'HDP_OU':
            ft_sections = match_container.find_all('div', class_='form_lebet_hdpou')
            for sec in ft_sections:
                bet_type_tag = sec.find('div', class_='head_lebet').find('span')
                bet_type = bet_type_tag.get_text(strip=True) if bet_type_tag else ''
                if bet_type in ['Handicap', 'Goals O/U']:
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
    """提取让球盘和大小球盘的赔率数据"""
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
    import re
    cleaned = handicap.strip().strip('*')
    if not re.match(r'^[+-]?(\d+(\.\d+)?|(\d+(\.\d+)?)/(\d+(\.\d+)?))$', cleaned):
        return None  # 无效盘口
    return cleaned if cleaned != '0' else '0'


def remove_plus_signs_from_handicap(data):
    """处理赔率数据，移除盘口值中的加号(+)"""
    processed_data = []
    for match in data:
        processed_match = match.copy()
        processed_odds = processed_match.get('odds', {})

        if 'handicap' in processed_odds:
            processed_handicap = {k.lstrip('+'): v for k, v in processed_odds['handicap'].items()}
            processed_odds['handicap'] = processed_handicap

        if 'total_points' in processed_odds:
            processed_total = {k.lstrip('+'): v for k, v in processed_odds['total_points'].items()}
            processed_odds['total_points'] = processed_total

        processed_match['odds'] = processed_odds
        processed_data.append(processed_match)
    return processed_data


def convert_fraction_handicap_to_decimal(data):
    """将分数盘口转换为小数盘口"""
    FRACTION_TO_DECIMAL_MAPPING = {
        '-0/0.5': '-0.25', '-0.5/1': '-0.75', '-1/1.5': '-1.25', '-1.5/2': '-1.75',
        '-2/2.5': '-2.25', '-2.5/3': '-2.75', '-3/3.5': '-3.25', '-3.5/4': '-3.75',
        '-4/4.5': '-4.25', '-4.5/5': '-4.75', '0/0.5': '0.25', '0.5/1': '0.75',
        '1/1.5': '1.25', '1.5/2': '1.75', '2/2.5': '2.25', '2.5/3': '2.75',
        '3/3.5': '3.25', '3.5/4': '3.75', '4/4.5': '4.25', '4.5/5': '4.75',
        '0': '0', '0.5': '0.5', '1': '1', '1.5': '1.5', '2': '2', '2.5': '2.5',
        '3': '3', '3.5': '3.5', '4': '4', '4.5': '4.5', '5': '5', '-0.5': '-0.5',
        '-1': '-1', '-1.5': '-1.5', '-2': '-2', '-2.5': '-2.5', '-3': '-3',
        '-3.5': '-3.5', '-4': '-4', '-4.5': '-4.5', '-5': '-5'
    }

    processed_data = []
    for match in data:
        processed_match = match.copy()
        processed_odds = processed_match['odds'].copy()

        for key in ['handicap', 'total_points']:
            if key in processed_odds:
                processed_odds[key] = {
                    FRACTION_TO_DECIMAL_MAPPING.get(k, k): v
                    for k, v in processed_odds[key].items()
                }

        processed_match['odds'] = processed_odds
        processed_data.append(processed_match)
    return processed_data


def rename_fields(data):
    """重命名字段"""
    processed_data = []
    for match in data:
        processed_match = {
            'league_name': match.pop('league'),
            'match_time': match['match_time'],
            'home_team': match['home_team'],
            'away_team': match['away_team'],
            'odds': {
                'spreads': match['odds'].pop('handicap', {}),
                'totals': match['odds'].pop('total_points', {})
            }
        }
        processed_data.append(processed_match)
    return processed_data


def all_odds_empty(data):
    """检查所有赔率数据是否都为空"""
    if not data:
        return True  # 如果没有比赛数据，视为赔率为空

    for match in data:
        odds = match.get('odds', {})
        spreads = odds.get('spreads', {})
        totals = odds.get('totals', {})

        # 如果有任何一个赔率不为空，则返回False
        if spreads or totals:
            return False

    # 所有赔率都为空
    return True


def stop_crawling():
    """停止抓取作业并清理资源"""
    global IS_RUNNING, driver

    print(f"连续 {MAX_EMPTY_COUNT} 次抓取数据为空，准备停止抓取作业...")
    IS_RUNNING = False  # 停止抓取线程

    # 关闭浏览器
    if driver:
        try:
            driver.quit()
            print("浏览器已关闭")
        except Exception as e:
            print(f"关闭浏览器失败: {e}")
        finally:
            driver = None

    # 清空缓存
    with CACHE_LOCK:
        global CACHE_DATA
        CACHE_DATA = None

    print("抓取作业已停止，相关工作线程已退出")


def fetch_data():
    """抓取数据并更新缓存"""
    global CACHE_DATA, ERROR_COUNT, driver, EMPTY_COUNT, RESTART_ATTEMPT_COUNT

    try:
        if not IS_RUNNING:
            return

        # 等待页面加载完成
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'btn_title_le'))
        )
        # 第二步：等赔率元素（确保比赛有赔率数据），最多等15秒
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'btn_hdpou_odd'))
            )
        except TimeoutException:
            print("抓取前未加载到赔率元素，本次抓取可能为空")
        time.sleep(2)  # 额外等待渲染

        soup = get_market_data(driver)
        if not soup:
            raise Exception("获取页面数据失败")

        data_list = parse_market_data(soup, 'HDP_OU')

        # 检查数据是否为空
        if not data_list:
            EMPTY_COUNT += 1
            print(f"解析数据为空，连续空数据计数: {EMPTY_COUNT}/{MAX_EMPTY_COUNT}")

            # 如果达到连续空数据阈值，停止抓取
            if EMPTY_COUNT >= MAX_EMPTY_COUNT:
                stop_crawling()
                return
        else:
            EMPTY_COUNT = 0  # 重置空数据计数器

        processed_data = remove_plus_signs_from_handicap(data_list)
        processed_data = convert_fraction_handicap_to_decimal(processed_data)
        valid_data = rename_fields(processed_data)

        if not valid_data:
            raise Exception("未找到有效赛事数据")

        # 新增检查：如果所有赔率数据都为空，则触发重启
        if all_odds_empty(valid_data):
            ERROR_COUNT += 1
            print(f"所有赔率数据为空，错误计数: {ERROR_COUNT}/{MAX_ERRORS}")
            if ERROR_COUNT >= MAX_ERRORS and IS_RUNNING:
                print("所有赔率数据为空达到最大错误次数，触发程序重启...")
                threading.Thread(target=restart_program).start()
            return

        with CACHE_LOCK:
            CACHE_DATA = valid_data

        print(f"数据抓取成功，更新缓存: {len(valid_data)} 场比赛")
        ERROR_COUNT = 0  # 重置错误计数
        RESTART_ATTEMPT_COUNT = 0  # 重启成功，重置场景2重启计数器

    except requests.exceptions.RequestException as e:
        ERROR_COUNT += 1
        print(f"HTTP请求失败 ({ERROR_COUNT}/{MAX_ERRORS}): {str(e)}")
        if ERROR_COUNT >= MAX_ERRORS and IS_RUNNING:
            print(f"连续 {MAX_ERRORS} 次错误，触发程序重启...")
            threading.Thread(target=restart_program).start()

    except Exception as e:
        ERROR_COUNT += 1
        print(f"数据处理失败 ({ERROR_COUNT}/{MAX_ERRORS}): {str(e)}")
        if ERROR_COUNT >= MAX_ERRORS and IS_RUNNING:
            print(f"连续 {MAX_ERRORS} 次错误，触发程序重启...")
            threading.Thread(target=restart_program).start()


def restart_program():
    """软重启：重置浏览器会话（最多重启3次）"""
    global driver, ERROR_COUNT, IS_RUNNING, RESTART_ATTEMPT_COUNT
    active = is_active_hours()
    print(f"[重启判断] 当前时间活跃时段: {active}, 重启尝试次数: {RESTART_ATTEMPT_COUNT}")
    if not IS_RUNNING:
        print("抓取作业已停止，不执行重启")
        return

    # 检查场景2重启次数是否达到阈值
    RESTART_ATTEMPT_COUNT += 1
    print(f"场景2重启尝试 {RESTART_ATTEMPT_COUNT}/{MAX_RESTART_ATTEMPTS}...")

    # 只有在非活跃时段（5:00 ~ 12:05）才允许因重启失败而停止
    if not is_active_hours() and RESTART_ATTEMPT_COUNT >= MAX_RESTART_ATTEMPTS:
        print(f"非活跃时段：已连续重启{MAX_RESTART_ATTEMPTS}次失败，停止抓取作业")
        stop_crawling()
        return
    elif is_active_hours():
        # 活跃时段：即使超过 MAX_RESTART_ATTEMPTS，也不停止，继续重试
        print(f"活跃时段：重启失败第 {RESTART_ATTEMPT_COUNT} 次，继续尝试重启...")

    print("开始执行软重启...")

    # 安全关闭旧驱动
    if driver:
        try:
            driver.quit()
            time.sleep(2)  # 确保关闭完成
        except Exception as e:
            print(f"关闭浏览器驱动失败: {e}")
        finally:
            driver = None

    ERROR_COUNT = 0  # 重置错误计数
    EMPTY_COUNT = 0  # 重置空数据计数

    # 初始化新驱动
    driver = init_driver()

    # 重新登录
    acc = ACCOUNTS[0]
    if not login(driver, acc['username'], acc['password']):
        print("软重启后登录失败，程序无法继续运行")
        return

    if not navigate_to_football(driver):
        print("软重启后导航到足球页面失败，程序无法继续运行")
        return

    # 切换盘口类型
    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, MARKET_TYPES['HDP_OU']))
        )
        button.click()
        print("软重启后已点击 HDP_OU 按钮")
    except Exception as e:
        print(f"切换盘口失败: {e}")
        return

    # 新增：重启后等待赔率加载
    try:
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'btn_hdpou_odd'))
        )
        time.sleep(2)
    except TimeoutException:
        print("重启后未加载到赔率元素，需后续抓取重试")

    print("软重启完成，继续数据抓取...")


def is_active_hours():
    """
    判断当前是否处于「活跃重启时段」：12:05 到 次日 5:00（含）
    返回 True 表示应该无限重启，False 表示允许挂机。
    """
    now = datetime.now().time()
    start_time = datetime.strptime("12:05", "%H:%M").time()
    end_time = datetime.strptime("05:00", "%H:%M").time()

    # 跨天逻辑：12:05 -> 23:59:59 || 00:00 -> 05:00
    if start_time <= now or now <= end_time:
        return True
    return False

def start_scheduler():
    """启动定时抓取任务"""

    def scheduler_loop():
        while IS_RUNNING:
            if driver:  # 确保浏览器已初始化
                fetch_data()
            time.sleep(2)  # 抓取间隔

    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    print("数据抓取调度器已启动")


def schedule_daily_restart():
    """每天中午12:05启动或重启抓取作业（最多尝试3次启动）"""

    def daily_start_attempt(attempt=1):
        """每日任务启动尝试（带重试逻辑）"""
        global IS_RUNNING, driver, ERROR_COUNT, EMPTY_COUNT, RESTART_ATTEMPT_COUNT

        if attempt > DAILY_START_MAX_ATTEMPTS:
            print(f"每日任务启动尝试{DAILY_START_MAX_ATTEMPTS}次失败，放弃启动")
            return

        print(f"每日12:05任务启动尝试 {attempt}/{DAILY_START_MAX_ATTEMPTS}...")

        try:
            # 如果当前抓取已停止，重新启动
            if not IS_RUNNING or driver is None:
                print("抓取作业已停止，正在重新启动...")
                ERROR_COUNT = 0
                EMPTY_COUNT = 0
                RESTART_ATTEMPT_COUNT = 0
                IS_RUNNING = True

                # 初始化浏览器并启动抓取
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                driver = init_driver()

                acc = ACCOUNTS[0]
                if login(driver, acc['username'], acc['password']):
                    if navigate_to_football(driver):
                        try:
                            button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.ID, MARKET_TYPES['HDP_OU']))
                            )
                            button.click()
                            print("已点击 HDP_OU 按钮")
                            start_scheduler()  # 启动调度器
                            print("每日任务启动成功")
                            return
                        except Exception as e:
                            print(f"切换盘口失败: {e}")
                    else:
                        print("导航到足球页面失败")
                else:
                    print("登录失败")

            # 如果当前正在运行，执行软重启
            else:
                print("抓取作业正在运行，执行软重启...")
                restart_program()
                print("每日任务软重启完成")
                return

        except Exception as e:
            print(f"每日任务启动尝试{attempt}失败: {e}")

        # 重试延迟5秒后再次尝试
        time.sleep(5)
        daily_start_attempt(attempt + 1)

    def daily_job():
        """每日任务主逻辑"""
        print("执行每日12:05任务 - 检查并重启抓取作业")
        daily_start_attempt(1)  # 开始第一次启动尝试

        # 安排下一次任务
        schedule_next_daily_run()

    def schedule_next_daily_run():
        """计算到明天12:05的时间差，安排下一次任务"""
        now = datetime.now()
        # 计算今天12:05，如果已过则计算明天的
        target_time = now.replace(hour=12, minute=5, second=0, microsecond=0)
        if now > target_time:
            target_time += timedelta(days=1)

        # 计算时间差
        time_diff = (target_time - now).total_seconds()
        print(f"下一次每日任务将在 {time_diff:.0f} 秒后执行 (大约 {time_diff / 3600:.1f} 小时)")

        # 安排任务
        threading.Timer(time_diff, daily_job).start()

    # 立即安排第一次任务
    schedule_next_daily_run()


@app.route("/get_odds2", methods=["GET"])
def get_odds():
    """API接口：返回缓存的赔率数据"""
    with CACHE_LOCK:
        if CACHE_DATA is not None:
            return jsonify(CACHE_DATA)
        else:
            return jsonify({"error": "缓存数据为空，请稍后再试"}), 503


def handle_any_popup(driver):
    """通用弹窗处理函数"""
    try:
        if not IS_RUNNING:
            return

        wait = WebDriverWait(driver, 2, poll_frequency=0.5)
        popup_rules = [
            {'detector': (By.ID, "alert_kick"), 'closer': (By.ID, "kick_ok_btn"), 're_login': True},
            {'detector': (By.ID, "system_show"), 'closer': (By.ID, "C_ok_btn_system")},
            {'detector': (By.ID, "alert_confirm"), 'closer': (By.ID, "yes_btn")},
            {'detector': (By.ID, "C_alert_confirm"), 'closer': (By.ID, "C_yes_btn")},
            {'detector': (By.ID, "alert_ok"), 'closer': (By.ID, "ok_btn")},
            {'detector': (By.ID, "C_alert_ok"), 'closer': (By.ID, "C_ok_btn")},
            {'detector': (By.ID, "info_pop"), 'closer': (By.ID, "info_close")},
            {'detector': (By.ID, "message_pop"), 'closer': (By.ID, "message_ok")},
        ]

        for rule in popup_rules:
            try:
                wait.until(EC.presence_of_element_located(rule['detector']))
                close_btn = wait.until(EC.element_to_be_clickable(rule['closer']))
                close_btn.click()
                print(f"已关闭 {rule['detector'][1]} 类型弹窗")

                if rule.get('re_login', False):
                    print("检测到登出，正在重新登录...")
                    acc = ACCOUNTS[0]
                    driver.get(BASE_URL)
                    login(driver, acc['username'], acc['password'])
                    navigate_to_football(driver)
                return
            except:
                continue

    except Exception as e:
        print(f"弹窗处理异常: {str(e)[:50]}")


def start_popup_monitor(driver):
    """后台弹窗监控线程"""

    def monitor_loop():
        while IS_RUNNING:
            if driver:
                handle_any_popup(driver)
            time.sleep(5)  # 监控频率

    thread = threading.Thread(target=monitor_loop, daemon=True)
    thread.start()
    print("弹窗监控线程已启动（每5秒检查一次）")


if __name__ == "__main__":
    print("程序启动中...")

    # 初始化浏览器驱动
    driver = init_driver()

    # 登录
    acc = ACCOUNTS[0]
    if not login(driver, acc['username'], acc['password']):
        driver.quit()
        sys.exit(1)

    # 导航到足球页面
    if not navigate_to_football(driver):
        driver.quit()
        sys.exit(1)

    # 点击HDP_OU按钮
    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, MARKET_TYPES['HDP_OU']))
        )
        button.click()
        print("已点击 HDP_OU 按钮")
        # 等待赔率核心元素（btn_hdpou_odd 是赔率按钮，确保数据渲染），最多等30秒
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'btn_hdpou_odd'))  # 等待赔率元素
            )
            time.sleep(2)  # 额外等2秒，确保所有赔率渲染完成
        except TimeoutException:
            print("警告：30秒内未加载到赔率元素，可能页面加载异常")
    except Exception as e:
        print(f"找不到 HDP_OU 按钮或点击失败: {e}")
        driver.quit()
        sys.exit(1)

    # 启动调度器、弹窗监控和每日重启任务
    start_scheduler()
    start_popup_monitor(driver)
    schedule_daily_restart()  # 启动每日12:05的重启任务

    # 运行Flask服务
    print("Flask服务启动，监听端口5002")
    app.run(host="0.0.0.0", port=5002, debug=False)

    # 程序结束时关闭浏览器
    if driver:
        driver.quit()
