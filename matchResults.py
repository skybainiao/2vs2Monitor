import time
import sys
import json
import psycopg2
import logging
import re
import schedule  # 新增：导入定时任务库
from datetime import datetime, timedelta
from psycopg2 import OperationalError, IntegrityError
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from flask import Flask, jsonify, request
import threading
from flask_cors import CORS

# 初始化Flask应用
app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 状态跟踪变量
is_running = False
current_task_id = None
task_history = []

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sports_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}

# 爬虫配置
CRAWL_CONFIG = {
    "accounts": [
        {'username': 'czzvaa22', 'password': 'dddd1111DD'},
    ],
    "base_url": 'https://205.201.0.120/',
    "target_days": 10  # 默认抓取天数
}

# 同步配置
SYNC_CONFIG = {
    "batch_size": 1000,
    "dry_run": False,
    "date_range": 1  # 日期范围：±1天
}

# 新增：定时任务配置
SCHEDULER_CONFIG = {
    "crawl_time": "12:10"  # 每天执行时间（北京时间）
}


# ------------------------------
# 爬虫相关函数
# ------------------------------
def init_driver():
    """初始化浏览器（补充网络配置，解决连接被拒绝）"""
    try:
        import random
        import os
        chrome_options = Options()
        # 基础核心配置（保留之前的崩溃修复参数）
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])

        # 1. 增强SSL/证书错误忽略（核心修复连接被拒）
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-certificate-errors-spki-list')  # 额外忽略SPKI证书错误
        chrome_options.add_argument('--allow-insecure-localhost')  # 允许不安全连接

        # 2. 禁用IPv6（部分网站IPv6连接不稳定，导致拒绝连接）
        chrome_options.add_argument('--disable-ipv6')

        # 3. 启用缓存+Cookie（维持会话，减少首次连接验证压力）
        prefs = {
            'profile.default_content_setting_values.cookies': 1,  # 启用Cookie
            'disk-cache-size': 4096000  # 启用磁盘缓存（4MB）
        }
        chrome_options.add_experimental_option('prefs', prefs)

        # 4. 禁用Chrome网络服务（Windows环境下部分版本冲突导致连接失败）
        chrome_options.add_argument('--disable-features=NetworkService')

        # 5. 简化用户数据目录（确保权限）
        user_data_dir = os.path.join(os.getcwd(), "chrome_data")
        chrome_options.add_argument(f'--user-data-dir={user_data_dir}')
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)
            logging.info(f"创建Chrome用户数据目录: {user_data_dir}")

        # 6. 随机User-Agent（匹配Chrome 143版本）
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.7499.40 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.7499.38 Safari/537.36"
        ]
        chrome_options.add_argument(f'user-agent={random.choice(USER_AGENTS)}')

        # 7. 兼容日志路径
        service = Service(ChromeDriverManager().install())
        service.log_path = 'nul' if os.name == 'nt' else '/dev/null'

        driver = webdriver.Chrome(service=service, options=chrome_options)

        # 8. 增强反检测JS（保留）
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, "webdriver", {get: () => undefined});
                Object.defineProperty(navigator, "languages", {get: () => ["en-US", "en", "zh-CN"]});
                Object.defineProperty(navigator, "plugins", {get: () => [1, 2, 3]});
                Object.defineProperty(navigator, "mimeTypes", {get: () => [1, 2, 3]});
                Object.defineProperty(navigator.permissions, "query", {
                    value: () => Promise.resolve({ state: "granted" })
                });
            '''
        })

        # 9. 超时时间（延长至180秒，适应网络波动）
        driver.set_page_load_timeout(180)
        driver.set_script_timeout(150)

        logging.info("WebDriver 初始化完成（已补充网络兼容配置）")
        return driver
    except Exception as e:
        logging.error(f"WebDriver 初始化失败: {e}")
        return None


def login(driver):
    """登录流程（添加随机延迟，模仿A程序人工操作）"""
    try:
        import random
        # 1. 访问前随机延迟2-4秒（核心！避免瞬间访问被拒绝）
        delay = random.uniform(2, 4)
        logging.info(f"登录前等待 {delay:.1f} 秒...")
        time.sleep(delay)

        # 2. 访问目标URL
        driver.get(CRAWL_CONFIG["base_url"])
        logging.info("已发送访问请求，等待页面响应...")

        # 3. 延长等待时间（120秒），确保网络连接稳定
        WebDriverWait(driver, 120).until(
            EC.element_to_be_clickable((By.ID, 'lang_en'))
        ).click()
        logging.info("已选择语言")

        # 4. 输入账号前再延迟1-2秒
        time.sleep(random.uniform(1, 2))
        WebDriverWait(driver, 60).until(
            EC.visibility_of_element_located((By.ID, 'usr'))
        ).send_keys(CRAWL_CONFIG["accounts"][0]['username'])

        # 5. 输入密码前延迟0.5-1秒（模仿人工输入间隔）
        time.sleep(random.uniform(0.5, 1))
        driver.find_element(By.ID, 'pwd').send_keys(CRAWL_CONFIG["accounts"][0]['password'])

        # 6. 点击登录前延迟1秒
        time.sleep(1)
        driver.find_element(By.ID, 'btn_login').click()
        logging.info("已点击登录按钮，等待跳转...")

        # 7. 处理弹窗（延长等待至20秒）
        try:
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, 'C_no_btn'))
            ).click()
            logging.info("已关闭弹窗")
        except:
            logging.info("无弹窗需要关闭")

        # 8. 验证登录成功（延长等待至120秒）
        WebDriverWait(driver, 120).until(
            EC.visibility_of_element_located((By.ID, 'today_page'))
        )
        logging.info("登录成功")
        return True
    except Exception as e:
        logging.error(f"登录失败: {e}")
        return False


def extract_results_title_only(driver):
    """提取标题并返回目标iframe索引"""
    try:
        original_handle = driver.current_window_handle
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.ID, 'rightM_result'))
        ).click()
        logging.info("已点击RESULTS按钮，等待新窗口...")

        WebDriverWait(driver, 20).until(
            lambda x: len(driver.window_handles) > 1
        )
        new_handle = [h for h in driver.window_handles if h != original_handle][0]
        driver.switch_to.window(new_handle)
        logging.info(f"已切换到新窗口（句柄: {new_handle[-10:]}）")

        logging.info("开始验证Results标题...")
        for _ in range(15):  # 延长等待至75秒
            try:
                js_code = """
                    var iframes = document.getElementsByTagName('iframe');
                    console.log('iframe总数: ' + iframes.length);
                    for(var i=0; i<iframes.length; i++){
                        try {
                            var doc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                            var header = doc.querySelector('div.acc_header.noFloat h1');
                            if(header && header.textContent.trim() === 'Results') {
                                console.log('找到目标iframe，索引: ' + i);
                                return i;
                            }
                        } catch(e) {
                            console.log('iframe['+i+']访问错误: ' + e);
                        }
                    }
                    return -1;
                """
                iframe_index = driver.execute_script(js_code)

                if iframe_index != -1:
                    logging.info(f"✅ 找到Results标题（目标iframe索引: {iframe_index}）")
                    return (new_handle, iframe_index)
                else:
                    logging.info("暂未找到标题，5秒后重试...")
                    time.sleep(5)
            except Exception as e:
                logging.error(f"标题提取错误: {e}，5秒后重试...")
                time.sleep(5)

        logging.error("❌ 标题验证超时")
        return None

    except Exception as e:
        logging.error(f"标题提取失败: {e}")
        return None


def get_current_date(driver, iframe_index):
    """获取当前日期"""
    try:
        js_code = f"""
            var iframe = document.getElementsByTagName('iframe')[{iframe_index}];
            try {{
                var doc = iframe.contentDocument || iframe.contentWindow.document;
                var dateElem = doc.getElementById('date_start');
                if(dateElem) return dateElem.textContent.trim();
                else return '日期元素不存在';
            }} catch(e) {{
                return '访问iframe错误: ' + e;
            }}
        """
        date_text = driver.execute_script(js_code)

        if date_text.startswith('访问iframe错误') or date_text == '日期元素不存在':
            logging.error(f"获取日期失败: {date_text}")
            return None

        # 验证日期格式
        datetime.strptime(date_text, '%Y-%m-%d')
        logging.info(f"当前页面日期: {date_text}")
        return date_text
    except Exception as e:
        logging.error(f"获取日期失败: {e}")
        return None


def switch_to_previous_day(driver, iframe_index):
    """切换到前一天"""
    try:
        # 先获取当前日期
        current_date = get_current_date(driver, iframe_index)
        if not current_date:
            return None

        # 执行JS点击按钮
        js_code = f"""
            var iframe = document.getElementsByTagName('iframe')[{iframe_index}];
            try {{
                var doc = iframe.contentDocument || iframe.contentWindow.document;
                var prevBtn = doc.querySelector('.acc_previous_btn');
                if(prevBtn) {{
                    prevBtn.click();
                    return '按钮已点击';
                }} else {{
                    return 'Previous按钮不存在';
                }}
            }} catch(e) {{
                return '点击失败: ' + e;
            }}
        """
        result = driver.execute_script(js_code)
        logging.info(f"JS操作结果: {result}")

        # 等待日期更新（最多15秒）
        for _ in range(15):
            new_date = get_current_date(driver, iframe_index)
            if new_date and new_date != current_date:
                logging.info(f"已切换到日期: {new_date}")
                return new_date
            time.sleep(1)

        logging.warning("日期未更新，可能切换失败")
        return None
    except Exception as e:
        logging.error(f"切换日期失败: {e}")
        return None


def extract_match_data(driver, new_handle, iframe_index, current_date):
    """提取比赛数据"""
    try:
        driver.switch_to.window(new_handle)
        logging.info(f"开始提取 {current_date} 的比赛数据（目标iframe索引: {iframe_index}）...")

        for _ in range(24):
            try:
                js_code = f"""
                    var iframe = document.getElementsByTagName('iframe')[{iframe_index}];
                    try {{
                        var doc = iframe.contentDocument || iframe.contentWindow.document;
                        var allRows = Array.from(doc.querySelectorAll('tr')).filter(row => 
                            !row.classList.contains('acc_results_tr_title')
                        );

                        var allData = [];
                        var currentLeague = null;

                        for(var r=0; r<allRows.length; r++){{
                            var row = allRows[r];

                            if(row.classList.contains('acc_results_league')){{
                                var leagueName = row.querySelector('span')?.textContent.trim() || '未知联赛';
                                currentLeague = {{ league: leagueName, matches: [] }};
                                allData.push(currentLeague);
                            }}
                            else if(row.classList.contains('acc_result_tr_top') && currentLeague){{
                                var timeCell = row.querySelector('td.acc_result_time');
                                var matchTime = timeCell ? timeCell.textContent.trim().replace(/\s+/g, ' ') : '未知时间';

                                var homeTeam = row.querySelector('td.acc_result_team')?.textContent.trim() || '未知主队';
                                var homeFull = row.querySelector('td.acc_result_full')?.textContent.trim() || '无数据';
                                var homeHalf = row.querySelector('td.acc_result_bg')?.textContent.trim() || '无数据';

                                var awayRow = allRows[r+1];
                                if(awayRow && awayRow.classList.contains('acc_result_tr_other')){{
                                    var awayTeam = awayRow.querySelector('td.acc_result_team')?.textContent.trim() || '未知客队';
                                    var awayFull = awayRow.querySelector('td.acc_result_full')?.textContent.trim() || '无数据';
                                    var awayHalf = awayRow.querySelector('td.acc_result_bg')?.textContent.trim() || '无数据';

                                    currentLeague.matches.push({{
                                        match_time: matchTime,
                                        home_team: homeTeam,
                                        away_team: awayTeam,
                                        full_time: `${{homeFull}}-${{awayFull}}`,
                                        half_time: `${{homeHalf}}-${{awayHalf}}`,
                                        status: homeFull.includes('Wrong') ? '数据异常' : '正常'
                                    }});
                                    r++;
                                }}
                            }}
                        }}
                        return allData;
                    }} catch(e) {{
                        return '提取错误: ' + e;
                    }}
                """
                match_data = driver.execute_script(js_code)

                if isinstance(match_data, list) and len(match_data) > 0:
                    total_leagues = len(match_data)
                    total_matches = sum(len(league['matches']) for league in match_data)
                    logging.info(f"✅ {current_date} 数据提取成功：{total_leagues}个联赛，{total_matches}场比赛")
                    return match_data
                elif isinstance(match_data, str):
                    logging.warning(f"{current_date} 提取错误: {match_data}，5秒后重试...")
                else:
                    logging.info(f"{current_date} 暂未提取到数据，5秒后重试...")
                time.sleep(5)
            except Exception as e:
                logging.error(f"{current_date} 数据提取错误: {e}，5秒后重试...")
                time.sleep(5)

        logging.error(f"❌ {current_date} 超时：未提取到数据")
        return None

    except Exception as e:
        logging.error(f"{current_date} 数据提取失败: {e}")
        return None


# ------------------------------
# 数据库相关函数
# ------------------------------
def create_db_connection():
    """创建数据库连接"""
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            port=DB_CONFIG["port"],
        )
        logging.info("数据库连接成功")
    except OperationalError as e:
        logging.error(f"数据库连接失败: {e}")
    return connection


def create_tables(connection):
    """创建必要的数据库表"""
    if not connection:
        logging.error("没有有效数据库连接，无法创建表")
        return False

    try:
        cursor = connection.cursor()

        # 创建联赛表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sports_leagues (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name)
        )
        ''')

        # 创建比赛表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sports_matches (
            id SERIAL PRIMARY KEY,
            league_id INTEGER REFERENCES sports_leagues(id),
            match_date DATE NOT NULL,
            match_time VARCHAR(50),
            home_team VARCHAR(255) NOT NULL,
            away_team VARCHAR(255) NOT NULL,
            full_time VARCHAR(50),
            half_time VARCHAR(50),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_date, match_time, home_team, away_team)
        )
        ''')

        connection.commit()
        logging.info("数据库表创建/验证成功")
        return True
    except Exception as e:
        logging.error(f"创建表失败: {e}")
        connection.rollback()
        return False


def insert_match_data(connection, date, data):
    """将比赛数据插入表中"""
    if not connection or not data:
        logging.error("没有有效数据库连接或数据，无法插入")
        return False

    try:
        cursor = connection.cursor()
        match_date = datetime.strptime(date, '%Y-%m-%d').date()

        for league in data:
            # 先插入联赛
            cursor.execute(
                "INSERT INTO sports_leagues (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (league['league'],)
            )

            # 获取联赛ID
            league_id = cursor.fetchone()
            if not league_id:
                # 如果联赛已存在，查询ID
                cursor.execute("SELECT id FROM sports_leagues WHERE name = %s", (league['league'],))
                league_id = cursor.fetchone()

            if league_id:
                league_id = league_id[0]

                # 插入比赛数据
                for match in league['matches']:
                    try:
                        cursor.execute('''
                        INSERT INTO sports_matches 
                        (league_id, match_date, match_time, home_team, away_team, full_time, half_time, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_date, match_time, home_team, away_team) DO NOTHING
                        ''', (
                            league_id,
                            match_date,
                            match['match_time'],
                            match['home_team'],
                            match['away_team'],
                            match['full_time'],
                            match['half_time'],
                            match['status']
                        ))
                    except IntegrityError as e:
                        logging.warning(f"比赛数据已存在，跳过: {match['home_team']} vs {match['away_team']}")
                        connection.rollback()
                        continue

        connection.commit()
        logging.info(f"成功将 {date} 的数据插入表中")
        return True
    except Exception as e:
        logging.error(f"插入数据失败: {e}")
        connection.rollback()
        return False


# ------------------------------
# 数据同步相关函数
# ------------------------------
def get_sports_data(connection):
    """获取sports_matches表中的数据"""
    if not connection:
        return []
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            sm.id AS sports_match_id,
            sm.match_date,
            sm.home_team,
            sm.away_team,
            sm.full_time,
            sm.half_time,
            sl.name AS league_name
        FROM sports_matches sm
        JOIN sports_leagues sl ON sm.league_id = sl.id
        WHERE sm.full_time IS NOT NULL AND sm.full_time != '无数据'
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logging.info(f"加载sports_matches数据: {len(data)}条")
        return data
    except Exception as e:
        logging.error(f"获取sports数据失败: {e}")
        return []


def get_matches_data(connection):
    """获取matches表中待更新的数据"""
    if not connection:
        return []
    try:
        cursor = connection.cursor()
        query = """
        SELECT id AS match_id, league_name, home_team, away_team, start_time_beijing
        FROM matches
        WHERE full_time IS NULL OR half_time IS NULL
        LIMIT %s
        """
        cursor.execute(query, (SYNC_CONFIG["batch_size"],))
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logging.info(f"加载待更新matches数据: {len(data)}条")
        return data
    except Exception as e:
        logging.error(f"获取matches数据失败: {e}")
        return []


def clean_text(text):
    """文本清洗：移除特殊字符、合并空格、统一小写"""
    if not text:
        return ""
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()


def filter_sports_by_date(sports_data, target_date):
    """按日期范围筛选数据"""
    start = target_date - timedelta(days=SYNC_CONFIG["date_range"])
    end = target_date + timedelta(days=SYNC_CONFIG["date_range"])
    filtered = [s for s in sports_data if start <= s['match_date'] <= end]
    logging.info(f"日期筛选: 目标[{target_date}]，范围[{start}至{end}]，候选{len(filtered)}条")
    return filtered


def parse_matches_date(time_str):
    """解析日期字符串"""
    try:
        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').date()
    except ValueError:
        logging.warning(f"无效时间格式: {time_str}")
        return None


def sync_match_data(connection):
    """同步比赛数据到matches表"""
    logging.info("=== 开始同步比赛数据 ===")
    logging.info(
        f"同步配置: 批量{SYNC_CONFIG['batch_size']}条, 日期±{SYNC_CONFIG['date_range']}天, 测试模式{SYNC_CONFIG['dry_run']}")

    if not connection:
        logging.error("无数据库连接，无法同步数据")
        return 0

    try:
        sports_data = get_sports_data(connection)
        matches_data = get_matches_data(connection)

        if not sports_data:
            logging.warning("无可用的sports数据，无法进行同步")
            return 0

        if not matches_data:
            logging.info("没有需要更新的matches数据")
            return 0

        total_matched = 0
        for target in matches_data:
            mid = target['match_id']
            logging.info(f"\n----- 处理比赛 {mid} -----")
            logging.info(f"目标比赛(matches表):")
            logging.info(f"  联赛: {target['league_name']}")
            logging.info(f"  对阵: {target['home_team']} vs {target['away_team']}")
            logging.info(f"  时间: {target['start_time_beijing']}")

            # 解析目标日期
            target_date = parse_matches_date(target['start_time_beijing'])
            if not target_date:
                logging.warning(f"比赛{mid}：跳过（日期解析失败）")
                continue

            # 筛选候选比赛
            candidates = filter_sports_by_date(sports_data, target_date)
            if not candidates:
                logging.info(f"比赛{mid}：无日期匹配的候选比赛")
                continue

            # 名称匹配
            matched = False
            for sport in candidates:
                # 提取并清洗两边的名称
                sport_league = sport['league_name']
                target_league = target['league_name']
                sport_home = sport['home_team']
                target_home = target['home_team']
                sport_away = sport['away_team']
                target_away = target['away_team']

                # 清洗后的值
                sl_clean = clean_text(sport_league)
                tl_clean = clean_text(target_league)
                sh_clean = clean_text(sport_home)
                th_clean = clean_text(target_home)
                sa_clean = clean_text(sport_away)
                ta_clean = clean_text(target_away)

                # 严格匹配
                if sl_clean == tl_clean and sh_clean == th_clean and sa_clean == ta_clean:
                    logging.info(f"✅ 匹配成功 (sports_matches表 ID: {sport['sports_match_id']})")
                    logging.info(f"  匹配详情:")
                    logging.info(f"    联赛: {sport_league} == {target_league}")
                    logging.info(f"    主队: {sport_home} == {target_home}")
                    logging.info(f"    客队: {sport_away} == {target_away}")
                    logging.info(f"  比分更新: 全场{sport['full_time']}, 半场{sport['half_time']}")

                    if not SYNC_CONFIG["dry_run"]:
                        cursor = connection.cursor()
                        cursor.execute("""
                            UPDATE matches
                            SET full_time = %s, half_time = %s
                            WHERE id = %s
                        """, (sport['full_time'], sport['half_time'], mid))
                        connection.commit()

                    total_matched += 1
                    matched = True
                    break

            if not matched:
                if candidates:
                    last_candidate = candidates[-1]
                    logging.info(
                        f"❌ 无匹配项 (最后候选示例: {last_candidate['league_name']} | {last_candidate['home_team']} vs {last_candidate['away_team']})")
                else:
                    logging.info("❌ 无匹配项")

        logging.info(f"\n=== 同步完成 ===")
        logging.info(f"总处理: {len(matches_data)}条，成功匹配: {total_matched}条")
        return total_matched

    except Exception as e:
        logging.error(f"同步过程错误: {e}")
        if connection:
            connection.rollback()
        return 0


# ------------------------------
# 核心流程函数
# ------------------------------
def run_crawl_and_sync(task_id):
    """执行完整的抓取和同步流程"""
    global is_running, current_task_id

    # 记录任务开始时间
    start_time = datetime.now()
    result = False
    message = ""
    matched_count = 0
    crawled_days = 0

    try:
        logging.info(f"=== 任务 {task_id} 开始执行: 体育比赛数据抓取与同步 ===")

        # 初始化数据库连接
        db_connection = create_db_connection()
        if not db_connection:
            message = "无法建立数据库连接，程序无法继续"
            logging.error(message)
            return

        # 初始化浏览器驱动
        driver = init_driver()
        if not driver:
            message = "无法初始化浏览器驱动，程序无法继续"
            logging.error(message)
            db_connection.close()
            return

        all_data = {}
        try:
            # 登录网站
            if not login(driver):
                message = "登录失败，程序终止"
                logging.error(message)
                return

            # 展开菜单
            try:
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.ID, 'myAcc_page'))
                ).click()
                logging.info("展开按钮点击成功")
                time.sleep(2)
            except Exception as e:
                message = f"展开按钮点击失败: {e}"
                logging.error(message)
                return

            # 获取目标窗口和iframe索引
            frame_result = extract_results_title_only(driver)
            if not frame_result:
                message = "标题验证失败，终止程序"
                logging.error(message)
                return
            new_window_handle, target_iframe_index = frame_result

            # 循环抓取前TARGET_DAYS天数据
            for day in range(CRAWL_CONFIG["target_days"]):
                driver.switch_to.window(new_window_handle)
                # 获取当前日期
                current_date = get_current_date(driver, target_iframe_index)
                if not current_date:
                    message = "无法获取当前日期，跳过该天"
                    logging.error(message)
                    break

                # 提取当天数据
                match_data = extract_match_data(driver, new_window_handle, target_iframe_index, current_date)
                if match_data:
                    all_data[current_date] = match_data

                    # 插入数据库
                    insert_match_data(db_connection, current_date, match_data)
                    logging.info("")
                    crawled_days += 1

                # 切换到前一天
                if day < CRAWL_CONFIG["target_days"] - 1:
                    if not switch_to_previous_day(driver, target_iframe_index):
                        message = "切换日期失败，终止后续抓取"
                        logging.error(message)
                        break
                    time.sleep(8)  # 等待数据加载

            logging.info(f"\n===== 所有数据抓取完成 =====")
            logging.info(f"共抓取 {len(all_data)} 天数据")

            # 抓取完成后立即同步数据
            matched_count = sync_match_data(db_connection)
            result = True
            message = f"操作成功，共抓取 {len(all_data)} 天数据，成功匹配 {matched_count} 条比赛数据"

        except Exception as e:
            message = f"程序运行出错: {e}"
            logging.error(message)
            result = False
        finally:
            # 关闭浏览器和数据库连接
            driver.quit()
            logging.info("浏览器已关闭")

            if db_connection:
                db_connection.close()
                logging.info("数据库连接已关闭")

    except Exception as e:
        message = f"任务执行出错: {str(e)}"
        logging.error(message)
    finally:
        # 更新任务状态
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # 分钟

        # 记录任务历史
        global task_history
        task_history.append({
            'task_id': task_id,
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': round(duration, 2),
            'success': result,
            'message': message,
            'crawled_days': crawled_days,
            'matched_count': matched_count
        })

        # 限制历史记录数量
        if len(task_history) > 100:
            task_history = task_history[-100:]

        # 更新全局状态
        is_running = False
        current_task_id = None
        logging.info(f"=== 任务 {task_id} 执行完毕 ===")


def handle_request(config=None):
    """处理抓取请求的包装函数"""
    global is_running, current_task_id

    # 如果没有提供配置，使用默认配置
    config = config or {}

    # 更新配置
    if 'target_days' in config:
        CRAWL_CONFIG['target_days'] = int(config['target_days'])
    if 'dry_run' in config:
        SYNC_CONFIG['dry_run'] = bool(config['dry_run'])

    # 如果当前有任务在运行，则不执行新任务
    if is_running:
        logging.info(f'定时任务触发时发现有任务正在运行 (任务ID: {current_task_id})，已跳过')
        return

    # 生成任务ID
    task_id = datetime.now().strftime('%Y%m%d%H%M%S')
    current_task_id = task_id
    is_running = True

    # 执行任务
    run_crawl_and_sync(task_id)


# ------------------------------
# 新增：定时任务相关函数
# ------------------------------
def start_scheduler():
    """启动定时任务调度器"""
    # 每天指定时间执行任务
    crawl_time = SCHEDULER_CONFIG["crawl_time"]
    schedule.every().day.at(crawl_time).do(scheduled_task)
    logging.info(f"定时任务已配置：每天北京时间 {crawl_time} 执行数据抓取与同步")

    # 启动调度器循环
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次


def scheduled_task():
    """定时任务执行的函数"""
    logging.info("===== 定时任务触发 =====")
    # 使用默认配置执行任务
    thread = threading.Thread(
        target=handle_request,
        args=(None,)
    )
    thread.start()


# ------------------------------
# API接口
# ------------------------------
@app.route('/start-crawl', methods=['POST'])
def start_crawl():
    """启动数据抓取和同步流程的API接口"""
    global is_running, current_task_id

    if is_running:
        return jsonify({
            'success': False,
            'message': f'当前有任务正在运行 (任务ID: {current_task_id})，请稍后再试',
            'current_task_id': current_task_id
        }), 400

    # 获取请求中的配置参数
    config = request.json or {}

    # 生成新任务ID
    task_id = datetime.now().strftime('%Y%m%d%H%M%S')

    # 在新线程中运行任务，避免阻塞API
    thread = threading.Thread(
        target=handle_request,
        args=(config,)
    )
    thread.start()

    return jsonify({
        'success': True,
        'message': f'数据抓取与同步任务已启动',
        'task_id': task_id,
        'config': {
            'target_days': CRAWL_CONFIG['target_days'],
            'dry_run': SYNC_CONFIG['dry_run']
        }
    })


@app.route('/status', methods=['GET'])
def get_status():
    """获取当前任务状态的API接口"""
    return jsonify({
        'is_running': is_running,
        'current_task_id': current_task_id,
        'current_config': {
            'target_days': CRAWL_CONFIG['target_days'],
            'dry_run': SYNC_CONFIG['dry_run'],
            'scheduled_time': SCHEDULER_CONFIG['crawl_time']  # 新增：返回定时任务时间
        }
    })


@app.route('/task-history', methods=['GET'])
def get_task_history():
    """获取任务历史记录"""
    return jsonify({
        'task_count': len(task_history),
        'tasks': task_history
    })


# ------------------------------
# 程序入口
# ------------------------------
if __name__ == "__main__":
    # 启动API服务，程序将一直运行等待请求
    logging.info("=== 体育数据抓取服务启动 ===")
    logging.info("API服务启动，监听端口：5040")
    logging.info("等待接收抓取请求...")

    # 新增：启动定时任务线程
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # 启动Flask服务，host=0.0.0.0允许外部访问
    app.run(host='0.0.0.0', port=5040, debug=False, use_reloader=False)
