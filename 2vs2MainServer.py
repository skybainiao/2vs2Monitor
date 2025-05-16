import asyncio
import aiohttp
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import DictCursor

# API配置
API_URLS = [
    "http://127.0.0.1:5001/get_odds1",
    "http://127.0.0.1:5002/get_odds2",
    "http://127.0.0.1:5003/get_odds3"
]

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "cjj2468830035",
    "port": 5432
}


async def fetch_api(session, url):
    """异步获取单个API数据"""
    print(f"⏳ 开始请求: {url}")
    start_time = datetime.now()

    try:
        async with session.get(url) as response:
            elapsed = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
            print(f"⏱️ 完成请求 {url} (耗时: {elapsed:.2f}ms)")

            if response.status == 200:
                data = await response.json()
                print(f"✅ {url} 返回状态码 {response.status}")
                return {
                    "url": url,
                    "status": "success",
                    "data": data,
                    "timestamp": datetime.now().isoformat(),
                    "response_time": elapsed
                }
            else:
                print(f"❌ {url} 返回错误状态码 {response.status}")
                return {
                    "url": url,
                    "status": "error",
                    "status_code": response.status,
                    "timestamp": datetime.now().isoformat(),
                    "response_time": elapsed
                }
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
        print(f"❗ 错误: {url} 请求失败 - {str(e)}")
        return {
            "url": url,
            "status": "error",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat(),
            "response_time": elapsed
        }


def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❗ 数据库连接失败: {str(e)}")
        return None


def find_matching_bindings(league_name: str, home_team: str, away_team: str) -> List[Dict[str, Any]]:
    """
    在数据库中查找匹配的bindings记录

    参数:
        league_name: 联赛名称
        home_team: 主队名称
        away_team: 客队名称

    返回:
        匹配的bindings记录列表
    """
    conn = get_db_connection()
    if not conn:
        return []

    matches = []

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 1. 先根据联赛名称查找所有可能的bindings
            query = """
            SELECT * FROM bindings 
            WHERE source3_league = %s
            """
            cursor.execute(query, (league_name,))
            league_bindings = cursor.fetchall()

            # 2. 在这些bindings中查找主队或客队匹配的记录
            for binding in league_bindings:
                # 检查主队或客队是否匹配source3中的任何一个队
                if (home_team == binding['source3_home_team'] or home_team == binding['source3_away_team'] or
                        away_team == binding['source3_home_team'] or away_team == binding['source3_away_team']):
                    matches.append({
                        "source1_league": binding['source1_league'],
                        "source1_home_team": binding['source1_home_team'],
                        "source1_away_team": binding['source1_away_team'],
                        "source2_league": binding['source2_league'],
                        "source2_home_team": binding['source2_home_team'],
                        "source2_away_team": binding['source2_away_team'],
                        "source3_league": binding['source3_league'],
                        "source3_home_team": binding['source3_home_team'],
                        "source3_away_team": binding['source3_away_team']
                    })

    except Exception as e:
        print(f"❗ 数据库查询错误: {str(e)}")
    finally:
        if conn:
            conn.close()

    return matches


async def process_api_data(results: List[Dict[str, Any]]):
    """处理API数据并与数据库数据交叉匹配"""
    # 假设第三个API是source3
    source3_result = next((r for r in results if r["url"] == API_URLS[2]), None)

    if not source3_result or source3_result["status"] != "success":
        print("❌ 无法获取source3数据")
        return

    source3_data = source3_result["data"]

    print("\n" + "=" * 50)
    print("🔍 开始交叉匹配数据")
    print("=" * 50)

    # 处理每个比赛
    for match in source3_data:
        league_name = match["league_name"]
        home_team = match["home_team"]
        away_team = match["away_team"]

        print(f"\n🔎 查找比赛: {league_name} - {home_team} vs {away_team}")

        # 在数据库中查找匹配的bindings
        matching_bindings = find_matching_bindings(league_name, home_team, away_team)

        if not matching_bindings:
            print("❌ 未找到匹配的bindings记录")
            continue

        print(f"✅ 找到 {len(matching_bindings)} 条匹配记录")

        # 生成3场比赛数据
        combined_matches = []

        # 1. 原始比赛
        combined_matches.append({
            "league": league_name,
            "home_team": home_team,
            "away_team": away_team,
            "source": "source3",
            "odds": match["odds"]
        })

        # 2. 添加source1的比赛信息
        if matching_bindings[0]["source1_league"]:
            combined_matches.append({
                "league": matching_bindings[0]["source1_league"],
                "home_team": matching_bindings[0]["source1_home_team"],
                "away_team": matching_bindings[0]["source1_away_team"],
                "source": "source1",
                "odds": match["odds"]  # 使用原始赔率数据
            })

        # 3. 添加source2的比赛信息
        if matching_bindings[0]["source2_league"]:
            combined_matches.append({
                "league": matching_bindings[0]["source2_league"],
                "home_team": matching_bindings[0]["source2_home_team"],
                "away_team": matching_bindings[0]["source2_away_team"],
                "source": "source2",
                "odds": match["odds"]  # 使用原始赔率数据
            })

        # 打印组合后的比赛数据
        print("\n" + "-" * 30)
        print(f"📊 组合后的比赛数据 ({len(combined_matches)}场)")
        print("-" * 30)

        for i, combined_match in enumerate(combined_matches, 1):
            print(f"\n🏆 比赛 {i} ({combined_match['source']})")
            print(f"联赛: {combined_match['league']}")
            print(f"主队: {combined_match['home_team']}")
            print(f"客队: {combined_match['away_team']}")
            print(f"赔率: {json.dumps(combined_match['odds'], indent=2)}")


async def main():
    """主函数：并发获取所有API数据并打印"""
    print("\n" + "=" * 50)
    print(f"🚀 开始并发API数据获取流程 - {datetime.now().isoformat()}")
    print("=" * 50)

    # 创建会话
    print("🔄 创建HTTP会话...")
    async with aiohttp.ClientSession() as session:
        print("📡 准备并发请求以下API:")
        for url in API_URLS:
            print(f"  - {url}")

        print("\n🚨 开始并发请求 (这将同时发送所有请求)...")
        start_time = datetime.now()

        # 创建并执行所有请求任务
        tasks = [fetch_api(session, url) for url in API_URLS]
        results = await asyncio.gather(*tasks)

        total_time = (datetime.now() - start_time).total_seconds() * 1000  # 毫秒
        print(f"\n🎉 所有API请求已完成 (总耗时: {total_time:.2f}ms)")

        # 计算成功和失败的请求数
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = len(results) - success_count

        print(f"\n📊 请求统计:")
        print(f"  ✅ 成功: {success_count}")
        print(f"  ❌ 失败: {error_count}")

        # 格式化输出每个API的结果
        print("\n" + "=" * 50)
        print(f"API数据获取结果详情 - {datetime.now().isoformat()}")
        print("=" * 50)

        for result in results:
            print(f"\nURL: {result['url']}")
            print(f"状态: {'✅成功' if result['status'] == 'success' else '❌失败'}")
            print(f"响应时间: {result['response_time']:.2f}ms")

            if result['status'] == 'success':
                # 美化JSON输出
                print("数据:")
                try:
                    print(json.dumps(result['data'], indent=2, ensure_ascii=False)[:1000])  # 限制最大长度
                except (TypeError, json.JSONDecodeError):
                    print(f"[非JSON数据] {str(result['data'])[:1000]}")
            else:
                error_info = result.get('status_code', result.get('error_message', '未知错误'))
                print(f"错误信息: {error_info}")

        # 处理API数据并与数据库交叉匹配
        await process_api_data(results)


if __name__ == "__main__":
    # 解决Windows系统下的事件循环问题
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())