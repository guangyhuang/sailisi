# -*- coding: utf-8 -*-
"""
load_mysql_V1.py
功能：
1) 连接 MySQL（支持中文库名），读取两张表到 DataFrame；
2) 用 LangChain 的 pandas DataFrame Agent（DeepSeek-V3 @ SiliconFlow）对两表并行问答；
3) 提供 refresh_cache() 一键刷新数据；
"""

import os
import time
import pandas as pd
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI


# ========= 1) LLM（SiliconFlow · DeepSeek-V3） =========
OPENAI_API_KEY = os.getenv("SILICONFLOW_API_KEY", "sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq")
OPENAI_BASE_URL = os.getenv("SILICONFLOW_BASE_URL", "https://api.siliconflow.cn/v1")

llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key=OPENAI_API_KEY,
    base_url=OPENAI_BASE_URL,   # SiliconFlow的OpenAI兼容接口
    temperature=0.2,            # 数据分析建议低温
    top_p=0.7,                  # 显式传参，避免告警
    streaming=False,            # 关闭流式，更稳
)

# ========= 2) MySQL 连接（中文库名直接写，不要手动编码） =========
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASS", "123456")
DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = os.getenv("MYSQL_DB", "台账")  # 可用中文库名

# 使用 SQLAlchemy 的 URL.create，避免你手动 quote_plus 导致 %e5%... 这种错误
url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,     # 若密码里有特殊符号，这里也不用你手动URL编码
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,     # 直接写中文库名
    query={"charset": "utf8mb4"},
)
engine = create_engine(url, pool_pre_ping=True)

# 两张目标表（中文表名）
TABLE_QR = "qr台账"
TABLE_SUP = "供应商质量问题台账"

# ========= 3) 从SQL读取为DataFrame（带缓存与行数上限） =========
MAX_ROWS = int(os.getenv("MAX_ROWS", "20000"))  # 防止一次性拉太多

COMMON_TIME_COLS = [
    "创建时间", "发现日期", "处理时间", "录入时间",
    "create_time", "order_date", "发生日期", "closed_time"
]

@lru_cache(maxsize=8)
def _read_table_cached(table_name: str, limit: int) -> pd.DataFrame:
    """真正执行数据库读取的被缓存函数。"""
    with engine.connect() as conn:
        sql = text(f"SELECT * FROM `{table_name}` LIMIT {limit}")
        df = pd.read_sql(sql, conn)
    # 解析常见时间列
    for col in COMMON_TIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="ignore")
    return df

def read_table(table_name: str, limit: int = MAX_ROWS) -> pd.DataFrame:
    """对外读取函数（使用缓存）。"""
    return _read_table_cached(table_name, limit)

def refresh_cache():
    """清空缓存并重新加载两个表为全局DataFrame。"""
    _read_table_cached.cache_clear()
    global df_qr, df_supplier
    df_qr = read_table(TABLE_QR, MAX_ROWS)
    df_supplier = read_table(TABLE_SUP, MAX_ROWS)
    print("🔄 已刷新 DataFrame 缓存。两表最新数据已载入。")

# 首次加载
df_qr = read_table(TABLE_QR, MAX_ROWS)
df_supplier = read_table(TABLE_SUP, MAX_ROWS)

# ========= 4) 构造 DataFrame 智能体 =========
custom_prefix = """
你是资深质量工程师，基于给定的DataFrame回答问题：
- 当问题需要筛选/聚合/去重/TopN时，请先说明步骤再给结论；
- 结果尽量控制在100行以内，可分组汇总与总结；
- 重点关注字段：供应商名称、零/部件名称、故障现象、故障类型、问题等级、发生频次、原因分析、临时/永久措施、发现区域、时间列等；
- 数据不足时请说明不足并给出下一步建议。
"""

agent_qr = create_pandas_dataframe_agent(
    llm=llm,
    df=df_qr,
    verbose=True,
    include_df_in_prompt=True,
    number_of_head_rows=5,
    agent_type="zero-shot-react-description",
    prefix=custom_prefix,
    allow_dangerous_code=True   # ✅ 打开执行 Python 的权限
)

agent_supplier = create_pandas_dataframe_agent(
    llm=llm,
    df=df_supplier,
    verbose=True,
    include_df_in_prompt=True,
    number_of_head_rows=5,
    agent_type="zero-shot-react-description",
    prefix=custom_prefix,
    allow_dangerous_code=True   # ✅ 同样这里也要打开
)

# ========= 5) 并行问答与重试 =========
def query_with_retry(agent, question: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            # 新版更稳的调用：传入 {"input": "..."}
            return agent.invoke({"input": question})
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1 * (attempt + 1))

def ask_both_tables(question: str):
    """并行对两张表进行问答，返回 [{},{}}] 格式。"""
    def process_qr():
        ans = query_with_retry(agent_qr, question)
        return {"QR台账分析结果": ans.get("output", ans)}
    def process_supplier():
        ans = query_with_retry(agent_supplier, question)
        return {"供应商质量问题台账分析结果": ans.get("output", ans)}
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(process_qr)
        f2 = ex.submit(process_supplier)
        return [f1.result(), f2.result()]

# ========= 6) 自检函数（可选）=========
def quick_self_check():
    with engine.connect() as conn:
        one = conn.execute(text("SELECT 1")).scalar()
        tables = conn.execute(text("SHOW TABLES")).fetchall()
    print("✅ MySQL连通测试：", one)
    print("✅ 当前库的表：", tables[:10], "...")

# ========= 7) 示例入口 =========
if __name__ == "__main__":
    # ——— 1) 快速连通自检（可注释掉） ———
    quick_self_check()

    # ——— 2) 示例问题 ———
    question = (
        "发运场发现一批ECU控制器短路问题："
        "①涉及的具体零/部件是什么；②涉及到的所有故障类型有哪些；"
        "③给出可操作的解决建议；请给出关键字段与统计口径，尽量汇总压缩。"
    )
    result = ask_both_tables(question)
    print("\n==== 最终结果 ====")
    print(result)

    # ——— 3) 刷新缓存示例（需要时再调用） ———
    # refresh_cache()
    # result2 = ask_both_tables("请统计2025年上半年按供应商的事件数量Top10。")
    # print(result2)
