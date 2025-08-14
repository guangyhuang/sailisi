# -*- coding: utf-8 -*-
import pandas as pd
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_community.chat_models import ChatOpenAI
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import time

# 1. 初始化 LLM
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    temperature=0.7,
    model_kwargs={"top_p": 0.7},
    streaming=True,
)

# 2. 文件路径
CSV_PATHS = {
    "QR": "QR台账.csv",
    "Supplier": "供应商质量问题台账.csv"
}

# 3. 带缓存的自动编码读取函数
@lru_cache(maxsize=2)
def read_csv_cached(path):
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="gbk")

# 4. 预加载数据
df_qr = read_csv_cached(CSV_PATHS["QR"])
df_supplier = read_csv_cached(CSV_PATHS["Supplier"])

# 5. 预初始化Agents
custom_prefix = """
你是一个资深质量工程师，善于分析产品台账中的根因、零部件问题和改进建议，请根据表格认真分析用户的问题。输出尽量控制在100行以内，必要时进行归类、合并或总结。
"""

agent_qr = create_pandas_dataframe_agent(
    llm=llm,
    df=df_qr,
    verbose=True,
    allow_dangerous_code=True,
    include_df_in_prompt=True,
    number_of_head_rows=5,
    agent_type="openai-tools",
    prefix=custom_prefix
)

agent_supplier = create_pandas_dataframe_agent(
    llm=llm,
    df=df_supplier,
    verbose=True,
    allow_dangerous_code=True,
    include_df_in_prompt=True,
    number_of_head_rows=5,
    agent_type="openai-tools",
    prefix=custom_prefix
)

# 6. 带重试机制的查询函数
def query_with_retry(agent, question, max_retries=3):
    for attempt in range(max_retries):
        try:
            return agent.invoke(question)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1 * (attempt + 1))
    return None

# 7. 主函数，并行分析两个CSV
def csv_main(question):
    def process_qr():
        return {"QR台账分析结果": query_with_retry(agent_qr, question)}
    
    def process_supplier():
        return {"供应商质量问题台账分析结果": query_with_retry(agent_supplier, question)}
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_qr = executor.submit(process_qr)
        future_supplier = executor.submit(process_supplier)
        
        result_qr = future_qr.result()
        result_supplier = future_supplier.result()
    
    return [result_qr, result_supplier]

# 8. 测试入口
if __name__ == '__main__':
    question = "我现在有一批ECU控制器在发运场发现短路问题，帮我分析一下具体的零部件是什么?它涉及的所有故障类型有哪些?如何解决?输出内容放在字典中返回。"
    ans = csv_main(question)
    print(ans)
