from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.chat_models import ChatOpenAI
import pandas as pd

# 1. 初始化 LLM
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    temperature=0.7,
    top_p=0.7,
    streaming=True
)

# 2. 正确读取 CSV 文件，自动尝试编码
csv_file_path = r"D:\projects\sailisi\full.csv"


def csv_main(question):

    # 尝试不同编码方式读取
    try:
        df = pd.read_csv(csv_file_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_file_path, encoding="gbk")  # 或 encoding="ISO-8859-1"

    # 3. 创建 Pandas DataFrame Agent
    agent = create_pandas_dataframe_agent(
        llm=llm,
        df=df,
        verbose=True,
        allow_dangerous_code=True,
        number_of_head_rows = 20
    )

    response = agent.invoke(question)

    return response


if __name__ == '__main__':
    question = "我现在有一批ECU控制器在发运场发现短路问题，帮我分析一下具体的零部件是什么？它涉及的所有故障类型有哪些？如何解决？输出内容放在字典中返回。"
    ans = csv_main(question)
    print(ans)


