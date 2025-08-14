import pandas as pd
from langchain.chat_models import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.chains import LLMChain
from tqdm import tqdm

# 设置 LLM
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    temperature=0.7,
    top_p=0.7,
    streaming=False  # 批量处理建议关闭流式
)

# 提示模板
prompt_template = ChatPromptTemplate.from_template("""
你是一个工业领域的问题分类专家，现在需要对以下“永久措施”内容进行标签提取。
要求如下：
1. 输出不超过3个标签；
2. 标签格式为简洁中文词语（如：“电气故障”、“设备老化”、“润滑不足”）；
3. 仅返回标签列表，格式为用逗号隔开的词语，如：标签1,标签2,标签3

问题描述：{question}
""")

chain = LLMChain(llm=llm, prompt=prompt_template)

# 读取 Excel
df = pd.read_excel(r"D:\study_project\84\图谱库语料\打标签2.xlsx")  # 请替换为你的路径
question_list = df["永久措施"].astype(str).tolist()

# 存储标签
labels = []

# 使用 tqdm 显示进度条
for question in tqdm(question_list, desc="标签生成中"):
    try:
        response = chain.run(question)
        labels.append(response.strip())
    except Exception as e:
        labels.append("")

# 将标签列添加到原始 DataFrame 中
df["永久措施标签"] = labels

# 保存新 Excel 文件
df.to_excel("output_with_tags.xlsx", index=False)
print("标签已保存到 output_with_tags.xlsx")
