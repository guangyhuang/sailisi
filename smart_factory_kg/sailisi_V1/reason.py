# -*- coding: utf-8 -*-
import pandas as pd
import re
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# step 1: 读取数据
data = pd.read_excel('../data/台账1.xlsx')

# step 2: 提取关键列
data = data[[
    '供应商代码', '供应商名称', '零部件件号', '零部件名称', '外观颜色',
    '发现区域', '发生频次', '故障类型', '故障现象', '问题等级', '批次编号', '故障数量', '故障比例(%)',
    '问题描述', 'D2-问题描述', '临时措施', '原因分析', '永久措施'
]]

# step 3: 初始化大模型
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    streaming=False,
    top_p=0.7
)

system_prompt = (
    "你是一位专注于汽车供应链质量管理的专家。我将提供一段故障的原因分析，请你仅基于其中的‘根本原因’部分，提取可用于构建知识图谱的三元组。\n"
    "请严格遵循以下规则：\n"
    "1. 仅处理‘根本原因’段落，不涉及其他部分内容；\n"
    "2. 每条输入文本最多提取一组三元组（即只输出一个‘实体-关系-实体’结构）；\n"
    "3. 输出格式为‘实体1-关系-实体2’，其中：实体1应为具体物体或部件，实体2为异常描述（如“硬度超标”）；关系为动词，如“存在”“导致”；\n"
    "4. 最终输出为 Python 中的列表形式，列表中每个元素为一个字典，字段包括 head、relation 和 tail，不需要使用 JSON 格式；\n"
    "5. 示例1：文本为“设备定位销磨损导致加工位置度超差”，应输出为：\n"
    "[{{'head': '设备定位销磨损', 'relation': '导致', 'tail': '加工位置度超差'}}]\n"
    "6. 示例2：文本为“胶水固化参数设置错误”，应输出为：\n"
    "[{{'head': '胶水', 'relation': '存在', 'tail': '固化参数设置错误'}}]"
)


prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{text}")
])
chain = prompt | llm

# step 4: 创建新列用于保存三元组
data["根本原因三元组"] = ""

# step 5: 遍历每一行数据
for idx, row in data.iterrows():
    raw_text = str(row.get("原因分析", ""))
    if not raw_text.strip():
        continue

    # 提取“根本原因”部分
    match = re.search(r'根本原因[:：]?\s*(.+?)(?:\\n|[\n。；]|流出原因|$)', raw_text)
    root_cause = match.group(1).strip() if match else ""

    if not root_cause:
        continue

    print(f"\n🔍 第{idx+1}条 根本原因文本：{root_cause}")

    try:
        response = chain.invoke({"text": root_cause})
        triple_text = response.content.strip()
        print("📩 大模型返回三元组：", triple_text)

        # 写入新列
        data.at[idx, "根本原因三元组"] = triple_text

    except Exception as e:
        print(f"⚠️ 第{idx+1}条调用失败：{e}")
        data.at[idx, "根本原因三元组"] = "调用失败"

# step 6: 保存为新的 Excel 文件
output_path = "../data/台账1_根本原因三元组.xlsx"
data.to_excel(output_path, index=False)
print(f"\n✅ 已保存新文件至：{output_path}")
