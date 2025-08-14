# -*- coding: utf-8 -*-
import json
import re
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from neo4j import GraphDatabase

# 初始化大模型
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    streaming=False,
    top_p=0.7
)

# 用户输入文本
text_input = input("请输入一段涉及供应链质量管理的文本：\n> ").strip()

# 用户是否手动指定标签
manual_input = input("是否手动指定实体标签？如需指定，请用逗号分隔输入（如：公司,产品,人员），否则直接回车：\n> ").strip()

# 清洗标签
def clean_labels(raw):
    return [label.strip() for label in re.split(r"[，,]", raw) if re.match(r"^[\u4e00-\u9fa5a-zA-Z0-9_]+$", label.strip())]

label_list = clean_labels(manual_input)
extract_labels = len(label_list) == 0  # 是否让大模型抽取labels

# 构造提示词
if extract_labels:
    system_prompt = (
        "你是一个信息抽取专家，专注于供应链质量管理领域。\n"
        "请根据以下规则提取信息：\n"
        "1. 如果输入内容与供应链质量管理无关，请返回：{{\"labels\": [], \"triples\": []}}。\n"
        "2. 如果内容相关，请执行以下任务：\n"
        "  a. 提取不超过3种与供应链质量管理高度相关的核心实体类型（Node labels），如：公司、零部件、责任人等；\n"
        "  b. 提取所有满足要求的知识三元组（head, head_type, relation, tail, tail_type）；\n"
        "  c. 仅保留head_type和tail_type属于Node labels中的三元组。\n"
        "请以如下JSON格式返回（使用中文，禁止添加markdown标记）：\n"
        "{{\n"
        "  \"labels\": [\"标签1\", \"标签2\", \"标签3\"],\n"
        "  \"triples\": [\n"
        "    {{\"head\": \"实体1\", \"head_type\": \"标签1\", \"relation\": \"关系\", \"tail\": \"实体2\", \"tail_type\": \"标签2\"}}\n"
        "  ]\n"
        "}}"
    )
else:
    system_prompt = (
        "你是一个信息抽取专家，专注于供应链质量管理领域。\n"
        "请根据以下规则处理输入内容：\n"
        "1. 如果输入内容与供应链质量管理无关，请返回：{{\"triples\": []}}。\n"
        "2. 如果相关，请从中提取所有满足以下条件的三元组（head, head_type, relation, tail, tail_type）：\n"
        f"   - 三元组的 head_type 和 tail_type 必须属于以下标签之一：{','.join(label_list)}。\n"
        "请以如下JSON格式返回（使用中文，禁止添加markdown标记）：\n"
        "{{\n"
        "  \"triples\": [\n"
        "    {{\"head\": \"实体1\", \"head_type\": \"标签1\", \"relation\": \"关系\", \"tail\": \"实体2\", \"tail_type\": \"标签2\"}}\n"
        "  ]\n"
        "}}"
    )

# 构造 chain
prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{text}")
])
chain = prompt | llm

# 调用大模型
response = chain.invoke({"text": text_input})
print("📩 大模型返回内容：\n", response.content)

if not response.content or response.content.strip() == "":
    print("⚠️ 大模型未返回任何内容，程序终止。")
    exit()

def clean_json_string(text):
    """清除 markdown 包裹"""
    if text.strip().startswith("```"):
        return re.sub(r"```[a-zA-Z]*\n?", "", text).strip()
    return text.strip()


# 尝试解析大模型 JSON 响应
try:
    cleaned_content = clean_json_string(response.content)
    data = json.loads(cleaned_content)
    if extract_labels:
        labels = data.get("labels", [])
        triples = data.get("triples", [])
    else:
        labels = label_list
        triples = data.get("triples", [])
except Exception as e:
    print("❌ 无法解析大模型返回内容。错误信息：", str(e))
    exit()

# 检查是否有三元组满足标签限制
if not extract_labels:
    valid_triples = [
        t for t in triples
        if t.get("head_type") in labels and t.get("tail_type") in labels
    ]
    if not valid_triples:
        print("📭 文本与指定标签内容不匹配，因此未写入数据库。")
        exit()
else:
    valid_triples = triples

# 再次判断是否有效
if not valid_triples:
    print("⚠️ 未提取到任何有效三元组，程序终止。")
    exit()

# 写入提示
print("📡 正在写入知识三元组到 Neo4j 数据库...")
# 初始化 Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "idt123456"))

def create_kg(tx, labels, triples):
    for triple in triples:
        head = triple["head"]
        head_type = triple["head_type"]
        relation = triple["relation"]
        tail = triple["tail"]
        tail_type = triple["tail_type"]
        cypher = (
            f"MERGE (a:`{head_type}` {{name: $head}}) "
            f"MERGE (b:`{tail_type}` {{name: $tail}}) "
            f"MERGE (a)-[:`{relation}`]->(b)"
        )
        tx.run(cypher, head=head, tail=tail)

# 写入数据库
with driver.session() as session:
    session.write_transaction(create_kg, labels, valid_triples)

print("✅ 已成功将知识三元组写入 Neo4j！")
