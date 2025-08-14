# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
from neo4j import GraphDatabase
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
import re

def read_table_headers(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件未找到: {file_path}")
    ext = os.path.splitext(file_path)[-1].lower()
    if ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif ext == '.csv':
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"不支持的文件格式: {ext}")
    return df.columns.tolist()

def clean_json_string(text):
    if text.strip().startswith("```"):
        return re.sub(r"```[a-zA-Z]*\n?", "", text).strip()
    return text.strip()

def extract_kg_from_headers(headers, llm, manual_labels=None):
    if manual_labels:
        labels_text = "、".join(manual_labels)
        system_prompt = (
            "你是一个信息抽取专家。我们需要从表头中提取供应链质量管理相关的结构信息。\n"
            f"以下是用户指定的实体标签：{labels_text}。\n"
            "请从表头中提取所有满足以下条件的三元组（head, head_type, relation, tail, tail_type）：\n"
            "仅保留 head_type 和 tail_type 属于上述标签的三元组。\n"
            "以如下JSON格式返回（不要加markdown符号）：\n"
            "{\n"
            "  \"triples\": [\n"
            "    {\"head\": \"字段A\", \"head_type\": \"标签X\", \"relation\": \"关系\", \"tail\": \"字段B\", \"tail_type\": \"标签Y\"}\n"
            "  ]\n"
            "}"
        )
    else:
        system_prompt = (
            "你是一个信息抽取专家。我们需要对涉及供应链质量管理的知识进行提取，"
            "如果内容不涉及供应链质量管理，就不进行提取。请从用户输入的表头中执行以下任务：\n"
            "1. 识别并输出不超过3种实体标签，记为Node labels；\n"
            "2. 提取所有知识三元组（head, head_type, relation, tail, tail_type）；\n"
            "3. 仅保留head_type和tail_type属于上述3类Node labels的三元组。\n"
            "以如下JSON格式返回（不要添加markdown标记）：\n"
            "{\n"
            "  \"labels\": [\"标签1\", \"标签2\", \"标签3\"],\n"
            "  \"triples\": [\n"
            "    {\"head\": \"字段A\", \"head_type\": \"标签X\", \"relation\": \"关系\", \"tail\": \"字段B\", \"tail_type\": \"标签Y\"}\n"
            "  ]\n"
            "}"
        )

    headers_text = "、".join(headers)
    human_input = f"以下是表格的表头字段：{headers_text}"

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_input)
    ])

    raw_text = clean_json_string(response.content.strip())
    print("🧾 模型原始响应：", raw_text)

    try:
        kg_info = json.loads(raw_text)
        if manual_labels:
            triples = [
                t for t in kg_info.get("triples", [])
                if t["head_type"] in manual_labels and t["tail_type"] in manual_labels
            ]
            if not triples:
                print("📭 表头与指定标签内容不匹配，因此未写入数据库。")
                return {"labels": manual_labels, "triples": []}
            return {"labels": manual_labels, "triples": triples}
        return kg_info
    except json.JSONDecodeError as e:
        raise ValueError(f"模型返回不是合法JSON格式：\n{raw_text}") from e

def build_graph_from_excel_data(path, kg_info, graph):
    df = pd.read_excel(path)
    triples = kg_info.get("triples", [])
    if not triples:
        print("⚠️ 没有可写入的三元组，已跳过数据库写入。")
        return

    with graph.session() as session:
        for _, row in df.iterrows():
            for triple in triples:
                head_col = triple["head"]
                tail_col = triple["tail"]
                relation = triple["relation"]
                head_type = triple["head_type"]
                tail_type = triple["tail_type"]

                head_value = str(row.get(head_col, "")).strip()
                tail_value = str(row.get(tail_col, "")).strip()
                if not head_value or not tail_value:
                    continue

                cypher = f"""
                MERGE (h:{head_type} {{name: $head}})
                MERGE (t:{tail_type} {{name: $tail}})
                MERGE (h)-[:`{relation}`]->(t)
                """
                session.run(cypher, head=head_value, tail=tail_value)
    print("✅ 表格数据已写入知识图谱！")

if __name__ == "__main__":
    path = "../data/供应商质量问题台账.xlsx"
    headers = read_table_headers(path)
    print("表头为：", headers)

    llm = ChatOpenAI(
        model_name="deepseek-ai/DeepSeek-V3",
        openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
        base_url="https://api.siliconflow.cn/v1",
        streaming=False,
        top_p=0.7
    )

    label_input = input("是否手动指定实体标签？如需指定，请用逗号分隔输入（如：公司,产品,人员），否则直接回车：\n> ").strip()
    manual_labels = [l.strip() for l in re.split(r"[，,]", label_input) if l.strip()] if label_input else None

    kg_info = extract_kg_from_headers(headers, llm, manual_labels=manual_labels)
    print("✅ 抽取结构：", json.dumps(kg_info, indent=2, ensure_ascii=False))

    graph = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "idt123456"))

    print("📡 正在写入知识图谱数据...")
    build_graph_from_excel_data(path, kg_info, graph)
    print("🎉 写入流程完成。")
