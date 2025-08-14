# -*- coding: utf-8 -*-
import pandas as pd
import json
import re
from py2neo import Graph, Node, Relationship

# 连接 Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))

# 读取 Excel
df = pd.read_excel("../data/台账_根本原因三元组.xlsx")

# 清洗含```json或异常引号的JSON文本
def clean_json_string(raw_str):
    if not isinstance(raw_str, str):
        return None
    cleaned = re.sub(r"```json|```", "", raw_str, flags=re.IGNORECASE).strip()
    cleaned = cleaned.replace('""', '"').replace('\\"', '"')
    return cleaned if cleaned else None

# 遍历每一行
for index, row in df.iterrows():
    part_name = str(row.get("零部件名称", "")).strip()
    phenomenon = str(row.get("故障现象", "")).strip()
    fault_type = str(row.get("故障类型", "")).strip()
    cause_json = row.get("原因分析", "")

    # 如果核心字段为空则跳过
    if not part_name or not phenomenon or not fault_type:
        continue

    # 创建并合并节点
    part_node = Node("零部件", 名称=part_name)
    graph.merge(part_node, "零部件", "名称")

    phenomenon_node = Node("故障现象", 名称=phenomenon)
    graph.merge(phenomenon_node, "故障现象", "名称")

    fault_type_node = Node("故障类型", 名称=fault_type)
    graph.merge(fault_type_node, "故障类型", "名称")

    # 创建链式关系
    graph.merge(Relationship(part_node, "表现为", phenomenon_node))
    graph.merge(Relationship(phenomenon_node, "存在", fault_type_node))

    # 根本原因处理
    try:
        cleaned_json = clean_json_string(cause_json)
        if not cleaned_json:
            continue
        cause_triples = json.loads(cleaned_json)

        if isinstance(cause_triples, list):
            for triple in cause_triples:
                head = triple.get("head", "").strip()
                relation = triple.get("relation", "").strip()
                tail = triple.get("tail", "").strip()

                if not head or not relation or not tail:
                    continue

                head_node = Node("材料或工艺", 名称=head)
                tail_node = Node("异常或原因", 名称=tail)
                graph.merge(head_node, "材料或工艺", "名称")
                graph.merge(tail_node, "异常或原因", "名称")

                graph.merge(Relationship(fault_type_node, "根本原因是", head_node))
                graph.merge(Relationship(head_node, relation, tail_node))

    except Exception as e:
        print(f"第{index}行解析原因分析失败：{e}")
