# -*- coding: utf-8 -*-
import pandas as pd
from py2neo import Graph, Node, Relationship

# 连接 Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))

# 读取 Excel
# df = pd.read_excel("../real_data/QR台账_已提取根本原因.xlsx")
df = pd.read_excel("../real_data/供应商质量问题台账_已提取根本原因.xlsx")

# 遍历每一行
for index, row in df.iterrows():
    try:
        part_name = str(row.get("零件名称", "")).strip()
        phenomenon = str(row.get("故障现象", "")).strip()
        fault_type = str(row.get("故障类别", "")).strip()
        root_cause = str(row.get("根本原因", "")).strip()

        # 必要字段为空则跳过
        if not part_name or not phenomenon or not fault_type or not root_cause:
            continue

        # 创建并合并节点
        part_node = Node("零件名称", 名称=part_name)
        phenomenon_node = Node("故障现象", 名称=phenomenon)
        fault_type_node = Node("故障类别", 名称=fault_type)
        cause_node = Node("根本原因", 名称=root_cause)

        graph.merge(part_node, "零件名称", "名称")
        graph.merge(phenomenon_node, "故障现象", "名称")
        graph.merge(fault_type_node, "故障类别", "名称")
        graph.merge(cause_node, "根本原因", "名称")

        # 创建链式关系
        graph.merge(Relationship(part_node, "表现为", phenomenon_node))
        graph.merge(Relationship(phenomenon_node, "由...导致", fault_type_node))
        graph.merge(Relationship(fault_type_node, "根本原因是", cause_node))

        # 写入成功提示
        print(f"✅ 第{index}行写入成功：{part_name} → {phenomenon} → {fault_type} → {root_cause}")

    except Exception as e:
        print(f"⚠️ 第{index}行处理失败：{e}")