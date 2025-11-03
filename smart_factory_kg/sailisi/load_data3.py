# -*- coding: utf-8 -*-
import pandas as pd
from py2neo import Graph, Node, Relationship, Subgraph

# 1. 连接 Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))

# 2. 读 Excel
df = pd.read_excel("../data/台账.xlsx")

# 3. 准备容器
nodes, rels = set(), []

for _, row in df.iterrows():
    # 3-1 创建实体节点
    factory   = Node("工厂", 名称=str(row["工厂"]).strip())
    part      = Node("零部件", 名称=str(row["零部件名称"]).strip())
    area      = Node("区域", 名称=str(row["发现区域"]).strip())
    color_val = str(row["外观颜色"]).strip()
    freq_val  = str(row["发生频次"]).strip()

    # 3-2 创建属性节点（颜色 / 频次）
    color = Node("颜色", 值=color_val)
    freq  = Node("频次", 值=freq_val)

    # 3-3 加入集合（自动去重）
    nodes.update([factory, part, area, color, freq])

    # 3-4 创建关系
    rels.append(Relationship(factory, "生产", part))
    rels.append(Relationship(part,   "颜色为", color))
    rels.append(Relationship(part,   "在…发现", area))
    rels.append(Relationship(part,   "发生频次为", freq))

# 4. 一次性写入
subgraph = Subgraph(nodes, rels)
graph.create(subgraph)

print("✅ 知识图谱写入完成！")