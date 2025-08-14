from py2neo import Graph, Node, Relationship
import pandas as pd

# 读取Excel
df = pd.read_excel("../data/台账.xlsx")

# 提取字段
kg_data = df[[
    'QR编号', '供应商名称', '供应商代码', '零部件名称', '零部件件号',
    '故障类型', '故障现象', '问题等级', '问题描述',
    '临时措施', '原因分析', '永久措施',
    'SQE责任人', 'SQE专业', 'SQE部门',
    '发起人', '发起部门'
]]

# 连接Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))
graph.delete_all()

# 写入图数据库
for _, row in kg_data.iterrows():
    print(_)
    qr_node = Node("问题事件", qr编号=row["QR编号"], 描述=row["问题描述"], 故障类型=row["故障类型"], 故障现象=row["故障现象"], 问题等级=row["问题等级"],
                   临时措施=row["临时措施"], 原因分析=row["原因分析"], 永久措施=row["永久措施"])
    supplier_node = Node("供应商", 名称=row["供应商名称"], 代码=row["供应商代码"])
    part_node = Node("零部件", 名称=row["零部件名称"], 编号=row["零部件件号"])
    sqe_node = Node("责任人", 姓名=row["SQE责任人"], 专业=row["SQE专业"], 部门=row["SQE部门"])
    sponsor_node = Node("发起人", 姓名=row["发起人"], 部门=row["发起部门"])

    graph.merge(supplier_node, "供应商", "名称")
    graph.merge(part_node, "零部件", "编号")
    graph.merge(sqe_node, "责任人", "姓名")
    graph.merge(sponsor_node, "发起人", "姓名")
    graph.merge(qr_node, "问题事件", "qr编号")

    graph.merge(Relationship(supplier_node, "供货", part_node))
    graph.merge(Relationship(part_node, "发生问题", qr_node))
    graph.merge(Relationship(sqe_node, "负责处理", qr_node))
    graph.merge(Relationship(sponsor_node, "发起", qr_node))
