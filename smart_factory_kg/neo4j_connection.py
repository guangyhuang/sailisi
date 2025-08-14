from py2neo import Graph

def connect_neo4j():
    # 替换为你的用户名密码
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))
    return graph
