from neo4j_connection import connect_neo4j

graph = connect_neo4j()

# 查询张三操作了哪些设备
query = """
MATCH (p:人员 {name: "张三"})-[:操作]->(d:设备)
RETURN d.name AS 设备名称
"""
result = graph.run(query)
for record in result:
    print(record["设备名称"])
