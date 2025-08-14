from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
import os
import json
import re

# 初始化大模型
llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    streaming=False,
    top_p=0.7
)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "idt123456"

class KGQA:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        self.labels = self.get_labels()
        self.relationships = self.get_relationships()
        self.properties = self.get_properties()

    def get_labels(self):
        with self.driver.session() as session:
            result = session.run("CALL db.labels()")
            return [record["label"] for record in result]

    def get_relationships(self):
        with self.driver.session() as session:
            result = session.run("CALL db.relationshipTypes()")
            return [record["relationshipType"] for record in result]

    def get_properties(self):
        with self.driver.session() as session:
            result = session.run("CALL db.propertyKeys()")
            return [record["propertyKey"] for record in result]

    def generate_cypher(self, question):
        schema_info = f"""
数据库Schema:
节点标签: {', '.join(self.labels)}
关系类型: {', '.join(self.relationships)}
属性名: {', '.join(self.properties)}
"""
        prompt = f"""
你是一个知识图谱问答专家。请根据下面的数据库Schema，将用户的问题转换为Cypher查询。

{schema_info}

问题: "{question}"

只返回Cypher查询，不要任何解释或说明。
"""

        try:
            response = llm.invoke(prompt)
            cypher_query = response.content.strip()

            if cypher_query.startswith("```") and cypher_query.endswith("```"):
                cypher_query = cypher_query[3:-3].strip()
                if cypher_query.lower().startswith("cypher"):
                    cypher_query = cypher_query[6:].strip()

            return cypher_query
        except Exception as e:
            print(f"大模型调用失败: {e}")
            raise

    def query_neo4j(self, cypher_query):
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query)
                return [record.data() for record in result]
        except Exception as e:
            print(f"Neo4j查询失败: {e}")
            return [{"error": str(e)}]

    def answer(self, question):
        try:
            cypher = self.generate_cypher(question)
            print(f"[生成的Cypher查询] {cypher}")
            results = self.query_neo4j(cypher)

            if not results:
                print("查询无结果，尝试备用查询...")
                match = re.search(r'\(.*?:(.*?)\)', cypher)
                if match:
                    label = match.group(1)
                    backup_query = f"MATCH (n) WHERE n.name CONTAINS '{label}' RETURN n LIMIT 10"
                    print(f"[备用查询] {backup_query}")
                    results = self.query_neo4j(backup_query)

            if results:
                simplified_results = []
                for record in results:
                    if 'n' in record and isinstance(record['n'], dict):
                        node_data = record['n']
                        simplified_results.append({
                            "name": node_data.get('name', '未知名称'),
                            "type": node_data.get('type', '未知类型'),
                            "id": node_data.get('id', '未知ID'),
                            "其他属性": {k: v for k, v in node_data.items() if k not in ['name', 'type', 'id']}
                        })
                    else:
                        simplified_results.append(record)
                return simplified_results
            else:
                return "没有找到相关信息"

        except Exception as e:
            return f"处理问题时出错: {e}"


qa = KGQA()
print("数据库标签:", qa.labels)
print("关系类型:", qa.relationships)
print("属性名:", qa.properties)
print("\n知识图谱问答系统已启动，输入 '退出' 或 'exit' 结束程序")


def agent_main(question):

    if question.lower() in ['退出', 'exit', 'quit']:
        print("收到退出指令")
        return None

    if not question.strip():
        raise ValueError("问题不能为空")

    try:
        answer = qa.answer(question)
        print("\n查询结果：")
        print(json.dumps(answer, indent=2, ensure_ascii=False))
        return answer
    except Exception as e:
        print(f"发生错误: {e}")
        raise e


if __name__ == "__main__":
    agent_main()