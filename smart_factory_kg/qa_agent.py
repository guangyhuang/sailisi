import requests
from neo4j import GraphDatabase
import os
import json
import re

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "idt123456"

QWEN_API_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_API_KEY = "sk-331a65ea7dc84825966d3b985929c771"


class KGQA:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        self.labels = self.get_labels()  # 获取数据库中的所有标签
        self.relationships = self.get_relationships()  # 获取所有关系类型
        self.properties = self.get_properties()  # 获取所有属性

    def get_labels(self):
        """获取数据库中的所有节点标签"""
        with self.driver.session() as session:
            result = session.run("CALL db.labels()")
            return [record["label"] for record in result]

    def get_relationships(self):
        """获取数据库中的所有关系类型"""
        with self.driver.session() as session:
            result = session.run("CALL db.relationshipTypes()")
            return [record["relationshipType"] for record in result]

    def get_properties(self):
        """获取数据库中的所有属性名"""
        with self.driver.session() as session:
            result = session.run("CALL db.propertyKeys()")
            return [record["propertyKey"] for record in result]

    def generate_cypher(self, question):
        # 构建包含数据库schema的提示
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

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {QWEN_API_KEY}"
        }

        data = {
            "model": "qwen-turbo",
            "input": {
                "prompt": prompt
            }
        }

        try:
            session = requests.Session()
            session.trust_env = False

            response = session.post(
                QWEN_API_URL,
                headers=headers,
                json=data,
                timeout=15
            )
            response.raise_for_status()
            result = response.json()

            # 调试：打印完整API响应
            print("API响应:", json.dumps(result, indent=2, ensure_ascii=False))

            if 'output' in result and 'text' in result['output']:
                cypher_query = result['output']['text'].strip()

                # 清理可能的代码块标记
                if cypher_query.startswith("```") and cypher_query.endswith("```"):
                    cypher_query = cypher_query[3:-3].strip()
                    if cypher_query.lower().startswith("cypher"):
                        cypher_query = cypher_query[6:].strip()

                return cypher_query
            else:
                raise Exception(f"API返回格式错误: {json.dumps(result, indent=2)}")

        except requests.exceptions.RequestException as e:
            print(f"连接API失败: {e}")
            raise
        except Exception as e:
            print(f"处理API响应失败: {e}")
            raise

    def query_neo4j(self, cypher_query):
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query)
                # 直接返回字典形式的结果
                return [record.data() for record in result]
        except Exception as e:
            print(f"Neo4j查询失败: {e}")
            return [{"error": str(e)}]

    def answer(self, question):
        try:
            cypher = self.generate_cypher(question)
            print(f"[生成的Cypher查询] {cypher}")
            results = self.query_neo4j(cypher)

            # 如果结果为空，尝试备用查询
            if not results:
                print("查询无结果，尝试备用查询...")
                # 提取查询中的主要概念
                match = re.search(r'\(.*?:(.*?)\)', cypher)
                if match:
                    label = match.group(1)
                    # 尝试更通用的查询
                    backup_query = f"MATCH (n) WHERE n.name CONTAINS '{label}' RETURN n LIMIT 10"
                    print(f"[备用查询] {backup_query}")
                    results = self.query_neo4j(backup_query)

            # 格式化结果（直接使用返回的字典）
            if results:
                # 简化结果展示
                simplified_results = []
                for record in results:
                    # 如果返回的是节点对象
                    if 'n' in record and isinstance(record['n'], dict):
                        node_data = record['n']
                        simplified_results.append({
                            "name": node_data.get('name', '未知名称'),
                            "type": node_data.get('type', '未知类型'),
                            "id": node_data.get('id', '未知ID'),
                            "其他属性": {k: v for k, v in node_data.items() if k not in ['name', 'type', 'id']}
                        })
                    else:
                        # 直接添加其他类型的结果
                        simplified_results.append(record)
                return simplified_results
            else:
                return "没有找到相关信息"

        except Exception as e:
            return f"处理问题时出错: {e}"


if __name__ == "__main__":
    os.environ['NO_PROXY'] = 'dashscope.aliyuncs.com'

    qa = KGQA()

    # 打印数据库schema以便调试
    print("数据库标签:", qa.labels)
    print("关系类型:", qa.relationships)
    print("属性名:", qa.properties)
    print("\n知识图谱问答系统已启动，输入 '退出' 或 'exit' 结束程序")

    # 添加循环以持续回答问题
    while True:
        try:
            question = input("\n请输入您的问题：")

            # 检查退出命令
            if question.lower() in ['退出', 'exit', 'quit']:
                print("感谢使用，再见！")
                break

            # 处理空输入
            if not question.strip():
                print("问题不能为空，请重新输入")
                continue

            answer = qa.answer(question)
            print("\n查询结果：")
            print(json.dumps(answer, indent=2, ensure_ascii=False))

        except KeyboardInterrupt:
            print("\n程序被中断，正在退出...")
            break
        except Exception as e:
            print(f"发生错误: {e}")
            # 继续运行而不是退出
            print("请尝试其他问题")