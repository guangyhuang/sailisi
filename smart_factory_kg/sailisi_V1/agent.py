import time

from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
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

NEO4J_URI = "bolt://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "idt123456"

def _new_driver():
    # 本地Neo4j默认无TLS；Aura或启用TLS时请去掉 encrypted=False 并把 URI 换成 neo4j+s://
    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS),
        encrypted=False,                # ←本地很重要；Aura请删除这行
        connection_timeout=8,
        max_connection_pool_size=10
    )

def _verify_with_retry(driver, retries=5, delay=1.5):
    last = None
    for _ in range(retries):
        try:
            driver.verify_connectivity()
            return
        except Exception as e:
            last = e
            time.sleep(delay)
    raise last

class KGQA:
    def __init__(self):
        self.driver = _new_driver()
        _verify_with_retry(self.driver)  # ←确保真的连上再继续
        self.labels = self.get_labels()
        self.relationships = self.get_relationships()
        self.properties = self.get_properties()

    def close(self):
        try:
            if self.driver:
                self.driver.close()
        except:
            pass

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

    def extract_entities(self, question):
        prompt = f"""
你是实体识别助手，请从下面的问题中提取出与汽车质量知识图谱相关的关键实体（如零部件名称、故障现象等）。

问题：{question}

返回格式：["实体1", "实体2", ...]，不带注释、不带其他内容。
"""
        response = llm.invoke(prompt)
        try:
            entities = json.loads(response.content.strip())
            return entities if isinstance(entities, list) else []
        except:
            return []

    def generate_cypher(self, question, entities=None):
        schema_info = f"""
数据库Schema:
节点标签: {', '.join(self.labels)}
关系类型: {', '.join(self.relationships)}
属性名: {', '.join(self.properties)}
"""
        entity_filter = f"\n实体相关关键词: {', '.join(entities)}" if entities else ""

        prompt = f"""
        你是一个Neo4j知识图谱专家。请把用户问题转换为**可直接运行且鲁棒**的Cypher，遵守以下强规则：

        [数据库Schema]
        {schema_info}
        {entity_filter}

        [强规则]
        - 不要假设固定的节点标签！优先用 (n) 匹配任意标签，再用 keys(n) 判断属性是否存在；
        - 人名/零件名等记录，常见属性名包含：姓名、名称、编号；部门字段可能是：部门、所在部门、部门名称；
        - 查询“属于哪个部门/什么部门”时，用如下安全写法：
          MATCH (n)
          WHERE (('姓名' IN keys(n) AND n.姓名 = '某人') OR ('名称' IN keys(n) AND n.名称 = '某人'))
          RETURN labels(n) AS 标签, coalesce(n.部门, n.所在部门, n.部门名称) AS 部门, coalesce(n.姓名, n.名称) AS 姓名
        - 若问题包含“包含/关键词/模糊”，再使用 CONTAINS 做模糊匹配；
        - 仅输出**一条**Cypher语句，不要任何解释。

        问题: "{question}"
        """
#         prompt = f"""
# 你是一个Neo4j知识图谱专家。请根据下面的数据库Schema和实体关键词，将用户的问题转换为Cypher查询语句。
#
# {schema_info}
# {entity_filter}
#
# 要求：
# - 所有节点属性使用中文字段名（如 名称、描述），不要使用英文字段如 name、type；
# - 当问题包含“包含”“关键词”“模糊”等意思时，请使用 CONTAINS 模糊查询；
# - 推荐查询结构如：MATCH (n:零部件) WHERE n.名称 CONTAINS '关键词'
# - 仅输出Cypher查询语句，不要输出任何说明文字。
#
# 问题: "{question}"
# """
        try:
            response = llm.invoke(prompt)
            cypher_query = response.content.strip()
            if cypher_query.startswith("```") and cypher_query.endswith("```"):
                cypher_query = cypher_query[3:-3].strip()
                if cypher_query.lower().startswith("cypher"):
                    cypher_query = cypher_query[6:].strip()
            return cypher_query
        except Exception as e:
            print(f"大模型生成Cypher失败: {e}")
            raise

    def query_neo4j(self, cypher_query):
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query)
                return [record.data() for record in result]
        except Exception as e:
            print(f"Neo4j查询失败: {e}")
            return [{"error": str(e)}]

    def generate_answer(self, question, records):
        result_text = json.dumps(records, ensure_ascii=False)
        prompt = f"""
你是一个汽车知识图谱问答助手，请根据以下问题和查询结果生成自然语言回答。
注意！！你的回答只能从给你的数据中提取，不能超出数据范围。

问题：{question}
查询结果：{result_text}

回答要求：
- 回答必须是**一句完整的陈述句**；
- 回答中必须有主语；
- 请使用自然语言连贯表达查询结果，不使用项目符号、不列举、不使用列表；
- 回答应简洁、清晰，用中文描述，不带代码、不带说明文字、不带引号。

仅输出一句自然语言回答，不需要其他任何说明。
"""
        response = llm.invoke(prompt)
        return response.content.strip()

    def answer(self, question):
        try:
            entities = self.extract_entities(question)
            cypher = self.generate_cypher(question, entities)
            print(f"[生成的Cypher查询] {cypher}")
            results = self.query_neo4j(cypher)

            if results and isinstance(results, list):
                answer = self.generate_answer(question, results)
                return answer
            else:
                return "没有查询到相关信息。"


        except Exception as e:

            import traceback

            print("发生错误类型：", type(e).__name__)

            print("错误详情：", repr(e))

            traceback.print_exc()

            raise e

qa = KGQA()
print("✅ 知识图谱问答系统已启动，输入 '退出' 结束程序")


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
    try:
        while True:
            question = input("\n请输入您的问题：")
            ans = agent_main(question)
            if ans is None:
                break
    except KeyboardInterrupt:
        print("\n程序被中断，正在退出...")
    finally:
        try:
            qa.close()   # ← 确保释放连接
        except:
            pass

# if __name__ == "__main__":
#
#     while True:
#
#         question = input("\n请输入您的问题：")
#         ans = agent_main(question)

        # print(ans)

    # while True:
    #     try:
    #         question = input("\n请输入您的问题：")
    #         if question.lower() in ['退出', 'exit', 'quit']:
    #             print("感谢使用，再见！")
    #             break
    #         if not question.strip():
    #             print("❗问题不能为空，请重新输入")
    #             continue

    #         answer = qa.answer(question)
    #         print("\n🤖 回答：")
    #         print(answer)

    #     except KeyboardInterrupt:
    #         print("\n程序被中断，正在退出...")
    #         break
    #     except Exception as e:
    #         print(f"❌ 发生错误: {e}")
    #         print("请尝试其他问题")
