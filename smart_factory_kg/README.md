# 智能工厂知识图谱模板

## 环境要求
- Python 3.8+
- Neo4j Desktop 或 Neo4j Server
- 安装依赖：`pip install -r requirements.txt`

## 步骤
1. 启动Neo4j数据库，设置用户名密码
2. 编辑`neo4j_connection.py`填入账号信息
3. 运行`load_data.py`导入知识图谱数据
4. 使用`query_examples.py`测试查询功能

## 示例数据说明
- `entities.csv`: 记录实体（设备、产线、人员）
- `relations.csv`: 记录实体之间的关系（如 属于、操作）
