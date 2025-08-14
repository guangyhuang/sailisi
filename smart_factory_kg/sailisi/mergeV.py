from py2neo import Graph, NodeMatcher, Relationship
from difflib import SequenceMatcher
from collections import defaultdict

# 建立 Neo4j 连接
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))
matcher = NodeMatcher(graph)

# 相似度计算
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# 检测名称字段
def detect_possible_name_fields(graph, top_k=5):
    labels = [r["label"] for r in graph.run("CALL db.labels()").data()]
    field_count = defaultdict(int)
    for label in labels:
        nodes = graph.run(f"MATCH (n:`{label}`) RETURN n LIMIT 100").data()
        for row in nodes:
            node = row["n"]
            for key in node.keys():
                field_count[key] += 1
    name_like = {k: v for k, v in field_count.items() if any(c in k.lower() for c in ["名", "称", "name"])}
    sorted_fields = sorted(name_like.items(), key=lambda x: x[1], reverse=True)
    print(f"识别出的候选名称字段：{sorted_fields[:top_k]}")
    return [f[0] for f in sorted_fields[:top_k]]

# 获取节点的名称
def get_node_name(node, candidate_fields):
    for field in candidate_fields:
        name = node.get(field)
        if isinstance(name, str) and name.strip():
            return name.strip()
    return ""

# 属性冲突处理
def detect_and_handle_conflicts(node, merged_nodes, strategy="keep_first"):
    print(f"🔍 正在处理节点({node.identity})：{get_node_name(node, candidate_fields)}")
    for other in merged_nodes:
        if other.identity == node.identity:
            continue
        for key in other.keys():
            val_main = node.get(key)
            val_other = other.get(key)
            if not val_main and val_other:
                node[key] = val_other
                print(f"  ✅ 属性补充：[{key}] <- '{val_other}'")
            elif val_main and val_other and val_main != val_other:
                print(f"  ⚠ 冲突字段：[{key}] -> '{val_main}' != '{val_other}'")
                if strategy == "merge_list":
                    node[key] = list(set(val_main if isinstance(val_main, list) else [val_main]) | set([val_other]))
                    print(f"  🔁 合并为列表：{node[key]}")
                elif strategy == "mark_conflict":
                    node[f"conflict_{key}"] = f"原:{val_main} | 新:{val_other}"
                    print(f"  🚩 冲突标记：{node[f'conflict_{key}']}")
                else:
                    print(f"  🛑 保留原值：'{val_main}'")
    graph.push(node)

# 合并节点（保留 node1）
def merge_nodes(node1, node2):
    print(f"🔁 合并节点：({node1.identity}) <- ({node2.identity})")
    for rel in graph.match((node2,), r_type=None):
        rel_type = type(rel).__name__
        if not graph.match((node1, rel.end_node), r_type=rel_type).first():
            graph.create(Relationship(node1, rel_type, rel.end_node))
            print(f"  ➕ 出边：({get_node_name(node1, candidate_fields)}) -[:{rel_type}]-> ({get_node_name(rel.end_node, candidate_fields)})")
    for rel in graph.match((None, node2), r_type=None):
        rel_type = type(rel).__name__
        if not graph.match((rel.start_node, node1), r_type=rel_type).first():
            graph.create(Relationship(rel.start_node, rel_type, node1))
            print(f"  ➕ 入边：({get_node_name(rel.start_node, candidate_fields)}) -[:{rel_type}]-> ({get_node_name(node1, candidate_fields)})")
    graph.delete(node2)
    print("✅ 合并完成\n")

# 实体对齐（包含匹配）
def align_similar_name_entities(label, threshold=0.85, candidate_fields=None):
    print(f"🚀 正在处理标签：{label}")
    nodes = list(matcher.match(label))
    name_to_nodes = {}
    for node in nodes:
        name = get_node_name(node, candidate_fields)
        if name:
            name_to_nodes[node.identity] = (name, node)
    visited = set()
    merged_result = []
    node_list = list(name_to_nodes.values())
    for i in range(len(node_list)):
        name1, node1 = node_list[i]
        if node1.identity in visited:
            continue
        visited.add(node1.identity)
        for j in range(i + 1, len(node_list)):
            name2, node2 = node_list[j]
            if node2.identity in visited:
                continue
            sim = similar(name1, name2)
            if sim >= threshold or name1 in name2 or name2 in name1:
                print(f"🔗 匹配：'{name1}' ~ '{name2}' 相似度={sim:.2f}")
                merge_nodes(node1, node2)
                visited.add(node2.identity)
        merged_result.append(node1)
    return merged_result

# 冲突字段处理
def resolve_conflicts(label, nodes, strategy="keep_first"):
    print(f"🧠 开始冲突处理：{label}")
    for node in nodes:
        others = [n for n in nodes if n.identity != node.identity and get_node_name(n, candidate_fields) == get_node_name(node, candidate_fields)]
        detect_and_handle_conflicts(node, others, strategy=strategy)

# 动态去除语义重复关系
def remove_semantically_redundant_relationships():
    print("\n🚮 开始清理语义重复关系...")
    query = """
    MATCH (a)-[r]->(b)
    RETURN a, b, collect(type(r)) as rel_types, collect(r) as rels
    """
    results = graph.run(query).data()
    removed_count = 0
    for row in results:
        rel_types = row["rel_types"]
        rels = row["rels"]
        if len(rel_types) <= 1:
            continue
        for i in range(len(rel_types)):
            for j in range(i + 1, len(rel_types)):
                r1, r2 = rel_types[i], rel_types[j]
                if r1 == r2:
                    continue
                if r1 in r2:
                    for rel in rels:
                        if type(rel).__name__ == r1:
                            graph.separate(rel)
                            print(f"  🗑 删除：'{r1}' 被包含于 '{r2}'")
                            removed_count += 1
                            break
                elif r2 in r1:
                    for rel in rels:
                        if type(rel).__name__ == r2:
                            graph.separate(rel)
                            print(f"  🗑 删除：'{r2}' 被包含于 '{r1}'")
                            removed_count += 1
                            break
    print(f"✅ 清理完成，共删除：{removed_count} 条")

# 主程序
def run_all(threshold=0.85, strategy="merge_list"):
    global candidate_fields
    candidate_fields = detect_possible_name_fields(graph)
    labels = [r['label'] for r in graph.run("CALL db.labels()").data()]
    print(f"\n📌 标签列表：{labels}")
    for label in labels:
        try:
            print(f"\n=== 处理标签：{label} ===")
            aligned_nodes = align_similar_name_entities(label, threshold, candidate_fields)
            resolve_conflicts(label, aligned_nodes, strategy)
        except Exception as e:
            print(f"❌ 标签 {label} 错误：{e}")
    remove_semantically_redundant_relationships()

if __name__ == "__main__":
    run_all(threshold=0.85, strategy="merge_list")
