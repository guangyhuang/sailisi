from py2neo import Graph, NodeMatcher, Relationship
from difflib import SequenceMatcher
from collections import defaultdict

# 建立 Neo4j 连接
graph = Graph("bolt://localhost:7687", auth=("neo4j", "idt123456"))
matcher = NodeMatcher(graph)

# 判断字符串相似度
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# 🔍 动态检测可能的名称字段
def detect_possible_name_fields(graph, top_k=5):
    label_results = graph.run("CALL db.labels()").data()
    labels = [r["label"] for r in label_results]

    field_count = defaultdict(int)

    for label in labels:
        query = f"MATCH (n:`{label}`) RETURN n LIMIT 100"
        results = graph.run(query).data()
        for row in results:
            node = row["n"]
            for key in node.keys():
                field_count[key] += 1

    name_like = {k: v for k, v in field_count.items() if any(c in k.lower() for c in ["名", "称", "name"])}

    sorted_fields = sorted(name_like.items(), key=lambda x: x[1], reverse=True)
    print(f" 识别出的候选名称字段：{sorted_fields[:top_k]}")
    return [f[0] for f in sorted_fields[:top_k]]

# ✨ 从节点中取出名称字段
def get_node_name(node, candidate_fields):
    for field in candidate_fields:
        name = node.get(field)
        if isinstance(name, str) and name.strip():
            return name.strip()
    return ""

# ⚠ 冲突检测与处理
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
                    if isinstance(val_main, list):
                        node[key] = list(set(val_main + [val_other]))
                    else:
                        node[key] = list(set([val_main, val_other]))
                    print(f"  🔁 合并为列表：{node[key]}")
                elif strategy == "mark_conflict":
                    node[f"conflict_{key}"] = f"原:{val_main} | 新:{val_other}"
                    print(f"  🚩 冲突标记：{node[f'conflict_{key}']}")
                else:
                    print(f"  🛑 保留原值：'{val_main}'")
    graph.push(node)

# 🔁 合并两个节点，保留 node1，避免重复关系
def merge_nodes(node1, node2):
    print(f"🔁 同名合并：({node1.identity}) <- ({node2.identity})")

    # 合并出边
    for rel in graph.match((node2,), r_type=None):
        exists = graph.match((node1, rel.end_node), r_type=type(rel).__name__).first()
        if not exists:
            graph.create(Relationship(node1, type(rel).__name__, rel.end_node))
            print(f"  ➕ 创建出边关系：({get_node_name(node1, candidate_fields)}) -[:{type(rel).__name__}]-> ({get_node_name(rel.end_node, candidate_fields)})")
        else:
            print(f"  ⚠ 跳过重复出边：{type(rel).__name__}")

    # 合并入边
    for rel in graph.match((None, node2), r_type=None):
        exists = graph.match((rel.start_node, node1), r_type=type(rel).__name__).first()
        if not exists:
            graph.create(Relationship(rel.start_node, type(rel).__name__, node1))
            print(f"  ➕ 创建入边关系：({get_node_name(rel.start_node, candidate_fields)}) -[:{type(rel).__name__}]-> ({get_node_name(node1, candidate_fields)})")
        else:
            print(f"  ⚠ 跳过重复入边：{type(rel).__name__}")

    graph.delete(node2)
    print("✅ 合并完成\n")

# 第一步：同名实体归并
def align_same_name_entities(label, threshold=0.85, candidate_fields=None):
    print(f"🚀 正在处理标签：{label}")
    nodes = list(matcher.match(label))
    name_to_nodes = {}

    # 抽取所有节点的名称
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
        group = [node1]
        visited.add(node1.identity)

        for j in range(i + 1, len(node_list)):
            name2, node2 = node_list[j]
            if node2.identity in visited:
                continue
            sim = similar(name1, name2)
            if sim >= threshold or name1 in name2 or name2 in name1:
                print(f"🔗 匹配组：'{name1}' ~ '{name2}' 相似度={sim:.2f}")
                merge_nodes(node1, node2)
                visited.add(node2.identity)

        merged_result.append(node1)

    return merged_result

# 第二步：冲突属性处理
def resolve_conflicts(label, nodes, strategy="keep_first"):
    print(f"🧠 开始冲突处理：{label}")
    for node in nodes:
        others = [n for n in nodes if n.identity != node.identity and get_node_name(n, candidate_fields) == get_node_name(node, candidate_fields)]
        detect_and_handle_conflicts(node, others, strategy=strategy)

# 主流程
def run_all(threshold=1.0, strategy="keep_first"):
    global candidate_fields
    candidate_fields = detect_possible_name_fields(graph)

    labels = graph.run("CALL db.labels()").data()
    labels = [r['label'] for r in labels]
    print(f"📌 检测到标签：{labels}")

    for label in labels:
        try:
            print(f"\n=== 处理标签：{label} ===")
            aligned_nodes = align_same_name_entities(label=label, threshold=threshold, candidate_fields=candidate_fields)
            resolve_conflicts(label=label, nodes=aligned_nodes, strategy=strategy)
        except Exception as e:
            print(f"❌ 标签 {label} 处理出错：{e}")

if __name__ == "__main__":
    run_all(threshold=1.0, strategy="merge_list")  # 也可改为 keep_first 或 mark_conflict
