from flask import Flask, render_template, jsonify
from neo4j_connection import connect_neo4j

app = Flask(__name__)
graph = connect_neo4j()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/graph_data')
def get_graph_data():
    query = """
    MATCH (n)-[r]->(m)
    RETURN n.id AS source, m.id AS target, type(r) AS relation, n.name AS source_name, m.name AS target_name
    """
    results = graph.run(query).data()

    nodes = {}
    links = []
    for record in results:
        sid = record['source']
        tid = record['target']

        if sid not in nodes:
            nodes[sid] = {'id': sid, 'name': record['source_name']}
        if tid not in nodes:
            nodes[tid] = {'id': tid, 'name': record['target_name']}

        links.append({'source': sid, 'target': tid, 'label': record['relation']})

    return jsonify({'nodes': list(nodes.values()), 'links': links})

if __name__ == '__main__':
    app.run(debug=True)
