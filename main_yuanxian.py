from data_preprocess import data_preprocessing
from main import knowledge_base
from flask_cors import CORS
from flask import Flask, jsonify, request


app = Flask(__name__)
CORS(app)

history = []

@app.route('/api/knowledge', methods=['POST'])
def main():
    try:
        # 对数据做预处理
        data_preprocessing()

        # 从前端获取数据
        data = request.json
        user_input = data.get('query', '')

        if not user_input:
            return jsonify({
                "code": 400,
                "error": "No query text provided"
            })
        
        
        final_output, ans = knowledge_base(user_input, history)

        # 保存历史
        history.append(("User:" + user_input, "AI:" + final_output))

        print("---------------------------------")
        print("final_output:", final_output)

        # print("---------------------------------")
        # print("history:", history)

        # 转换 ans 为可序列化格式
        serialized_ans = [
            {"metadata": doc.metadata, "content": doc.page_content}
            for doc in ans
        ]

        # 返回JSON数据
        return jsonify({
            "code": 200,
            "data": final_output,
            "ans": serialized_ans,
        })
    except Exception as e:
        return jsonify({
            "code": 500,
            "error": str(e)
        })

@app.route('/api/refresh', methods=['POST'])
def refresh_history():

    history.clear()

    return jsonify({
            "code": 200,
        })  # 返回一个空响应
    
if __name__ == "__main__":
    # 启动Flask服务
    app.run(host='0.0.0.0', port=83)