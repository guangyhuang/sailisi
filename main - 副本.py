from doc_similarity import doc_initialization, load_docs_from_directory, extract_context_snippet
from langchain.chat_models import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from csv_ag import csv_main
from smart_factory_kg.sailisi_V1.agent import agent_main
from data_preprocess import data_preprocessing
import json
from flask_cors import CORS
from flask import Flask, jsonify, request, Response

app = Flask(__name__)
CORS(app)

history = []


directory_path = "切片"  #替换成你的路径

# 读取文件夹中的所有文件，并构建docs文档
docs = load_docs_from_directory(directory_path)

doc_retriever = doc_initialization(docs = docs)
print("模型初始化完成，数据库创建完成。")

# llm = ChatOpenAI(
#     model_name='qwen3-32b-fp8',
#     temperature=0.1,
#     top_p=0.4,  
#     streaming=True,
#     callbacks=[StreamingStdOutCallbackHandler()],
#     openai_api_key='gpustack_26bab5d79592beb4_6280f57b05b67748122e695b36b37d39',
#     openai_api_base='http://113.214.15.252:18650/v1'
# )

llm = ChatOpenAI(
    model_name="deepseek-ai/DeepSeek-V3",
    openai_api_key="sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq",
    base_url="https://api.siliconflow.cn/v1",
    temperature=0.7,
    top_p=0.7,
    streaming=True  # 开启流式输出
)


def chat_with_llm(prompt):
    response_text = ""

    for chunk in llm.stream(prompt):
        response_text += chunk.content  # 提取内容
    return response_text



def knowledge_base(user_input, history, rag, output, csv_ans):


    template = """
你是由赛力斯自主开发的专业助手，你叫赛赛。请你参考下方提供的相关资料，知识库检索结果和历史对话来尽量完整地回答用户问题。（回答需说明出处）
注意！！你的回答只能从给你的数据中提取，不能超出数据范围。

用户的问题：{user_input}。

知识库检索结果：{knowledge_graph};{csv_ans}

该问题涉及到的相关文档：{ans}。

历史对话如下：{history}。
"""

    prompt = ChatPromptTemplate.from_template(template)
    MAX_HISTORY = 5  # 最多保留5轮对话

    # 在 knowledge_base() 函数中：
    if len(history) > MAX_HISTORY:
        history.pop(0)  # 删除最早一条

    final_prompt = prompt.format(user_input=user_input, ans=rag, history=history, knowledge_graph = output, csv_ans = csv_ans)

    print("\nfinal_prompt:", final_prompt)

    final_ans = chat_with_llm(final_prompt)
    final_ans = final_ans.split("</think>")[-1]

    # 保存历史
    # history.append(("User:" + user_input, "AI:" + final_ans))

    return final_ans



@app.route('/api/knowledge', methods=['POST'])
def main():
    try:
        print("收到请求")
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
        
        def generate():
            message_id = 0
            
            # 开始处理通知
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'start', 'message': f'开始处理查询: {user_input}'}})}\n\n"
            message_id += 1

            # RAG检索
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'rag_start', 'message': '正在检索文档库...'}})}\n\n"
            message_id += 1
            
            intermediate_results = doc_retriever.invoke(user_input)
            ans = [extract_context_snippet(doc) for doc in intermediate_results]
            
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'rag_complete', 'message': '文档库检索完成', 'ans': [{'metadata': doc.metadata, 'content': doc.page_content} for doc in ans]}})}\n\n"
            message_id += 1

            # 知识图谱检索
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'kg_start', 'message': '正在检索知识图谱...'}})}\n\n"
            message_id += 1
            
            output = agent_main(question=user_input)
            
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'kg_complete', 'message': '知识图谱检索完成', 'kg_result': output}})}\n\n"
            message_id += 1

            # CSV检索
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'csv_start', 'message': '正在检索CSV数据...'}})}\n\n"
            message_id += 1
            
            csv_user_input = user_input + "。输出内容放在字典中返回。"
            csv_ans = csv_main(csv_user_input)
            for item in csv_ans:
                for key in item:
                    input_text = item[key]['input']
                    item[key]['input'] = input_text.replace('。输出内容放在字典中返回。', '')
            
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'csv_complete', 'message': 'CSV检索完成', 'csv_result': csv_ans}})}\n\n"
            message_id += 1

            # LLM处理
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'llm_start', 'message': '正在生成回答...'}})}\n\n"
            message_id += 1
            
            import time
            buffer = ""
            last_send_time = time.time()
            
            for chunk in llm.stream(ChatPromptTemplate.from_template("""
你是由赛力斯自主开发的专业助手，你叫赛赛。请你参考下方提供的相关资料，知识库检索结果和历史对话来尽量完整地回答用户问题。（回答需说明出处）
注意！！你的回答只能从给你的数据中提取，不能超出数据范围。

用户的问题：{user_input}。

知识库检索结果：{knowledge_graph};{csv_ans}

该问题涉及到的相关文档：{ans}。

历史对话如下：{history}。
""").format(
                user_input=user_input,
                ans=ans,
                history=history,
                knowledge_graph=output,
                csv_ans=csv_ans
            )):
                content = chunk.content.split("</think>")[-1]
                buffer += content
                
                current_time = time.time()
                if current_time - last_send_time >= 0.2 or len(buffer) > 100:
                    yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'llm_processing', 'message': buffer}})}\n\n"
                    message_id += 1
                    buffer = ""
                    last_send_time = current_time
            
            # 发送剩余缓冲内容
            if buffer:
                yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'llm_processing', 'message': buffer}})}\n\n"
                message_id += 1

            # 最终完成
            yield f"event:message\ndata:{json.dumps({'id': message_id, 'content': {'code': 200, 'stage': 'complete', 'message': '处理完成'}})}\n\n"
            message_id += 1

            # 保存历史
            final_output = "".join([chunk.content for chunk in llm.stream(ChatPromptTemplate.from_template("""
你是由赛力斯自主开发的专业助手，你叫赛赛。请你参考下方提供的相关资料，知识库检索结果和历史对话来尽量完整地回答用户问题。（回答需说明出处）
注意！！你的回答只能从给你的数据中提取，不能超出数据范围。

用户的问题：{user_input}。

知识库检索结果：{knowledge_graph};{csv_ans}

该问题涉及到的相关文档：{ans}。

历史对话如下：{history}。
""").format(
                user_input=user_input,
                ans=ans,
                history=history,
                knowledge_graph=output,
                csv_ans=csv_ans
            ))])
            history.append(("User:" + user_input, "AI:" + final_output))
            print("final_output:", final_output)

        return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        }
    )
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
    app.run(host='0.0.0.0', port=83, debug=True, threaded=True)






# def main():
#     history = []
#     while True:
#         user_input = input("请输入您的问题：")
#         output, ans = knowledge_base(user_input, output="", history=history)

#         history.append(("User:" + user_input, "AI:" + output))
#         print(output)



# if __name__ == '__main__':
#     main()