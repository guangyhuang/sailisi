# -*- coding: utf-8 -*-
"""
serper_google_search_llm_qa.py
Serper.dev (Google Search) + DeepSeek-V3 (SiliconFlow) 联网问答
依赖:
    pip install requests langchain-openai langchain-core tiktoken
"""

import re
import json
import argparse
from typing import List, Dict, Tuple

import requests
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate


# ================== 工具函数 ==================
def clean_text(s: str) -> str:
    import html
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def build_context(snippets: List[Dict], k: int = 6) -> Tuple[str, List[Dict]]:
    chosen = snippets[:k]
    bullets = []
    for i, r in enumerate(chosen, 1):
        bullets.append(
            f"[{i}] {r.get('title') or '（无标题）'}\n"
            f"- 摘要：{(r.get('snippet') or '')[:200]}\n"
            f"- 来源：{r.get('link') or r.get('url') or ''}"
        )
    return "\n\n".join(bullets), chosen


# ================== Serper 搜索 ==================
def search_serper(query: str, api_key: str, max_results: int = 12, use_proxy: bool = False, proxies: Dict = None) -> List[Dict]:
    """
    调用 Serper.dev Google Search API
    - 默认禁用系统代理（trust_env=False + proxies={"http": None, "https": None}）
    - 如需代理，将 use_proxy=True 并传入 proxies={"http": "http://127.0.0.1:7890", "https": ...}
    - 403/非2xx时打印返回体，便于定位（key错误/额度等）
    """
    url = "https://google.serper.dev/search"
    payload = {"q": query, "num": max_results, "gl": "cn", "hl": "zh-cn"}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}

    session = requests.Session()
    session.trust_env = False  # 忽略系统环境中的 HTTP_PROXY/HTTPS_PROXY
    req_proxies = proxies if use_proxy else {"http": None, "https": None}

    try:
        resp = session.post(url, json=payload, headers=headers, timeout=20, proxies=req_proxies)
    except requests.exceptions.ProxyError as e:
        raise RuntimeError(f"[Serper] 代理连接失败：{e}") from e
    except Exception as e:
        raise RuntimeError(f"[Serper] 请求异常：{e}") from e

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"[Serper] 状态码：{resp.status_code}\n返回体：{resp.text}")

    data = resp.json()
    items = data.get("organic") or []
    # 统一字段名：title/link/snippet
    return [{"title": clean_text(i.get("title")),
             "link": i.get("link"),
             "snippet": clean_text(i.get("snippet"))} for i in items]


# ================== DeepSeek-V3 (SiliconFlow) ==================
def get_llm(api_key: str, temperature: float = 0.4, top_p: float = 0.7, streaming: bool = False) -> ChatOpenAI:
    return ChatOpenAI(
        model_name="deepseek-ai/DeepSeek-V3",
        openai_api_key=api_key,                 # 某些版本也支持 api_key=
        base_url="https://api.siliconflow.cn/v1",
        temperature=temperature,
        top_p=top_p,
        streaming=streaming,
    )


SYSTEM_PROMPT = (
    "你是一个可靠的中文检索增强问答助手。"
    "请严格依据给定资料回答问题；若资料没有明确信息，坦诚说明“不确定”。"
    "回答尽量结构化、要点化，并在末尾附上参考编号（如[1][2]）。"
)
USER_PROMPT = """你将看到若干条来自 Google 搜索的资料（编号在中括号中）：

{context}

请基于以上资料，回答用户问题：
【问题】{question}

要求：
1) 只使用资料中的信息，避免主观臆断；
2) 结论给出简明要点列表；
3) 在末尾以参考编号形式标注信息来源（如：[1][3]）。
"""


def answer_with_google_and_llm(question: str, serper_key: str, siliconflow_key: str, k: int = 6,
                               use_proxy: bool = False, proxies: Dict = None) -> Dict:
    # 1) 搜索
    hits = search_serper(question, api_key=serper_key, max_results=max(10, k * 2), use_proxy=use_proxy, proxies=proxies)
    if not hits:
        return {"answer": "未检索到有效结果。", "sources": []}

    # 2) 组装上下文
    context, chosen = build_context(hits, k=k)

    # 3) LLM 回答
    llm = get_llm(siliconflow_key)
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("user", USER_PROMPT),
    ])
    chain = prompt | llm
    resp = chain.invoke({"context": context, "question": question})
    content = (resp.content or "").strip()

    return {"answer": content, "sources": chosen}


# ================== 入口 ==================
def main():
    parser = argparse.ArgumentParser(description="Serper + DeepSeek-V3 联网问答")
    parser.add_argument("--q", type=str, default="什么是知识图谱？", help="问题")
    parser.add_argument("--k", type=int, default=6, help="取前K条结果")
    args = parser.parse_args()

    # ======= 把所有 API/代理配置都写在这里 =======
    SERPER_API_KEY = "9ea635b44f7a98ba18892bf5e078313bc6237660"   # 你提供的 Serper Key
    SILICONFLOW_API_KEY = "sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq"              # 请填你的 SiliconFlow Key

    # 代理开关（大多数情况下保持 False）
    USE_PROXY_FOR_SERPER = False
    PROXIES = {
        "http":  "http://127.0.0.1:7890",   # 如果你有本地代理端口，填这里；否则无视
        "https": "http://127.0.0.1:7890",
    }

    result = answer_with_google_and_llm(
        args.q,
        serper_key=SERPER_API_KEY,
        siliconflow_key=SILICONFLOW_API_KEY,
        k=args.k,
        use_proxy=USE_PROXY_FOR_SERPER,
        proxies=PROXIES
    )

    print("\n=== 答案 ===\n")
    print(result["answer"])
    print("\n=== 参考来源（Top-{}）===\n".format(len(result["sources"])))
    for i, s in enumerate(result["sources"], 1):
        print(f"[{i}] {s.get('title') or '（无标题）'}")
        print(f"    {s.get('link') or ''}")


if __name__ == "__main__":
    main()
