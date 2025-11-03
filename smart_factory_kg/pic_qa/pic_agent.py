# -*- coding: utf-8 -*-
r"""
FastAPI 接口：基于 Chroma 文本向量库的问答（RAG）。
- /ask：从文本向量库检索、用大模型生成答案；返回答案与来源，并附带“原始图像”（base64 data URL）
"""

import os
import io
import re
import time
import base64
import hashlib
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

import numpy as np
from PIL import Image
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import chromadb
from sentence_transformers import SentenceTransformer

# 多模态 LLM（OpenAI 兼容）
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# ---------------- 配置（可通过环境变量覆盖） ----------------
DEFAULT_MODEL = os.getenv("VL_MODEL", "deepseek-ai/DeepSeek-V3")  # 传入时会自动切成纯文本模型用于回答
DEFAULT_BASE_URL = os.getenv("OPENAI_COMPAT_BASE_URL", "https://api.siliconflow.cn/v1")
DEFAULT_API_KEY = os.getenv("SILICONFLOW_API_KEY", 'sk-bfqyraeeuqfhkikwlqhqvuszlbdouyeppwvjtsexsalkpmzh')  # 建议用环境变量
DEFAULT_ST_MODEL = os.getenv("ST_MODEL", r"D:\code\sailisi\bge-m3")
DEFAULT_CLIP_PATH = os.getenv("CLIP_MODEL_PATH", r"D:\code\sailisi\models\clip-vit-large-patch14")  # 未使用，但保留
DEFAULT_PERSIST = os.getenv("CHROMA_DIR", "./llm_chroma_data")
DEFAULT_TEXT_COL = os.getenv("CHROMA_TEXT_COL", "text_vec")
DEFAULT_IMG_COL = os.getenv("CHROMA_IMG_COL", "image_vec")  # 未使用，但保留
DEFAULT_STORAGE = os.getenv("STORAGE_DIR", "./uploaded_images")

# ---------------- FastAPI 初始化 & CORS ----------------
app = FastAPI(title="LLM RAG API (Ask only)", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产建议改成你的前端域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件（可选：前端想直接访问原图时使用；问答仅用到 abs_path->base64，不依赖此目录，但保留不影响）
Path(DEFAULT_STORAGE).mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=DEFAULT_STORAGE), name="static")

# ---------------- 工具函数 ----------------
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}

def _to_data_url_bytes(raw: bytes, suffix: str) -> str:
    mime = {
        "jpg":"image/jpeg","jpeg":"image/jpeg","png":"image/png",
        "webp":"image/webp","bmp":"image/bmp","tiff":"image/tiff"
    }.get(suffix.lower().lstrip("."), "application/octet-stream")
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{b64}"

def _file_to_data_url(path: Path) -> str:
    return _to_data_url_bytes(path.read_bytes(), path.suffix)

# ---------------- LLM 相关 ----------------
SYSTEM_PROMPT_RAG = (
    "你是一名严格的检索问答助手。只根据提供的检索片段回答用户问题；片段不足则答“不确定”。\n"
    "输出规则：\n"
    "1）必须只输出一行中文句子（不得换行/列点/代码块）,要有主语。\n"
    "2）不得遗漏片段中的关键要点词，保持原词（如“辅导”等）。\n"
    "3）不添加片段之外的内容。"
)

def build_llm(model: str, base_url: str, api_key: str, top_p: float, temperature: float, max_tokens: int) -> ChatOpenAI:
    """
    不自建 httpx.Client；显式 top_p；兼容 openai_api_base / base_url 两种字段。
    """
    kwargs = dict(
        model=model,
        openai_api_key=api_key,
        openai_api_base=base_url,   # 若你的 langchain_openai 新版不认，再走 except 分支
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,                # 显式传递
    )
    try:
        return ChatOpenAI(**kwargs)
    except TypeError:
        # 兼容部分版本用 base_url
        kwargs.pop("openai_api_base", None)
        kwargs["base_url"] = base_url
        return ChatOpenAI(**kwargs)

def _choose_answer_model(model_name: str | None) -> str:
    """
    问答阶段优先选纯文本大模型；如果传入的是 VL 视觉模型，就自动替换为文本模型。
    """
    fallback_text_model = "Qwen/Qwen2.5-7B-Instruct"
    if not model_name:
        return fallback_text_model
    upper = model_name.upper()
    if "VL" in upper or "VISION" in upper:
        return fallback_text_model
    return model_name

def llm_answer(llm: ChatOpenAI, question: str, contexts: List[Dict[str, Any]]) -> str:
    blocks = []
    print(contexts)
    for i, c in enumerate(contexts, 1):
        md = c.get("meta", {})
        title = md.get("rel_path") or md.get("abs_path") or md.get("image_id", f"doc{i}")
        snippet = (c.get("text") or "")[:2000]
        blocks.append(f"[Source #{i}] {title}\n{snippet}\n")
    ctx_prompt = "\n".join(blocks) if blocks else "（无检索片段）"

    msgs = [
        SystemMessage(content=SYSTEM_PROMPT_RAG),
        HumanMessage(content=f"问题：{question}\n\n检索片段：\n{ctx_prompt}\n\n请按上面的输出规则，仅输出一行答案。")
    ]

    last_err = None
    for attempt in range(3):
        try:
            resp = llm.invoke(msgs)
            return (resp.content or "").strip()
        except Exception as e:
            last_err = e
            print(f"[LLM_ANSWER_ERROR][attempt {attempt+1}] {repr(e)}")
            time.sleep(1.2 * (attempt + 1))
    return f"[ERROR] {last_err}"

# ---------------- Embedders（仅用于检索编码） ----------------
_embedder_cache: Dict[str, SentenceTransformer] = {}

def get_text_embedder(model_name: str) -> SentenceTransformer:
    if model_name not in _embedder_cache:
        _embedder_cache[model_name] = SentenceTransformer(model_name)
    return _embedder_cache[model_name]

def text_to_vec(text_model: SentenceTransformer, text: str) -> List[float]:
    return text_model.encode([text or "空"], normalize_embeddings=True)[0].tolist()

# ---------------- Chroma ----------------
_collection_cache: Dict[str, Dict[str, Any]] = {}

def get_chroma_collections(persist_dir: str, text_col: str, img_col: str):
    key = f"{Path(persist_dir).resolve()}::{text_col}::{img_col}"
    if key in _collection_cache:
        return _collection_cache[key]["client"], _collection_cache[key]["text"], _collection_cache[key]["img"]
    Path(persist_dir).mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=persist_dir)
    tcol = client.get_or_create_collection(text_col)
    icol = client.get_or_create_collection(img_col)
    _collection_cache[key] = {"client": client, "text": tcol, "img": icol}
    return client, tcol, icol

# ---------------- Pydantic Schemas（仅问答） ----------------
class AskRequest(BaseModel):
    question: str
    k: int = 3
    persist_dir: str = DEFAULT_PERSIST
    collection_text: str = DEFAULT_TEXT_COL
    st_model: str = DEFAULT_ST_MODEL
    # LLM
    model: str = DEFAULT_MODEL
    base_url: str = DEFAULT_BASE_URL
    api_key: Optional[str] = DEFAULT_API_KEY
    top_p: float = 0.7
    temperature: float = 0.0
    max_tokens: int = 1024

class AskResponse(BaseModel):
    question: str
    answer: str
    hits: List[Dict[str, Any]]  # 每条包含 id/score/meta/doc/image_data_url(原图)

# ---------------- 路由（仅问答） ----------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ask", response_model=AskResponse)
def ask_endpoint(req: AskRequest):
    """检索问答：从文本向量库召回，LLM 生成答案，并返回带原图的来源。"""
    if not (req.api_key or DEFAULT_API_KEY):
        raise HTTPException(400, "缺少 API Key（AskRequest.api_key 或环境变量 SILICONFLOW_API_KEY）")

    # 连接向量库 & embedder
    client = chromadb.PersistentClient(path=req.persist_dir)
    tcol = client.get_or_create_collection(req.collection_text)

    # 空库保护
    try:
        if hasattr(tcol, "count") and tcol.count() == 0:
            raise HTTPException(400, f"集合 `{req.collection_text}` 为空，请先准备好向量数据。")
    except Exception:
        pass

    text_model = get_text_embedder(req.st_model)

    # 编码查询并检索
    qvec = text_model.encode([req.question], normalize_embeddings=True)[0].tolist()
    print(qvec)
    qr = tcol.query(
        query_embeddings=[qvec],
        n_results=max(1, req.k),
        include=["documents", "metadatas", "distances"]  # 注意：不能写 "ids"
    )

    hits = []
    if qr and qr.get("ids") and qr["ids"][0]:
        ids = qr["ids"][0]
        docs = qr.get("documents", [[]])[0]
        metas = qr.get("metadatas", [[]])[0]
        dists = qr.get("distances", [[]])[0]
        for i, _id in enumerate(ids):
            meta = metas[i] if i < len(metas) else {}
            doc  = docs[i] if i < len(docs) else ""
            abs_path = meta.get("abs_path")
            # 组装“原始图像” data URL（不重编码）
            image_data_url = None
            try:
                if abs_path and Path(abs_path).exists():
                    image_data_url = _file_to_data_url(Path(abs_path))
            except Exception:
                image_data_url = None
            hits.append({
                "rank": i + 1,
                "id": _id,
                "score": float(dists[i]) if i < len(dists) else 0.0,
                "meta": meta,
                "doc": doc,
                "image_data_url": image_data_url,  # 前端直接 <img src=... />
            })

    # LLM 作答
    ans_model = _choose_answer_model(req.model)
    llm = build_llm(ans_model, req.base_url, req.api_key or DEFAULT_API_KEY, req.top_p, req.temperature, req.max_tokens)
    answer = llm_answer(llm, req.question, [{"text": h["doc"], "meta": h["meta"]} for h in hits])

    # —— 单行化（把所有换行收敛为中文分号）
    if isinstance(answer, str):
        answer = re.sub(r"\s*\n+\s*", "；", answer).strip("； ").strip()

    return AskResponse(question=req.question, answer=answer, hits=hits)

# 兼容你前端使用的 /api/ask 路径
app.add_api_route("/api/ask", ask_endpoint, methods=["POST"], response_model=AskResponse)

if __name__ == "__main__":
    import uvicorn
    # 生产环境把 reload=False；开发可以 True 便于热重载
    uvicorn.run(
        "pic_qwen_V:app",           # 如果文件名不是 app.py，改成 "文件名:app"
        host="127.0.0.1",
        port=8000,
        reload=True
    )
