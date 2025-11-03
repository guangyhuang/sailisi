# -*- coding: utf-8 -*-
"""
ingest_folder_to_chroma.py
批量将 ./pic 文件夹中的图片写入向量知识库（仅向量库，不建图谱）

特性：
- 图片向量：CLIP (openai/clip-vit-large-patch14)
- 文本向量：BGE-M3（对 OCR 文本编码）
- OCR：PaddleOCR（中文优先）
- 两个集合：image_vec（图）、text_vec（文本）
- 去重策略：以文件的相对路径作为 id（可 --overwrite 覆盖）

依赖：
pip install pillow numpy torch torchvision transformers sentence-transformers paddleocr chromadb tqdm

用法：
python ingest_folder_to_chroma.py --pic_dir ./pic --persist_dir ./chroma_data
python ingest_folder_to_chroma.py --pic_dir D:/data/pic --persist_dir D:/data/chroma --overwrite
"""

import os
import time
import argparse
import hashlib
from typing import Dict, List, Tuple

import numpy as np
from PIL import Image
from tqdm import tqdm

import chromadb
from chromadb.config import Settings

import torch
from transformers import CLIPProcessor, CLIPModel
from sentence_transformers import SentenceTransformer
from paddleocr import PaddleOCR


IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


def sha1_of_path(p: str) -> str:
    return hashlib.sha1(p.encode("utf-8")).hexdigest()[:16]


def list_images(root: str) -> List[str]:
    files = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower()
            if ext in IMG_EXTS:
                files.append(os.path.join(dirpath, fn))
    files.sort()
    return files


@torch.no_grad()
def embed_image(pil: Image.Image, clip_model, clip_proc) -> np.ndarray:
    inputs = clip_proc(images=pil, return_tensors="pt")
    feats = clip_model.get_image_features(**inputs)[0].cpu().numpy()
    return feats / (np.linalg.norm(feats) + 1e-9)


def ocr_text(pil: Image.Image, ocr: PaddleOCR) -> str:
    arr = np.array(pil)[:, :, ::-1]  # RGB->BGR
    result = ocr.ocr(arr, cls=True)
    lines = []
    for page in result:
        for item in page:
            lines.append(item[1][0])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pic_dir", type=str, default="./pic", help="图片根目录")
    parser.add_argument("--persist_dir", type=str, default="./chroma_data", help="Chroma 持久化目录")
    parser.add_argument("--collection_image", type=str, default="image_vec", help="图片向量集合名")
    parser.add_argument("--collection_text", type=str, default="text_vec", help="文本向量集合名")
    parser.add_argument("--overwrite", action="store_true", help="同 id 时是否覆盖（默认跳过）")
    parser.add_argument("--add_title", action="store_true", help="（可选）将自动标题一起写入 text_vec（需要额外VLM时再扩展）")
    args = parser.parse_args()

    os.makedirs(args.persist_dir, exist_ok=True)

    # 初始化模型
    print("-> 初始化模型（CLIP / BGE-M3 / PaddleOCR）...")
    clip_name = "openai/clip-vit-large-patch14"
    clip_model = CLIPModel.from_pretrained(clip_name).eval()
    clip_proc  = CLIPProcessor.from_pretrained(clip_name)

    text_model = SentenceTransformer("BAAI/bge-m3")

    ocr = PaddleOCR(use_angle_cls=True, lang="ch")

    # 初始化 Chroma
    client = chromadb.Client(Settings(persist_directory=args.persist_dir))
    img_col  = client.get_or_create_collection(args.collection_image)
    text_col = client.get_or_create_collection(args.collection_text)

    # 收集图片
    files = list_images(args.pic_dir)
    if not files:
        print(f"未在 {args.pic_dir} 发现图片。支持的后缀：{sorted(IMG_EXTS)}")
        return

    print(f"-> 发现 {len(files)} 张图片，开始入库（仅向量库）...")

    added, skipped, failed = 0, 0, 0

    for path in tqdm(files, ncols=100):
        try:
            rel = os.path.relpath(path, args.pic_dir).replace("\\", "/")
            image_id = sha1_of_path(rel)  # 稳定 id（相对路径的 sha1 片段）

            # 重复处理：尝试查询已有同 id（Chroma 无直接 exists API，使用 try/except 简化）
            if not args.overwrite:
                try:
                    # 简单探测：如果能查回则跳过
                    res = img_col.get(ids=[image_id])
                    if res and res.get("ids"):
                        skipped += 1
                        continue
                except Exception:
                    pass  # 不存在则继续

            pil = Image.open(path).convert("RGB")

            # 1) OCR
            text = ocr_text(pil, ocr)

            # 2) 向量
            img_vec = embed_image(pil, clip_model, clip_proc).tolist()
            txt_vec = text_model.encode([text or "空"], normalize_embeddings=True)[0].tolist()

            # 3) metadata（可按需加字段）
            md = {
                "image_id": image_id,
                "rel_path": rel,
                "abs_path": os.path.abspath(path),
                "file_url": f"/static/images/{rel}",  # 若你用FastAPI挂载/静态目录，可用此URL
                "ts": int(time.time()),
            }

            # 4) upsert 到两集合
            if args.overwrite:
                try:
                    img_col.delete(ids=[image_id])
                except Exception:
                    pass
                try:
                    text_col.delete(ids=[image_id])
                except Exception:
                    pass

            img_col.add(ids=[image_id], embeddings=[img_vec], metadatas=[md])
            text_col.add(ids=[image_id], embeddings=[txt_vec], metadatas=[md], documents=[text])

            added += 1

        except Exception as e:
            failed += 1
            print(f"\n[失败] {path}: {e}")

    print(f"\n完成：新增 {added}，跳过 {skipped}，失败 {failed}")
    print(f"Chroma 持久化目录：{args.persist_dir}")
    print(f"集合：image_vec = {args.collection_image}；text_vec = {args.collection_text}")
    print("现在可以用 /ask（文本）或 /ask_image（以图搜图）在你的服务里做检索了。")


if __name__ == "__main__":
    main()
