# server.py
# 启动：uvicorn server:app --host 0.0.0.0 --port 3001 --reload
import os, base64, typing
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from tencentcloud.common import credential
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.ai3d.v20250513 import ai3d_client, models

# ---------- 环境 & SDK ----------
load_dotenv()
SECRET_ID  = os.getenv("TC_SECRET_ID", "").strip()
SECRET_KEY = os.getenv("TC_SECRET_KEY", "").strip()
REGION     = os.getenv("AI3D_REGION", "ap-guangzhou").strip()
ENDPOINT   = os.getenv("AI3D_ENDPOINT", "ai3d.tencentcloudapi.com").strip()
if not SECRET_ID or not SECRET_KEY:
    raise RuntimeError("未读取到 TC_SECRET_ID / TC_SECRET_KEY，请在 .env 中配置。")

cred = credential.Credential(SECRET_ID, SECRET_KEY)
http_profile = HttpProfile(endpoint=ENDPOINT)
client_profile = ClientProfile(httpProfile=http_profile)
client = ai3d_client.Ai3dClient(cred, REGION, client_profile)

# ---------- FastAPI ----------
app = FastAPI(title="Hunyuan 3D Backend", version="1.0.0")

# 放开CORS给前端调用（生产环境请按需限制域名）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

# ---------- Pydantic 模型 ----------
class SubmitJsonBody(BaseModel):
    prompt: typing.Optional[str] = Field(default=None, description="文生3D提示词")
    imageUrl: typing.Optional[str] = Field(default=None, description="图生3D：远程图片URL")
    resultFormat: str = Field(default="GLB", description="GLB/OBJ/STL/USDZ/FBX/MP4")
    enablePBR: bool = Field(default=False, description="是否启用PBR材质")

class SubmitResp(BaseModel):
    jobId: str
    requestId: str | None = None

class File3D(BaseModel):
    Type: str
    Url: str
    PreviewImageUrl: typing.Optional[str] = None

class StatusResp(BaseModel):
    Status: str
    ResultFile3Ds: typing.List[File3D] | None = None
    ErrorMessage: typing.Optional[str] = None
    RequestId: typing.Optional[str] = None

# ---------- 工具 ----------
def _build_submit_request(
    prompt: str | None,
    image_base64: str | None,
    image_url: str | None,
    result_format: str,
    enable_pbr: bool
) -> models.SubmitHunyuanTo3DJobRequest:
    # 互斥校验：prompt 与 image 二选一
    has_text = bool(prompt and prompt.strip())
    has_img  = bool((image_base64 and image_base64.strip()) or (image_url and image_url.strip()))
    if has_text == has_img:
        # 同时提供 / 都没提供 -> 抛错
        raise HTTPException(status_code=400, detail="参数错误：文生3D与图生3D二选一。请仅提供 prompt 或 image(Url/Base64)。")

    req = models.SubmitHunyuanTo3DJobRequest()
    req.ResultFormat = result_format
    req.EnablePBR = enable_pbr

    if has_text:
        req.Prompt = prompt.strip()
    else:
        if image_base64:
            req.ImageBase64 = image_base64.strip()
        elif image_url:
            req.ImageUrl = image_url.strip()
    return req

# ---------- 接口：JSON 提交（文生/图生二选一） ----------
@app.post("/api/ai3d/submit", response_model=SubmitResp)
def submit_job_json(b: SubmitJsonBody):
    try:
        req = _build_submit_request(
            prompt=b.prompt,
            image_base64=None,               # JSON 里不直接传 base64（也可扩展）
            image_url=b.imageUrl,
            result_format=b.resultFormat.upper(),
            enable_pbr=b.enablePBR
        )
        resp = client.SubmitHunyuanTo3DJob(req)
        return SubmitResp(jobId=resp.JobId, requestId=resp.RequestId)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"submit failed: {e}")

# ---------- 接口：表单上传（前端直接传图片文件） ----------
@app.post("/api/ai3d/submit-form", response_model=SubmitResp)
async def submit_job_form(
    prompt: str | None = Form(None),
    image: UploadFile | None = File(None),
    imageUrl: str | None = Form(None),
    resultFormat: str = Form("GLB"),
    enablePBR: bool = Form(False)
):
    try:
        image_b64 = None
        if image is not None:
            # 读取文件并转 base64（仅保留纯base64串，不含 dataURL 头）
            data = await image.read()
            image_b64 = base64.b64encode(data).decode("utf-8")

        req = _build_submit_request(
            prompt=prompt,
            image_base64=image_b64,
            image_url=imageUrl,
            result_format=resultFormat.upper(),
            enable_pbr=enablePBR
        )
        resp = client.SubmitHunyuanTo3DJob(req)
        return SubmitResp(jobId=resp.JobId, requestId=resp.RequestId)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"submit failed: {e}")

# ---------- 接口：查询任务状态 ----------
@app.get("/api/ai3d/status", response_model=StatusResp)
def query_status(jobId: str = Query(..., description="提交时返回的JobId")):
    try:
        req = models.QueryHunyuanTo3DJobRequest()
        req.JobId = jobId
        resp = client.QueryHunyuanTo3DJob(req)
        # 直接把 SDK 响应“抹平”为前端友好结构
        result = []
        if getattr(resp, "ResultFile3Ds", None):
            for f in resp.ResultFile3Ds:
                result.append(File3D(Type=f.Type, Url=f.Url, PreviewImageUrl=getattr(f, "PreviewImageUrl", None)))
        return StatusResp(
            Status=resp.Status,
            ResultFile3Ds=result or None,
            ErrorMessage=getattr(resp, "ErrorMessage", None),
            RequestId=resp.RequestId
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"query failed: {e}")

# ---------- 可选：健康检查 ----------
@app.get("/api/ai3d/health")
def health():
    return {"ok": True, "region": REGION, "endpoint": ENDPOINT}
