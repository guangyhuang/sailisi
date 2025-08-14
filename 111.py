import os
import httpx

# 删除环境变量里的代理配置
for k in ["HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy","NO_PROXY","no_proxy"]:
    os.environ.pop(k, None)

client = httpx.Client(timeout=15.0)  # 老版本不传 proxies
r = client.get("https://api.siliconflow.cn/v1/models",
               headers={"Authorization": "sk-joyiukvlkwsxvbxliqzepnxawudighulwwuddeahlypadkvq"})
print(r.status_code, r.text[:200])
