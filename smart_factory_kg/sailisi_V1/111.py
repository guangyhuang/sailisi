# -*- coding: utf-8 -*-
import pandas as pd
import re

# step 1: 读取数据
data = pd.read_excel('../data/台账.xlsx')

# step 2: 提取关键列
data = data[[
    '供应商代码', '供应商名称', '零部件件号', '零部件名称', '外观颜色',
    '发现区域', '发生频次', '故障类型', '故障现象', '问题等级', '批次编号', '故障数量', '故障比例(%)',
    '问题描述', 'D2-问题描述', '临时措施', '原因分析', '永久措施'
]]

# step 3: 提取“根本原因”段文本（原样，不加工）
root_causes = []

for idx, row in data.iterrows():
    raw_text = str(row.get("原因分析", "")).strip()
    if not raw_text:
        root_causes.append("")
        continue

    # 使用正则匹配“根本原因”后紧跟的文本
    match = re.search(r'根本原因[:：]?\s*(.+?)(?:\\n|[\n。；]|流出原因|对策|$)', raw_text)
    root_cause = match.group(1).strip() if match else ""
    root_causes.append(root_cause)

# step 4: 添加为新列
data["根本原因"] = root_causes

# step 5: 保存结果
data.to_excel("../data/台账_已提取根本原因.xlsx", index=False)

print("✅ 根本原因提取完成，已保存为：台账_已提取根本原因.xlsx")
