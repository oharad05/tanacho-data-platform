#!/usr/bin/env python3
"""
Excelファイルの読み込みをデバッグ
"""
import io
import pandas as pd
from google.cloud import storage

LANDING_BUCKET = "data-platform-landing-prod"
table_name = "profit_plan_term"
yyyymm = "202509"

storage_client = storage.Client()
bucket = storage_client.bucket(LANDING_BUCKET)

# Excelファイル読み込み
raw_path = f"raw/{yyyymm}/{table_name}.xlsx"
raw_blob = bucket.blob(raw_path)
excel_bytes = raw_blob.download_as_bytes()

# 東京支店目標103期シートを読み込み
df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='東京支店目標103期')

print(f"データ: {len(df)}行 × {len(df.columns)}列")
print("\n最初の列の情報:")
print(f"列名: {df.columns[0]}")
print(f"データ型: {df[df.columns[0]].dtype}")
print(f"最初の3値:")
for i in range(min(3, len(df))):
    val = df[df.columns[0]].iloc[i]
    print(f"  [{i}] 値={val}, 型={type(val)}, repr={repr(val)}")

print("\nすべての列名:")
for col in df.columns:
    print(f"  - {col}")
