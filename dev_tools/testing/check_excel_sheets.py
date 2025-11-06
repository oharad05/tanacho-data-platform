#!/usr/bin/env python3
"""
Excelファイルのシート構成を確認
"""
import io
import pandas as pd
from google.cloud import storage

LANDING_BUCKET = "data-platform-landing-prod"
yyyymm = "202509"

# 確認するファイル
files_to_check = [
    "profit_plan_term",
    "ledger_income",
    "billing_balance",
    "ledger_loss"
]

storage_client = storage.Client()
bucket = storage_client.bucket(LANDING_BUCKET)

for table_name in files_to_check:
    print(f"\n{'='*60}")
    print(f"📄 {table_name}")
    print('='*60)

    raw_path = f"raw/{yyyymm}/{table_name}.xlsx"
    raw_blob = bucket.blob(raw_path)

    if not raw_blob.exists():
        print(f"❌ ファイルが存在しません: {raw_path}")
        continue

    excel_bytes = raw_blob.download_as_bytes()

    try:
        # 全シート名を取得
        xls = pd.ExcelFile(io.BytesIO(excel_bytes))
        sheet_names = xls.sheet_names

        print(f"シート数: {len(sheet_names)}")
        print(f"シート名:")
        for i, sheet in enumerate(sheet_names, 1):
            print(f"  {i}. {sheet}")

        # 各シートの行数を確認
        print(f"\n各シートの行数:")
        total_rows = 0
        for sheet in sheet_names:
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=sheet)
            rows = len(df)
            total_rows += rows
            print(f"  {sheet}: {rows:,}行 × {len(df.columns)}列")

        print(f"\n全シート合計: {total_rows:,}行")

    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
