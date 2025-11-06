#!/usr/bin/env python3
"""
CSVファイルの生成だけをテスト
"""
from google.cloud import storage
import sys
sys.path.insert(0, '.')
from transform_profit_plan_term import transform_excel_to_csv

storage_client = storage.Client()
success = transform_excel_to_csv(storage_client, "profit_plan_term", "202509")

if success:
    print("\n✅ CSVファイル生成成功")
    print("内容を確認:")
    import subprocess
    subprocess.run(["gsutil", "cat", "gs://data-platform-landing-prod/proceed/202509/profit_plan_term.csv"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, encoding='utf-8')
    result = subprocess.run(["gsutil", "cat", "gs://data-platform-landing-prod/proceed/202509/profit_plan_term.csv"], stdout=subprocess.PIPE, text=True, encoding='utf-8')
    lines = result.stdout.split('\n')[:5]
    for line in lines:
        print(line)
else:
    print("\n❌ CSVファイル生成失敗")
    sys.exit(1)
