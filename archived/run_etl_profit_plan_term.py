#!/usr/bin/env python3
"""
profit_plan_term専用のETL実行スクリプト
"""
import sys
sys.path.append('gcs_to_bq_service')

from google.cloud import storage, bigquery
from gcs_to_bq_service.main import (
    transform_excel_to_csv,
    delete_partition_data,
    load_csv_to_bigquery,
    update_table_and_column_descriptions
)

def main():
    yyyymm = "202509"
    table_name = "profit_plan_term"

    print("=" * 60)
    print(f"profit_plan_term ETL処理開始")
    print(f"対象年月: {yyyymm}")
    print("=" * 60)

    storage_client = storage.Client()
    bq_client = bigquery.Client(project="data-platform-prod-475201")

    # 1. Excel → CSV変換
    print("\n[1/3] Excel → CSV 変換中...")
    if not transform_excel_to_csv(storage_client, table_name, yyyymm):
        print("❌ 変換失敗")
        return False

    # 2. BigQueryロード
    print("\n[2/3] CSV → BigQuery ロード中...")
    if not load_csv_to_bigquery(bq_client, table_name, yyyymm):
        print("❌ ロード失敗")
        return False

    # 3. メタデータ更新
    print("\n[3/3] メタデータ更新中...")
    update_table_and_column_descriptions(bq_client, storage_client, table_name)

    print("\n" + "=" * 60)
    print("✅ profit_plan_term ETL処理完了")
    print("=" * 60)

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
