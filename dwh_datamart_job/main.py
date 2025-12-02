#!/usr/bin/env python3
"""
DWH/DataMart更新ジョブ
=====================
GCSからSQLファイルを読み込み、BigQueryで順次実行するCloud Run Job

使用方法:
  - 環境変数 UPDATE_TYPE で更新タイプを指定
    - "dwh": DWHテーブルのみ更新
    - "datamart": DataMartテーブルのみ更新
    - "all": DWH + DataMart 両方更新（デフォルト）
"""

import os
import sys
from google.cloud import bigquery
from google.cloud import storage

PROJECT_ID = "data-platform-prod-475201"
GCS_BUCKET = "data-platform-landing-prod"
SQL_PREFIX = "sql/split_dwh_dm"

# DWH SQLファイル（実行順序）
DWH_SQL_FILES = [
    "dwh_sales_actual.sql",
    "dwh_sales_actual_prev_year.sql",
    "dwh_sales_target.sql",
    "operating_expenses.sql",
    "non_operating_income.sql",
    "non_operating_expenses.sql",
    "non_operating_expenses_nagasaki.sql",
    "non_operating_expenses_fukuoka.sql",
    "miscellaneous_loss.sql",
    "head_office_expenses.sql",
    "dwh_recurring_profit_target.sql",
    "operating_expenses_target.sql",
    "operating_income_target.sql",
]

# DataMart SQLファイル（実行順序）
DATAMART_SQL_FILES = [
    "aggregated_metrics_all_branches.sql",
    "datamart_management_report_tokyo.sql",
    "datamart_management_report_nagasaki.sql",
    "datamart_management_report_fukuoka.sql",
    "datamart_management_report_all.sql",
    "datamart_management_report_all_for_display.sql",
    "cumulative_management_documents_all_period_all.sql",
    "cumulative_management_documents_all_period_all_for_display.sql",
]


def get_sql_from_gcs(bucket_name: str, blob_path: str) -> str:
    """GCSからSQLファイルを読み込む"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_text()


def execute_sql(bq_client: bigquery.Client, sql: str, description: str) -> bool:
    """BigQueryでSQLを実行"""
    print(f"  実行中: {description}")
    try:
        query_job = bq_client.query(sql)
        query_job.result()  # 完了を待機
        print(f"  ✓ 完了: {description}")
        return True
    except Exception as e:
        print(f"  ✗ エラー: {description}")
        print(f"    {str(e)}")
        return False


def update_dwh(bq_client: bigquery.Client) -> bool:
    """DWHテーブルを更新"""
    print("\n" + "=" * 50)
    print("DWH更新処理を開始します")
    print("=" * 50)

    success_count = 0
    total = len(DWH_SQL_FILES)

    for i, sql_file in enumerate(DWH_SQL_FILES, 1):
        print(f"\n[{i}/{total}] {sql_file}")
        blob_path = f"{SQL_PREFIX}/{sql_file}"

        try:
            sql = get_sql_from_gcs(GCS_BUCKET, blob_path)
            if execute_sql(bq_client, sql, sql_file):
                success_count += 1
        except Exception as e:
            print(f"  ✗ SQLファイル読み込みエラー: {blob_path}")
            print(f"    {str(e)}")

    print(f"\nDWH更新完了: {success_count}/{total} 成功")
    return success_count == total


def update_datamart(bq_client: bigquery.Client) -> bool:
    """DataMartテーブルを更新"""
    print("\n" + "=" * 50)
    print("DataMart更新処理を開始します")
    print("=" * 50)

    success_count = 0
    total = len(DATAMART_SQL_FILES)

    for i, sql_file in enumerate(DATAMART_SQL_FILES, 1):
        print(f"\n[{i}/{total}] {sql_file}")
        blob_path = f"{SQL_PREFIX}/{sql_file}"

        try:
            sql = get_sql_from_gcs(GCS_BUCKET, blob_path)
            if execute_sql(bq_client, sql, sql_file):
                success_count += 1
        except Exception as e:
            print(f"  ✗ SQLファイル読み込みエラー: {blob_path}")
            print(f"    {str(e)}")

    print(f"\nDataMart更新完了: {success_count}/{total} 成功")
    return success_count == total


def main():
    """メイン処理"""
    update_type = os.environ.get("UPDATE_TYPE", "all").lower()
    print(f"更新タイプ: {update_type}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"SQLソース: gs://{GCS_BUCKET}/{SQL_PREFIX}/")

    bq_client = bigquery.Client(project=PROJECT_ID)

    dwh_success = True
    datamart_success = True

    if update_type in ("dwh", "all"):
        dwh_success = update_dwh(bq_client)

    if update_type in ("datamart", "all"):
        datamart_success = update_datamart(bq_client)

    print("\n" + "=" * 50)
    if dwh_success and datamart_success:
        print("全ての更新処理が正常に完了しました")
        print("=" * 50)
        sys.exit(0)
    else:
        print("一部の更新処理でエラーが発生しました")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()
