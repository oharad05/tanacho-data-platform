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

バリデーション機能:
  - DataMart更新後に「secondary_department='その他'」のvalue>0をチェック
  - 結果はGoogle Cloud Loggingに出力
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import bigquery
from google.cloud import storage

# ============================================================
# バリデーション設定
# ============================================================

VALIDATION_ENABLED = os.environ.get("VALIDATION_ENABLED", "true").lower() == "true"

# バリデーションログ用のlogger
validation_logger = logging.getLogger("datamart-validation")
if not validation_logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    validation_logger.addHandler(handler)
    validation_logger.setLevel(logging.INFO)

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


# ============================================================
# バリデーション関数
# ============================================================

def log_validation_result(result: Dict[str, Any]) -> None:
    """
    バリデーション結果をCloud Loggingに出力

    構造化ログとしてCloud Loggingで検索・フィルタリング可能。
    """
    log_entry = {
        "severity": "ERROR" if result.get("status") == "ERROR" else "INFO",
        "message": _format_validation_message(result),
        "labels": {
            "service": "datamart-validation",
            "validation_type": result.get("validation_type", "unknown"),
            "status": result.get("status", "unknown")
        },
        "jsonPayload": result
    }

    if result.get("status") == "ERROR":
        validation_logger.error(json.dumps(log_entry, ensure_ascii=False))
    else:
        validation_logger.info(json.dumps(log_entry, ensure_ascii=False))


def _format_validation_message(result: Dict[str, Any]) -> str:
    """ログメッセージを整形"""
    status = result.get("status", "UNKNOWN")
    validation_type = result.get("validation_type", "validation")

    if status == "OK":
        return f"[VALIDATION {status}] DataMart: {validation_type} passed"
    else:
        count = result.get("sonota_non_zero_count", 0)
        return f"[VALIDATION {status}] DataMart: {validation_type} failed ({count} records with その他 > 0)"


def validate_sonota_values(bq_client: bigquery.Client) -> Dict[str, Any]:
    """
    secondary_department='その他' の value > 0 をチェック

    Args:
        bq_client: BigQueryクライアント

    Returns:
        検証結果の辞書
    """
    errors = []

    # チェック対象テーブル
    table_id = f"{PROJECT_ID}.corporate_data_dm.management_documents_all_period_all"

    query = f"""
    SELECT
        date,
        main_department,
        main_category,
        secondary_category,
        secondary_department,
        value
    FROM `{table_id}`
    WHERE secondary_department = 'その他'
      AND value > 0
    ORDER BY date DESC, main_department, main_category
    LIMIT 20
    """

    try:
        result = bq_client.query(query).result()
        alerts = []
        for row in result:
            alerts.append({
                "date": str(row.date) if row.date else None,
                "main_department": row.main_department,
                "main_category": row.main_category,
                "secondary_category": row.secondary_category,
                "value": float(row.value) if row.value else 0
            })

        sonota_count = len(alerts)

        if sonota_count > 0:
            errors.append({
                "type": "SONOTA_NON_ZERO",
                "message": f"secondary_department='その他' で value > 0 のレコードが {sonota_count} 件あります",
                "details": {
                    "count": sonota_count,
                    "sample_records": alerts[:10]  # 最大10件のサンプル
                }
            })

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "datamart-validation",
            "validation_type": "sonota_check",
            "table": "management_documents_all_period_all",
            "status": "ERROR" if errors else "OK",
            "sonota_non_zero_count": sonota_count,
            "errors": errors
        }

    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "datamart-validation",
            "validation_type": "sonota_check",
            "status": "ERROR",
            "errors": [{
                "type": "QUERY_ERROR",
                "message": f"その他チェッククエリ実行エラー: {str(e)}"
            }]
        }


def run_datamart_validation(bq_client: bigquery.Client) -> bool:
    """DataMartバリデーションを実行"""
    print("\n" + "=" * 50)
    print("DataMartバリデーションを開始します")
    print("=" * 50)

    # その他チェック
    print("\n[1/1] secondary_department='その他' チェック")
    result = validate_sonota_values(bq_client)
    log_validation_result(result)

    if result.get("status") == "ERROR":
        for error in result.get("errors", []):
            print(f"  ⚠️  {error.get('message')}")
            if error.get("details", {}).get("sample_records"):
                print("  サンプルレコード:")
                for record in error["details"]["sample_records"][:5]:
                    print(f"    - {record.get('date')}: {record.get('main_department')} / "
                          f"{record.get('main_category')} / {record.get('secondary_category')} = {record.get('value')}")
        return False
    else:
        print("  ✅ バリデーションOK: その他チェック passed")
        return True


def main():
    """メイン処理"""
    update_type = os.environ.get("UPDATE_TYPE", "all").lower()
    print(f"更新タイプ: {update_type}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"SQLソース: gs://{GCS_BUCKET}/{SQL_PREFIX}/")
    print(f"バリデーション: {'有効' if VALIDATION_ENABLED else '無効'}")

    bq_client = bigquery.Client(project=PROJECT_ID)

    dwh_success = True
    datamart_success = True
    validation_success = True

    if update_type in ("dwh", "all"):
        dwh_success = update_dwh(bq_client)

    if update_type in ("datamart", "all"):
        datamart_success = update_datamart(bq_client)

        # DataMart更新後にバリデーションを実行
        if datamart_success and VALIDATION_ENABLED:
            validation_success = run_datamart_validation(bq_client)

    print("\n" + "=" * 50)
    if dwh_success and datamart_success:
        if not validation_success:
            print("更新処理は完了しましたが、バリデーションで警告があります")
            print("=" * 50)
            # バリデーション警告は終了コードに影響させない（警告のみ）
            sys.exit(0)
        else:
            print("全ての更新処理が正常に完了しました")
            print("=" * 50)
            sys.exit(0)
    else:
        print("一部の更新処理でエラーが発生しました")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()
