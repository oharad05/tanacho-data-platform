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
  - corporate_dataテーブルの重複チェック
  - 結果はGoogle Cloud Loggingに出力
"""

import os
import sys
import json
import logging
import yaml
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

# バックアップ設定
SOURCE_DATASET = "corporate_data"
BACKUP_DATASET = "corporate_data_bk"

# corporate_dataのテーブル一覧（バックアップ対象）
CORPORATE_DATA_TABLES = [
    "billing_balance",
    "construction_progress_days_amount",
    "construction_progress_days_final_date",
    "customer_sales_target_and_achievements",
    "department_summary",
    "internal_interest",
    "ledger_income",
    "ledger_loss",
    "management_materials_current_month",
    "ms_allocation_ratio",
    "profit_plan_term",
    "profit_plan_term_fukuoka",
    "profit_plan_term_nagasaki",
    "sales_target_and_achievements",
    "ss_gs_sales_profit",
    "ss_inventory_advance_fukuoka",
    "ss_inventory_advance_nagasaki",
    "ss_inventory_advance_tokyo",
    "stocks",
]

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

# ユニークキー定義ファイルのGCSパス
TABLE_UNIQUE_KEYS_GCS_PATH = "config/table_unique_keys.yml"


def load_table_unique_keys() -> Dict[str, Dict]:
    """
    GCSからテーブルのユニークキー定義を読み込む

    Returns:
        テーブル名をキー、設定を値とする辞書
    """
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(TABLE_UNIQUE_KEYS_GCS_PATH)
        yaml_content = blob.download_as_text()
        config = yaml.safe_load(yaml_content)
        return config.get("tables", {})
    except Exception as e:
        print(f"[WARN] ユニークキー定義の読み込みに失敗: {e}")
        return {}


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


def backup_corporate_data(bq_client: bigquery.Client) -> Dict[str, int]:
    """
    corporate_dataのテーブルをcorporate_data_bkにコピーし、件数を返す

    Returns:
        テーブル名をキー、件数を値とする辞書
    """
    print("\n" + "=" * 50)
    print("corporate_data → corporate_data_bk バックアップ開始")
    print("=" * 50)

    row_counts = {}

    for table_name in CORPORATE_DATA_TABLES:
        source_table = f"{PROJECT_ID}.{SOURCE_DATASET}.{table_name}"
        backup_table = f"{PROJECT_ID}.{BACKUP_DATASET}.{table_name}"

        try:
            # テーブルをコピー（上書き）
            copy_sql = f"""
            CREATE OR REPLACE TABLE `{backup_table}` AS
            SELECT * FROM `{source_table}`
            """
            bq_client.query(copy_sql).result()

            # 件数を取得
            count_sql = f"SELECT COUNT(*) as cnt FROM `{source_table}`"
            result = bq_client.query(count_sql).result()
            count = list(result)[0].cnt
            row_counts[table_name] = count

            print(f"  ✓ {table_name}: {count:,} 件")

        except Exception as e:
            print(f"  ✗ {table_name}: エラー - {str(e)}")
            row_counts[table_name] = -1  # エラーを示す

    print(f"\nバックアップ完了: {len([v for v in row_counts.values() if v >= 0])}/{len(CORPORATE_DATA_TABLES)} テーブル")
    return row_counts


def compare_row_counts(bq_client: bigquery.Client, backup_counts: Dict[str, int]) -> None:
    """
    バックアップ時の件数と現在の件数を比較し、ログに出力

    Args:
        bq_client: BigQueryクライアント
        backup_counts: バックアップ時の件数
    """
    print("\n" + "=" * 50)
    print("テーブル件数比較（バックアップ vs 現在）")
    print("=" * 50)

    comparison_results = []

    for table_name in CORPORATE_DATA_TABLES:
        backup_count = backup_counts.get(table_name, -1)

        try:
            # 現在の件数を取得
            source_table = f"{PROJECT_ID}.{SOURCE_DATASET}.{table_name}"
            count_sql = f"SELECT COUNT(*) as cnt FROM `{source_table}`"
            result = bq_client.query(count_sql).result()
            current_count = list(result)[0].cnt

            diff = current_count - backup_count if backup_count >= 0 else None
            diff_str = f"{diff:+,}" if diff is not None else "N/A"

            comparison_results.append({
                "table": table_name,
                "backup_count": backup_count,
                "current_count": current_count,
                "diff": diff
            })

            # 差分がある場合は目立つように表示
            if diff and diff != 0:
                print(f"  📊 {table_name}: {backup_count:,} → {current_count:,} ({diff_str})")
            else:
                print(f"  {table_name}: {backup_count:,} → {current_count:,} ({diff_str})")

        except Exception as e:
            print(f"  ✗ {table_name}: 件数取得エラー - {str(e)}")
            comparison_results.append({
                "table": table_name,
                "backup_count": backup_count,
                "current_count": -1,
                "diff": None,
                "error": str(e)
            })

    # 構造化ログとして出力
    log_entry = {
        "severity": "INFO",
        "message": "テーブル件数比較結果",
        "labels": {
            "service": "dwh-datamart-update",
            "operation": "row_count_comparison"
        },
        "jsonPayload": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "comparison": comparison_results
        }
    }
    validation_logger.info(json.dumps(log_entry, ensure_ascii=False))


def check_duplicates(bq_client: bigquery.Client) -> Dict[str, Any]:
    """
    corporate_dataテーブルの重複をチェック

    Args:
        bq_client: BigQueryクライアント

    Returns:
        重複チェック結果の辞書
    """
    print("\n" + "=" * 50)
    print("corporate_data 重複チェック開始")
    print("=" * 50)

    # ユニークキー定義を読み込み
    table_configs = load_table_unique_keys()

    if not table_configs:
        print("  ⚠️  ユニークキー定義が見つかりません")
        return {"status": "SKIPPED", "reason": "no_config"}

    duplicate_results = []
    has_duplicates = False

    for table_name in CORPORATE_DATA_TABLES:
        if table_name not in table_configs:
            print(f"  ⚠️  {table_name}: ユニークキー未定義（スキップ）")
            continue

        config = table_configs[table_name]
        unique_keys = config.get("unique_keys", [])

        if not unique_keys:
            print(f"  ⚠️  {table_name}: ユニークキーが空（スキップ）")
            continue

        try:
            # ユニークキーを結合してCONCATで重複チェック
            table_id = f"{PROJECT_ID}.{SOURCE_DATASET}.{table_name}"

            # カラムの存在確認
            table = bq_client.get_table(table_id)
            existing_columns = {field.name for field in table.schema}
            valid_keys = [k for k in unique_keys if k in existing_columns]

            if len(valid_keys) != len(unique_keys):
                missing = set(unique_keys) - set(valid_keys)
                print(f"  ⚠️  {table_name}: カラム不足 {missing}（スキップ）")
                continue

            # 重複チェッククエリを生成
            key_concat = ", '-', ".join([f"CAST({k} AS STRING)" for k in valid_keys])
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT CONCAT({key_concat})) as unique_keys,
                COUNT(*) - COUNT(DISTINCT CONCAT({key_concat})) as duplicates
            FROM `{table_id}`
            """

            result = bq_client.query(query).result()
            row = list(result)[0]

            total_rows = row.total_rows
            unique_count = row.unique_keys
            duplicates = row.duplicates

            result_entry = {
                "table": table_name,
                "total_rows": total_rows,
                "unique_keys": unique_count,
                "duplicates": duplicates,
                "unique_key_columns": valid_keys
            }
            duplicate_results.append(result_entry)

            if duplicates > 0:
                has_duplicates = True
                print(f"  ❌ {table_name}: {total_rows:,}行 / ユニーク{unique_count:,} / 重複{duplicates:,}")
            else:
                print(f"  ✅ {table_name}: {total_rows:,}行 / 重複なし")

        except Exception as e:
            print(f"  ✗ {table_name}: チェックエラー - {str(e)}")
            duplicate_results.append({
                "table": table_name,
                "error": str(e)
            })

    # 結果サマリー
    tables_with_duplicates = [r for r in duplicate_results if r.get("duplicates", 0) > 0]
    print(f"\n重複チェック完了: {len(duplicate_results)}テーブル中 {len(tables_with_duplicates)}テーブルに重複あり")

    # 構造化ログとして出力
    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "dwh-datamart-update",
        "validation_type": "duplicate_check",
        "status": "ERROR" if has_duplicates else "OK",
        "tables_checked": len(duplicate_results),
        "tables_with_duplicates": len(tables_with_duplicates),
        "details": duplicate_results
    }

    log_entry = {
        "severity": "ERROR" if has_duplicates else "INFO",
        "message": f"重複チェック結果: {len(tables_with_duplicates)}テーブルに重複あり" if has_duplicates else "重複チェック結果: 重複なし",
        "labels": {
            "service": "dwh-datamart-update",
            "validation_type": "duplicate_check",
            "status": result["status"]
        },
        "jsonPayload": result
    }
    validation_logger.info(json.dumps(log_entry, ensure_ascii=False))

    return result


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
    enable_backup = os.environ.get("ENABLE_BACKUP", "true").lower() == "true"
    print(f"更新タイプ: {update_type}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"SQLソース: gs://{GCS_BUCKET}/{SQL_PREFIX}/")
    print(f"バリデーション: {'有効' if VALIDATION_ENABLED else '無効'}")
    print(f"バックアップ: {'有効' if enable_backup else '無効'}")

    bq_client = bigquery.Client(project=PROJECT_ID)

    dwh_success = True
    datamart_success = True
    validation_success = True
    backup_counts = {}

    # Step 1: corporate_data → corporate_data_bk へバックアップ
    if enable_backup:
        backup_counts = backup_corporate_data(bq_client)

    # Step 2: DWH更新
    if update_type in ("dwh", "all"):
        dwh_success = update_dwh(bq_client)

    # Step 3: DataMart更新
    if update_type in ("datamart", "all"):
        datamart_success = update_datamart(bq_client)

        # DataMart更新後にバリデーションを実行
        if datamart_success and VALIDATION_ENABLED:
            validation_success = run_datamart_validation(bq_client)

    # Step 4: 件数比較（バックアップが有効な場合）
    if enable_backup and backup_counts:
        compare_row_counts(bq_client, backup_counts)

    # Step 5: 重複チェック
    if VALIDATION_ENABLED:
        duplicate_result = check_duplicates(bq_client)
        if duplicate_result.get("status") == "ERROR":
            print("\n⚠️  重複が検出されました（警告のみ）")

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
