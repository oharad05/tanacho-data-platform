#!/usr/bin/env python3
"""
データ洗いがえ統合スクリプト

使用方法:
    # 1-1: 全データ洗いがえ
    python scripts/manual/data_refresh.py --mode=full

    # 1-2: 指定月のみ洗いがえ
    python scripts/manual/data_refresh.py --mode=monthly --month=202509

    # オプション
    --dry-run           実行せずに処理内容を表示
    --skip-backup       バックアップをスキップ
    --skip-spreadsheet  スプレッドシート連携をスキップ
    --skip-drive        Drive連携をスキップ
"""

import os
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from google.cloud import bigquery, storage

# 固定値設定
PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
BACKUP_DATASET_ID = "corporate_data_bk"
LANDING_BUCKET = "data-platform-landing-prod"
FISCAL_START_YYYYMM = "202409"
FISCAL_START_DATE = "2024-09-01"

# テーブル定義（load_to_bigquery.pyと同期）
TABLE_CONFIG = {
    "sales_target_and_achievements": {"partition_field": "sales_accounting_period"},
    "billing_balance": {"partition_field": "sales_month"},
    "ledger_income": {"partition_field": "slip_date"},
    "department_summary": {"partition_field": "sales_accounting_period"},
    "internal_interest": {"partition_field": "year_month"},
    "profit_plan_term": {"partition_field": "period"},
    "profit_plan_term_nagasaki": {"partition_field": "period"},
    "profit_plan_term_fukuoka": {"partition_field": "period"},
    "ledger_loss": {"partition_field": "accounting_month"},
    "stocks": {"partition_field": "year_month"},
    "ms_allocation_ratio": {"partition_field": "year_month"},
    "customer_sales_target_and_achievements": {"partition_field": "sales_accounting_period"},
    "construction_progress_days_amount": {"partition_field": "property_period"},
    "construction_progress_days_final_date": {"partition_field": "final_billing_sales_date"},
}

# スプレッドシート連携テーブル
SPREADSHEET_TABLES = [
    "ss_gs_sales_profit",
    "ss_inventory_advance_tokyo",
    "ss_inventory_advance_nagasaki",
    "ss_inventory_advance_fukuoka",
    "management_materials_current_month",
]


@dataclass
class RefreshResult:
    """処理結果を格納するクラス"""
    # Step 1: Drive → GCS
    drive_success: List[str] = field(default_factory=list)
    drive_failed: List[str] = field(default_factory=list)

    # Step 1': スプレッドシート
    spreadsheet_success: List[str] = field(default_factory=list)
    spreadsheet_failed: List[str] = field(default_factory=list)

    # Step 2: raw → proceed
    transform_success: List[str] = field(default_factory=list)
    transform_failed: List[str] = field(default_factory=list)

    # Step 5: BigQuery ロード
    bq_success: List[str] = field(default_factory=list)
    bq_failed: List[str] = field(default_factory=list)

    # Step 6: 重複チェック
    duplicates: Dict[str, int] = field(default_factory=dict)

    # Step 7: 差分
    diff_results: Dict[str, Dict] = field(default_factory=dict)


def get_available_months() -> List[str]:
    """GCSのproceed/フォルダから利用可能な年月リストを取得"""
    client = storage.Client()
    bucket = client.bucket(LANDING_BUCKET)
    blobs = bucket.list_blobs(prefix="proceed/")

    months = set()
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) >= 2 and parts[1].isdigit() and len(parts[1]) == 6:
            yyyymm = parts[1]
            if yyyymm >= FISCAL_START_YYYYMM:
                months.add(yyyymm)

    return sorted(list(months))


def get_drive_months() -> List[str]:
    """Driveから取得可能な年月リストを取得"""
    # sync_drive_to_gcs.pyのロジックを使用
    # ここでは簡易的にGCSのraw/から取得
    client = storage.Client()
    bucket = client.bucket(LANDING_BUCKET)
    blobs = bucket.list_blobs(prefix="raw/")

    months = set()
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) >= 2 and parts[1].isdigit() and len(parts[1]) == 6:
            yyyymm = parts[1]
            if yyyymm >= FISCAL_START_YYYYMM:
                months.add(yyyymm)

    return sorted(list(months))


# ===== Step 1: Drive → GCS =====
def sync_drive_to_gcs_wrapper(yyyymm: str, dry_run: bool = False) -> Tuple[bool, str]:
    """Drive → GCS 同期のラッパー"""
    if dry_run:
        print(f"  [DRY-RUN] sync_drive_to_gcs({yyyymm})")
        return True, yyyymm

    try:
        from scripts.manual.sync_drive_to_gcs import sync_drive_to_gcs
        result = sync_drive_to_gcs(yyyymm)
        return True, yyyymm
    except Exception as e:
        print(f"  ❌ Drive同期エラー ({yyyymm}): {e}")
        return False, yyyymm


# ===== Step 1': スプレッドシート → GCS/BQ =====
def sync_spreadsheet_wrapper(dry_run: bool = False) -> Tuple[List[str], List[str]]:
    """スプレッドシート同期のラッパー"""
    if dry_run:
        print(f"  [DRY-RUN] sync_spreadsheet_to_bq()")
        return SPREADSHEET_TABLES, []

    success = []
    failed = []

    try:
        # スプレッドシート連携スクリプトをインポート
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spreadsheet'))
        from sync_spreadsheet_to_bq import (
            load_mapping_from_gcs,
            load_columns_mapping_from_gcs,
            fetch_spreadsheet_data,
            transform_data,
            save_to_gcs,
            load_to_bigquery
        )

        mapping_df = load_mapping_from_gcs()

        for _, row in mapping_df.iterrows():
            table_name = row['en_name']
            try:
                columns_mapping = load_columns_mapping_from_gcs(table_name)
                raw_data = fetch_spreadsheet_data(row['sheet_id'], row['sheet_name'])
                df = transform_data(raw_data, columns_mapping)

                if not df.empty:
                    gcs_path = save_to_gcs(df, table_name)
                    load_to_bigquery(gcs_path, table_name, columns_mapping)
                    success.append(table_name)
                else:
                    failed.append(f"{table_name} (empty)")

            except Exception as e:
                print(f"  ❌ スプレッドシートエラー ({table_name}): {e}")
                failed.append(table_name)

        return success, failed

    except Exception as e:
        print(f"  ❌ スプレッドシート連携全体エラー: {e}")
        return [], ["all"]


# ===== Step 2: raw → proceed =====
def transform_raw_to_proceed_wrapper(yyyymm: str, dry_run: bool = False) -> Tuple[bool, str]:
    """raw → proceed 変換のラッパー"""
    if dry_run:
        print(f"  [DRY-RUN] transform_raw_to_proceed({yyyymm})")
        return True, yyyymm

    try:
        from scripts.manual.transform_raw_to_proceed import process_gcs_files
        process_gcs_files(yyyymm)
        return True, yyyymm
    except Exception as e:
        print(f"  ❌ 変換エラー ({yyyymm}): {e}")
        return False, yyyymm


# ===== Step 3: バックアップ =====
def backup_tables(dry_run: bool = False) -> bool:
    """corporate_data → corporate_data_bk へバックアップ"""
    print("\n" + "=" * 60)
    print("Step 3: バックアップ作成")
    print("=" * 60)

    if dry_run:
        print(f"  [DRY-RUN] {DATASET_ID} → {BACKUP_DATASET_ID}")
        return True

    client = bigquery.Client(project=PROJECT_ID)

    try:
        # バックアップ対象テーブル一覧を取得
        tables = list(client.list_tables(f"{PROJECT_ID}.{DATASET_ID}"))

        for table in tables:
            table_name = table.table_id
            source_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            dest_table = f"{PROJECT_ID}.{BACKUP_DATASET_ID}.{table_name}"

            try:
                # テーブルコピー（上書き）
                job_config = bigquery.CopyJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )

                copy_job = client.copy_table(source_table, dest_table, job_config=job_config)
                copy_job.result()

                print(f"  ✅ {table_name}")

            except Exception as e:
                print(f"  ⚠️ {table_name}: {e}")

        print(f"\n✅ バックアップ完了: {len(tables)} テーブル")
        return True

    except Exception as e:
        print(f"❌ バックアップエラー: {e}")
        return False


# ===== Step 4-5: BigQuery ロード =====
def load_to_bigquery_full(dry_run: bool = False) -> Tuple[List[str], List[str]]:
    """全データをBigQueryにロード（1-1用）"""
    if dry_run:
        print(f"  [DRY-RUN] 全データ削除 → 全ロード")
        return list(TABLE_CONFIG.keys()), []

    try:
        from scripts.manual.load_to_bigquery import process_all_tables
        process_all_tables()
        return list(TABLE_CONFIG.keys()), []
    except Exception as e:
        print(f"  ❌ BigQueryロードエラー: {e}")
        return [], list(TABLE_CONFIG.keys())


def load_to_bigquery_monthly(yyyymm: str, dry_run: bool = False) -> Tuple[List[str], List[str]]:
    """指定月のみBigQueryにロード（1-2用）"""
    if dry_run:
        print(f"  [DRY-RUN] {yyyymm}のデータ削除 → ロード")
        return list(TABLE_CONFIG.keys()), []

    success = []
    failed = []

    client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    for table_name, config in TABLE_CONFIG.items():
        try:
            partition_field = config["partition_field"]
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

            # 年月を日付形式に変換
            year = yyyymm[:4]
            month = yyyymm[4:6]
            target_date = f"{year}-{month}-01"

            # 指定月のデータを削除
            if table_name in ["ledger_income", "ledger_loss"]:
                delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE DATE_TRUNC(DATE({partition_field}), MONTH) = DATE('{target_date}')
                """
            else:
                delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE {partition_field} = DATE('{target_date}')
                """

            query_job = client.query(delete_query)
            query_job.result()

            deleted_rows = query_job.num_dml_affected_rows or 0
            print(f"  🗑️ {table_name}: {deleted_rows}行削除")

            # CSVをロード
            gcs_uri = f"gs://{LANDING_BUCKET}/proceed/{yyyymm}/{table_name}.csv"
            blob = bucket.blob(f"proceed/{yyyymm}/{table_name}.csv")

            if not blob.exists():
                print(f"  ⚠️ {table_name}: CSVファイルなし")
                continue

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                allow_quoted_newlines=True,
            )

            load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
            load_job.result()

            print(f"  ✅ {table_name}: {load_job.output_rows}行ロード")
            success.append(table_name)

        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
            failed.append(table_name)

    return success, failed


# ===== Step 6: 重複チェック =====
def check_duplicates(dry_run: bool = False) -> Dict[str, int]:
    """重複レコードをチェック"""
    print("\n" + "=" * 60)
    print("Step 6: 重複チェック")
    print("=" * 60)

    if dry_run:
        print("  [DRY-RUN] 重複チェックをスキップ")
        return {}

    client = bigquery.Client(project=PROJECT_ID)
    duplicates = {}

    for table_name, config in TABLE_CONFIG.items():
        partition_field = config["partition_field"]
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        try:
            # 重複チェッククエリ（全カラムで重複を検出）
            query = f"""
            SELECT COUNT(*) as total_rows,
                   COUNT(DISTINCT CONCAT(CAST({partition_field} AS STRING), '_', TO_JSON_STRING(t))) as unique_rows
            FROM `{table_id}` t
            WHERE {partition_field} >= '{FISCAL_START_DATE}'
            """

            result = client.query(query).result()
            for row in result:
                diff = row.total_rows - row.unique_rows
                if diff > 0:
                    duplicates[table_name] = diff
                    print(f"  ⚠️ {table_name}: {diff}件の重複")
                else:
                    print(f"  ✅ {table_name}: 重複なし")

        except Exception as e:
            print(f"  ⚠️ {table_name}: チェックエラー - {e}")

    return duplicates


# ===== Step 7: 差分調査 =====
def compare_with_backup(dry_run: bool = False) -> Dict[str, Dict]:
    """バックアップと新データの差分を調査"""
    print("\n" + "=" * 60)
    print("Step 7: バックアップとの差分調査")
    print("=" * 60)

    if dry_run:
        print("  [DRY-RUN] 差分調査をスキップ")
        return {}

    client = bigquery.Client(project=PROJECT_ID)
    diff_results = {}

    for table_name in TABLE_CONFIG.keys():
        try:
            source_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            backup_table = f"{PROJECT_ID}.{BACKUP_DATASET_ID}.{table_name}"

            # 行数比較
            query = f"""
            SELECT
                (SELECT COUNT(*) FROM `{source_table}`) as new_count,
                (SELECT COUNT(*) FROM `{backup_table}`) as backup_count
            """

            result = client.query(query).result()
            for row in result:
                diff = row.new_count - row.backup_count
                diff_results[table_name] = {
                    "new": row.new_count,
                    "backup": row.backup_count,
                    "diff": diff
                }

                if diff != 0:
                    print(f"  📊 {table_name}: {row.backup_count:,} → {row.new_count:,} ({diff:+,})")
                else:
                    print(f"  ✅ {table_name}: {row.new_count:,}行（変更なし）")

        except Exception as e:
            print(f"  ⚠️ {table_name}: 比較エラー - {e}")

    return diff_results


# ===== Step 8: 結果サマリー =====
def print_summary(result: RefreshResult, mode: str, month: Optional[str] = None):
    """結果サマリーを表示"""
    print("\n" + "=" * 60)
    print("Step 8: 実行結果サマリー")
    print("=" * 60)

    print(f"\n📋 実行モード: {mode}" + (f" ({month})" if month else ""))
    print(f"📅 実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ① Drive → GCS
    print(f"\n① Drive → GCS:")
    print(f"   成功: {len(result.drive_success)}件")
    print(f"   失敗: {len(result.drive_failed)}件")
    if result.drive_failed:
        print(f"   失敗ファイル: {', '.join(result.drive_failed)}")

    # ② スプレッドシート
    print(f"\n② スプレッドシート → BigQuery:")
    print(f"   成功: {len(result.spreadsheet_success)}件")
    print(f"   失敗: {len(result.spreadsheet_failed)}件")
    if result.spreadsheet_failed:
        print(f"   失敗シート: {', '.join(result.spreadsheet_failed)}")

    # ③ raw → proceed
    print(f"\n③ GCS raw/ → proceed/:")
    print(f"   成功: {len(result.transform_success)}件")
    print(f"   失敗: {len(result.transform_failed)}件")
    if result.transform_failed:
        print(f"   失敗ファイル: {', '.join(result.transform_failed)}")

    # ④ BigQuery ロード
    print(f"\n④ BigQuery ロード:")
    print(f"   成功: {len(result.bq_success)}件")
    print(f"   失敗: {len(result.bq_failed)}件")
    if result.bq_failed:
        print(f"   失敗テーブル: {', '.join(result.bq_failed)}")

    # ⑤ 重複チェック
    print(f"\n⑤ 重複チェック:")
    if result.duplicates:
        for table, count in result.duplicates.items():
            print(f"   ⚠️ {table}: {count}件の重複")
    else:
        print(f"   ✅ 重複なし")

    # ⑥ 差分調査
    print(f"\n⑥ バックアップとの差分:")
    changes = [t for t, d in result.diff_results.items() if d.get("diff", 0) != 0]
    if changes:
        for table in changes:
            d = result.diff_results[table]
            print(f"   📊 {table}: {d['backup']:,} → {d['new']:,} ({d['diff']:+,})")
    else:
        print(f"   ✅ 差分なし")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="データ洗いがえ統合スクリプト")
    parser.add_argument("--mode", choices=["full", "monthly"], required=True,
                        help="full: 全データ洗いがえ, monthly: 指定月のみ")
    parser.add_argument("--month", type=str, help="対象年月（monthlyモード時必須）例: 202509")
    parser.add_argument("--dry-run", action="store_true", help="実行せずに処理内容を表示")
    parser.add_argument("--skip-backup", action="store_true", help="バックアップをスキップ")
    parser.add_argument("--skip-spreadsheet", action="store_true", help="スプレッドシート連携をスキップ")
    parser.add_argument("--skip-drive", action="store_true", help="Drive連携をスキップ")

    args = parser.parse_args()

    # バリデーション
    if args.mode == "monthly" and not args.month:
        parser.error("--mode=monthly の場合、--month は必須です")

    if args.month and len(args.month) != 6:
        parser.error("--month は YYYYMM 形式で指定してください（例: 202509）")

    # 開始
    print("=" * 60)
    print("データ洗いがえ統合スクリプト")
    print("=" * 60)
    print(f"モード: {'全データ洗いがえ (1-1)' if args.mode == 'full' else '指定月のみ (1-2)'}")
    if args.month:
        print(f"対象年月: {args.month}")
    print(f"Dry-run: {args.dry_run}")
    print(f"Skip backup: {args.skip_backup}")
    print(f"Skip spreadsheet: {args.skip_spreadsheet}")
    print(f"Skip drive: {args.skip_drive}")
    print("=" * 60)

    result = RefreshResult()

    # ===== Step 1: Drive → GCS =====
    if not args.skip_drive:
        print("\n" + "=" * 60)
        print("Step 1: Drive → GCS raw/")
        print("=" * 60)

        if args.mode == "full":
            # 全月を取得（Driveから）
            # ここでは202409から現在月までを生成
            from datetime import datetime
            from dateutil.relativedelta import relativedelta

            start = datetime.strptime(FISCAL_START_YYYYMM, "%Y%m")
            end = datetime.now()
            months = []
            current = start
            while current <= end:
                months.append(current.strftime("%Y%m"))
                current += relativedelta(months=1)
        else:
            months = [args.month]

        for yyyymm in months:
            success, month = sync_drive_to_gcs_wrapper(yyyymm, args.dry_run)
            if success:
                result.drive_success.append(month)
            else:
                result.drive_failed.append(month)
    else:
        print("\n[SKIP] Drive連携をスキップ")

    # ===== Step 1': スプレッドシート =====
    if not args.skip_spreadsheet:
        print("\n" + "=" * 60)
        print("Step 1': スプレッドシート → GCS/BigQuery")
        print("=" * 60)

        success, failed = sync_spreadsheet_wrapper(args.dry_run)
        result.spreadsheet_success = success
        result.spreadsheet_failed = failed
    else:
        print("\n[SKIP] スプレッドシート連携をスキップ")

    # ===== Step 2: raw → proceed =====
    if not args.skip_drive:
        print("\n" + "=" * 60)
        print("Step 2: GCS raw/ → proceed/")
        print("=" * 60)

        if args.mode == "full":
            months = result.drive_success  # Step 1で成功した月
        else:
            months = [args.month]

        for yyyymm in months:
            success, month = transform_raw_to_proceed_wrapper(yyyymm, args.dry_run)
            if success:
                result.transform_success.append(month)
            else:
                result.transform_failed.append(month)

    # ===== Step 3: バックアップ =====
    if not args.skip_backup:
        backup_tables(args.dry_run)
    else:
        print("\n[SKIP] バックアップをスキップ")

    # ===== Step 4-5: BigQuery ロード =====
    print("\n" + "=" * 60)
    print("Step 4-5: BigQuery ロード")
    print("=" * 60)

    if args.mode == "full":
        success, failed = load_to_bigquery_full(args.dry_run)
    else:
        success, failed = load_to_bigquery_monthly(args.month, args.dry_run)

    result.bq_success = success
    result.bq_failed = failed

    # ===== Step 6: 重複チェック =====
    result.duplicates = check_duplicates(args.dry_run)

    # ===== Step 7: 差分調査 =====
    if not args.skip_backup:
        result.diff_results = compare_with_backup(args.dry_run)

    # ===== Step 8: 結果サマリー =====
    print_summary(result, args.mode, args.month)

    # 終了コード
    if result.drive_failed or result.spreadsheet_failed or result.transform_failed or result.bq_failed:
        sys.exit(1)
    else:
        print("\n✅ 全処理が正常に完了しました")
        sys.exit(0)


if __name__ == "__main__":
    main()
