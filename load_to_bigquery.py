#!/usr/bin/env python3
"""
proceed/ → BigQuery 連携スクリプト
CSVファイルをBigQueryテーブルにロード（月次APPENDモード）
"""

import os
import sys
import time
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# 固定値設定
PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
LANDING_BUCKET = "data-platform-landing-prod"

# テーブル定義とパーティション列のマッピング
TABLE_CONFIG = {
    "sales_target_and_achievements": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["branch_code"]
    },
    "billing_balance": {
        "partition_field": "sales_month", 
        "clustering_fields": ["branch_code"]
    },
    "ledger_income": {
        "partition_field": "slip_date",  # DATE(slip_date)でパーティション
        "clustering_fields": ["classification_type"]
    },
    "department_summary": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["code"]
    },
    "internal_interest": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "profit_plan_term": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "ledger_loss": {
        "partition_field": "slip_date",  # DATE(slip_date)でパーティション
        "clustering_fields": ["classification_type"]
    }
}

def create_bigquery_client():
    """BigQueryクライアントの作成"""
    client = bigquery.Client(project=PROJECT_ID)
    return client

def check_table_exists(client: bigquery.Client, table_name: str) -> bool:
    """テーブルの存在確認"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False

def load_csv_to_bigquery(
    client: bigquery.Client,
    table_name: str,
    gcs_uri: str,
    yyyymm: str
) -> bool:
    """
    CSVファイルをBigQueryにロード
    
    Args:
        client: BigQueryクライアント
        table_name: テーブル名
        gcs_uri: GCS上のCSVファイルURI
        yyyymm: 対象年月
    
    Returns:
        成功時True
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    try:
        # ジョブ設定
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # ヘッダー行をスキップ
            autodetect=False,  # スキーマは定義済み
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 追加モード
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=False,
            max_bad_records=0,  # エラーレコードを許容しない
        )
        
        # ロードジョブの実行
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        
        print(f"   ⏳ ロード開始: {table_name} (Job ID: {load_job.job_id})")
        
        # ジョブの完了を待機（最大5分）
        load_job.result(timeout=300)
        
        # ロード結果の確認
        destination_table = client.get_table(table_id)
        print(f"   ✅ ロード完了: {load_job.output_rows} 行を追加")
        print(f"      総レコード数: {destination_table.num_rows:,} 行")
        
        return True
        
    except GoogleCloudError as e:
        print(f"   ❌ ロードエラー: {e}")
        # エラーの詳細を表示
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"      詳細: {error}")
        return False
    except Exception as e:
        print(f"   ❌ 予期しないエラー: {e}")
        return False

def delete_partition_data(
    client: bigquery.Client,
    table_name: str,
    yyyymm: str
) -> bool:
    """
    指定月のパーティションデータを削除（重複防止）
    
    Args:
        client: BigQueryクライアント
        table_name: テーブル名
        yyyymm: 対象年月（例: 202509）
    
    Returns:
        成功時True
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    partition_field = TABLE_CONFIG[table_name]["partition_field"]
    
    # yyyymmから年月を抽出
    year = yyyymm[:4]
    month = yyyymm[4:6]
    
    # パーティション条件に応じたDELETE文を作成
    if table_name in ["ledger_income", "ledger_loss"]:
        # DATETIME型の場合
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE({partition_field}) = '{year}-{month}-01'
        """
    else:
        # DATE型の場合
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE {partition_field} = '{year}-{month}-01'
        """
    
    try:
        print(f"   🗑️  既存データ削除中: {year}-{month}")
        query_job = client.query(delete_query)
        query_job.result()  # 完了を待機
        
        if query_job.num_dml_affected_rows:
            print(f"      削除: {query_job.num_dml_affected_rows} 行")
        else:
            print(f"      削除対象なし")
        
        return True
        
    except Exception as e:
        print(f"   ⚠️  削除処理スキップ: {e}")
        return True  # 削除失敗してもロードは続行

def process_all_tables(yyyymm: str, replace_existing: bool = False):
    """
    全テーブルのBigQueryロード処理
    
    Args:
        yyyymm: 対象年月（例: 202509）
        replace_existing: 既存データを削除してから追加
    """
    print("=" * 60)
    print(f"proceed/ → BigQuery ロード処理")
    print(f"対象年月: {yyyymm}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print(f"モード: {'REPLACE' if replace_existing else 'APPEND'}")
    print("=" * 60)
    
    # BigQueryクライアント作成
    client = create_bigquery_client()
    
    success_count = 0
    error_count = 0
    
    # 各テーブルを処理
    for table_name in TABLE_CONFIG.keys():
        print(f"\n📊 処理中: {table_name}")
        
        # テーブル存在確認
        if not check_table_exists(client, table_name):
            print(f"   ❌ テーブルが存在しません: {table_name}")
            error_count += 1
            continue
        
        # GCS URI
        gcs_uri = f"gs://{LANDING_BUCKET}/proceed/{yyyymm}/{table_name}.csv"
        
        # CSVファイルの存在確認
        storage_client = storage.Client()
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob_name = f"proceed/{yyyymm}/{table_name}.csv"
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            print(f"   ⚠️  CSVファイルが存在しません: {gcs_uri}")
            error_count += 1
            continue
        
        # 既存データの削除（オプション）
        if replace_existing:
            delete_partition_data(client, table_name, yyyymm)
        
        # BigQueryへロード
        if load_csv_to_bigquery(client, table_name, gcs_uri, yyyymm):
            success_count += 1
        else:
            error_count += 1
    
    print("\n" + "=" * 60)
    print(f"処理完了: 成功 {success_count} / エラー {error_count}")
    print("=" * 60)
    
    # 統計情報の表示
    if success_count > 0:
        print("\n📈 テーブル統計:")
        for table_name in TABLE_CONFIG.keys():
            try:
                table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
                table = client.get_table(table_id)
                print(f"   {table_name}: {table.num_rows:,} 行")
            except:
                pass

def verify_load(table_name: str, yyyymm: str):
    """
    ロード結果を確認
    
    Args:
        table_name: テーブル名
        yyyymm: 対象年月
    """
    client = create_bigquery_client()
    
    year = yyyymm[:4]
    month = yyyymm[4:6]
    partition_field = TABLE_CONFIG[table_name]["partition_field"]
    
    # 件数確認クエリ
    if table_name in ["ledger_income", "ledger_loss"]:
        query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        WHERE DATE({partition_field}) = '{year}-{month}-01'
        """
    else:
        query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        WHERE {partition_field} = '{year}-{month}-01'
        """
    
    result = client.query(query).result()
    for row in result:
        print(f"テーブル: {table_name}")
        print(f"対象月: {year}-{month}")
        print(f"レコード数: {row.row_count:,}")

if __name__ == "__main__":
    # コマンドライン引数
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python load_to_bigquery.py YYYYMM [--replace]")
        print("  例: python load_to_bigquery.py 202509")
        print("  例: python load_to_bigquery.py 202509 --replace")
        sys.exit(1)
    
    yyyymm = sys.argv[1]
    replace_mode = "--replace" in sys.argv
    
    # 実行
    process_all_tables(yyyymm, replace_existing=replace_mode)