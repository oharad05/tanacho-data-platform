#!/usr/bin/env python3
"""
proceed/ → BigQuery 連携スクリプト
CSVファイルをBigQueryテーブルにロード（月次APPENDモード）
テーブルとカラムの説明も自動設定
"""

import os
import sys
import time
import pandas as pd
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# 固定値設定
PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
LANDING_BUCKET = "data-platform-landing-prod"
MAPPING_FILE = "config/mapping/excel_mapping.csv"  # Note: このファイルは現在使用されていない可能性があります
COLUMNS_PATH = "config/columns"

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
    },
    "stocks": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "ms_allocation_ratio": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "ms_department_category": {
        "partition_field": None,  # マスターテーブルなのでパーティション不要
        "clustering_fields": ["department_category_code"]
    }
}

def create_bigquery_client():
    """BigQueryクライアントの作成"""
    client = bigquery.Client(project=PROJECT_ID)
    return client

def load_table_name_mapping() -> Dict[str, str]:
    """
    テーブル名マッピング（日本語→英語）を読み込み

    Returns:
        {英語テーブル名: 日本語名}の辞書
    """
    if not os.path.exists(MAPPING_FILE):
        print(f"⚠️  マッピングファイルが見つかりません: {MAPPING_FILE}")
        return {}

    df = pd.read_csv(MAPPING_FILE)
    mapping = {}
    for _, row in df.iterrows():
        en_name = row['en_name']
        jp_name = row['jp_name'].replace('.xlsx', '')  # 拡張子を除去
        mapping[en_name] = jp_name

    return mapping

def load_column_descriptions(table_name: str) -> Dict[str, str]:
    """
    カラムの説明を読み込み

    Args:
        table_name: テーブル名（英語）

    Returns:
        {英語カラム名: 説明}の辞書
    """
    column_file = f"{COLUMNS_PATH}/{table_name}.csv"
    if not os.path.exists(column_file):
        print(f"⚠️  カラム定義ファイルが見つかりません: {column_file}")
        return {}

    df = pd.read_csv(column_file)
    descriptions = {}
    for _, row in df.iterrows():
        en_name = row['en_name']
        description = row['description']
        descriptions[en_name] = description

    return descriptions

def update_table_and_column_descriptions(
    client: bigquery.Client,
    table_name: str
) -> bool:
    """
    テーブルとカラムの説明を更新

    Args:
        client: BigQueryクライアント
        table_name: テーブル名

    Returns:
        成功時True
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        # テーブルを取得
        table = client.get_table(table_id)

        # テーブル名マッピングを読み込み
        table_mapping = load_table_name_mapping()
        if table_name in table_mapping:
            table.description = table_mapping[table_name]
            print(f"   📝 テーブル説明を設定: {table_mapping[table_name]}")

        # カラムの説明を読み込み
        column_descriptions = load_column_descriptions(table_name)

        # 既存のスキーマを取得し、説明を追加
        new_schema = []
        for field in table.schema:
            description = column_descriptions.get(field.name, field.description)
            new_field = bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=description,
                fields=field.fields
            )
            new_schema.append(new_field)

        table.schema = new_schema

        # テーブルを更新
        table = client.update_table(table, ["description", "schema"])
        print(f"   ✅ {len(column_descriptions)}個のカラム説明を設定")

        return True

    except Exception as e:
        print(f"   ⚠️  説明の更新に失敗: {e}")
        return False

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
    既存データを削除（重複防止）

    CSVには複数月のデータが含まれている場合があるため、
    テーブルごとに適切な削除方法を使用

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

    # テーブル全体を削除するテーブル（CSVに複数月のデータが含まれるため）
    if table_name in ["billing_balance", "profit_plan_term", "ledger_loss"]:
        delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
        print(f"   🗑️  全データ削除中（CSVに複数月のデータが含まれるため）")
    elif table_name in ["ledger_income"]:
        # DATETIME型で、指定月のすべての日付を削除
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE_TRUNC(DATE({partition_field}), MONTH) = '{year}-{month}-01'
        """
        print(f"   🗑️  既存データ削除中: {year}-{month}のすべての日付")
    else:
        # その他のテーブルは指定月のみ削除
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE_TRUNC({partition_field}, MONTH) = '{year}-{month}-01'
        """
        print(f"   🗑️  既存データ削除中: {year}-{month}")

    try:
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

def process_all_tables(yyyymm: str, replace_existing: bool = True):
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
            # テーブルとカラムの説明を更新
            update_table_and_column_descriptions(client, table_name)
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