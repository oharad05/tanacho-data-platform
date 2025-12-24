#!/usr/bin/env python3
"""
proceed/ → BigQuery 連携スクリプト
CSVファイルをBigQueryテーブルにロード

デフォルト動作:
- 2024/9以降のデータを全て削除
- GCSのproceed/フォルダにある全年月のCSVをロード

テーブルとカラムの説明も自動設定

テーブルタイプ:
- 単月型: 各CSVがその月のデータのみ含む → 全CSVをそのままロード
- 累積型: 各CSVが全期間のデータを含む → 全CSVを結合してキー毎に最新フォルダを優先
"""

import os
import sys
import time
import io
import pandas as pd
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# 固定値設定
PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
LANDING_BUCKET = "data-platform-landing-prod"
MAPPING_FILE = "google-drive/config/mapping/excel_mapping.csv"  # Note: このファイルは現在使用されていない可能性があります
COLUMNS_PATH = "google-drive/config/columns"

# スプレッドシート連携用設定
SPREADSHEET_PROCEED_PATH = "spreadsheet/proceed"
SPREADSHEET_COLUMNS_PATH = "spreadsheet/config/columns"
SPREADSHEET_TABLE_PREFIX = "ss_"

# ゼロ日付変換設定（0000/00/00をnullに変換）
ZERO_DATE_CONFIG = {
    "construction_progress_days_amount": [
        "contract_date",
        "construction_start_date",
        "construction_end_date",
        "completion_date",
    ],
}

# スプレッドシートテーブル設定（パーティションなし）
SPREADSHEET_TABLE_CONFIG = {
    "gs_sales_profit": {
        "description": "GS売上利益",
    },
    "inventory_advance_tokyo": {
        "description": "東京在庫前払",
    },
    "inventory_advance_nagasaki": {
        "description": "長崎在庫前払",
    },
    "inventory_advance_fukuoka": {
        "description": "福岡在庫前払",
    },
}

# 期首（データ開始日）
FISCAL_START_YYYYMM = "202409"  # 2024年9月
FISCAL_START_DATE = "2024-09-01"

# 累積型テーブルの定義（各CSVが全期間のデータを含むテーブル）
# キー毎に最新フォルダ（max(source_folder)）のデータを優先してロード
CUMULATIVE_TABLE_CONFIG = {
    "billing_balance": {
        # ソース: 3_請求残高一覧表（月間）.xlsx
        "unique_keys": ["sales_month", "branch_code", "branch_name"],
    },
    "profit_plan_term": {
        # ソース: 12_損益5期目標.xlsx（東京支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "profit_plan_term_nagasaki": {
        # ソース: 12_損益5期目標.xlsx（長崎支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "profit_plan_term_fukuoka": {
        # ソース: 12_損益5期目標.xlsx（福岡支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "ms_allocation_ratio": {
        # ソース: 10_案分比率マスタ.xlsx
        "unique_keys": ["year_month", "branch", "department", "category"],
    },
    "construction_progress_days_amount": {
        # ソース: 工事進捗日数金額.xlsx
        # property_period（パーティション列）も含めてユニークキーとする
        "unique_keys": ["property_period", "branch_code", "staff_code", "property_number", "customer_code", "contract_date"],
    },
    "stocks": {
        # ソース: 9_在庫.xlsx
        # 年月・支店・部署・カテゴリでユニーク
        "unique_keys": ["year_month", "branch", "department", "category"],
    },
    "construction_progress_days_final_date": {
        # ソース: 工事進捗日数最終日.xlsx
        # 最終請求売上日・物件番号・物件データ区分でユニーク
        "unique_keys": ["final_billing_sales_date", "property_number", "property_data_classification"],
    },
}

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
    "profit_plan_term_nagasaki": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "profit_plan_term_fukuoka": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "ledger_loss": {
        "partition_field": "accounting_month",  # accounting_monthで削除判定
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
    "customer_sales_target_and_achievements": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["branch_code"]
    },
    "construction_progress_days_amount": {
        "partition_field": "property_period",
        "clustering_fields": ["branch_code"]
    },
    "construction_progress_days_final_date": {
        "partition_field": "final_billing_sales_date",
        "clustering_fields": []
    },
}

def create_bigquery_client():
    """BigQueryクライアントの作成"""
    client = bigquery.Client(project=PROJECT_ID)
    return client

def get_available_months_from_gcs() -> list:
    """GCSのgoogle-drive/proceed/フォルダから利用可能な年月リストを取得（2024/9以降）"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)
    blobs = bucket.list_blobs(prefix="google-drive/proceed/")

    months = set()
    for blob in blobs:
        # google-drive/proceed/202409/xxx.csv のような形式からyyyymmを抽出
        parts = blob.name.split("/")
        if len(parts) >= 3 and parts[2].isdigit() and len(parts[2]) == 6:
            yyyymm = parts[2]
            # 2024/9以降のみ対象
            if yyyymm >= FISCAL_START_YYYYMM:
                months.add(yyyymm)

    return sorted(list(months))

def delete_all_data_since_fiscal_start(
    client: bigquery.Client,
    table_name: str
) -> bool:
    """
    2024年9月以降のデータを全て削除（冪等性保証）

    Args:
        client: BigQueryクライアント
        table_name: テーブル名

    Returns:
        成功時True
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    partition_field = TABLE_CONFIG[table_name]["partition_field"]

    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE {partition_field} >= '{FISCAL_START_DATE}'
    """

    try:
        print(f"   🗑️  既存データ削除中: {FISCAL_START_DATE}以降")
        query_job = client.query(delete_query)
        query_job.result()

        if query_job.num_dml_affected_rows:
            print(f"      削除: {query_job.num_dml_affected_rows} 行")
        else:
            print(f"      削除対象なし")

        return True

    except Exception as e:
        print(f"   ⚠️  削除処理スキップ: {e}")
        return True

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

def delete_partition_data_by_csv(
    client: bigquery.Client,
    table_name: str,
    gcs_uri: str
) -> bool:
    """
    CSVファイルの実際のデータ月に基づいて既存データを削除（重複防止）

    フォルダ名ではなく、CSVに含まれる実際のデータ月を読み取って削除する。
    これにより、フォルダ名とデータ月がずれている場合でも正しく動作する。

    Args:
        client: BigQueryクライアント
        table_name: テーブル名
        gcs_uri: GCS上のCSVファイルURI

    Returns:
        成功時True
    """
    import io

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    partition_field = TABLE_CONFIG[table_name]["partition_field"]

    try:
        # GCSからCSVを読み込んでデータ月を取得
        storage_client = storage.Client()

        # gs://bucket/path の形式からバケット名とパスを抽出
        gcs_path = gcs_uri.replace("gs://", "")
        bucket_name = gcs_path.split("/")[0]
        blob_path = "/".join(gcs_path.split("/")[1:])

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        csv_content = blob.download_as_bytes()

        df = pd.read_csv(io.BytesIO(csv_content))

        if partition_field not in df.columns:
            print(f"   ⚠️  パーティションフィールド '{partition_field}' がCSVに存在しません")
            return False

        # CSVに含まれるユニークな月を取得
        unique_months = df[partition_field].dropna().unique()

        if len(unique_months) == 0:
            print(f"   ⚠️  CSVにデータがありません")
            return True

        print(f"   🗑️  CSVに含まれるデータ月: {list(unique_months)}")

        total_deleted = 0

        for month_value in unique_months:
            # 日付型かどうかで処理を分ける
            if table_name in ["ledger_income", "ledger_loss"]:
                # DATETIME型の場合、月の範囲で削除
                # month_valueは "2024-09-01" のような形式を想定
                delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE DATE_TRUNC(DATE({partition_field}), MONTH) = DATE('{month_value}')
                """
            else:
                # DATE型の場合
                delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE {partition_field} = DATE('{month_value}')
                """

            query_job = client.query(delete_query)
            query_job.result()

            if query_job.num_dml_affected_rows:
                total_deleted += query_job.num_dml_affected_rows

        if total_deleted > 0:
            print(f"      削除: {total_deleted} 行")
        else:
            print(f"      削除対象なし")

        return True

    except Exception as e:
        print(f"   ⚠️  削除処理エラー: {e}")
        import traceback
        traceback.print_exc()
        return True  # 削除失敗してもロードは続行


def process_cumulative_table(
    client: bigquery.Client,
    storage_client: storage.Client,
    table_name: str,
    target_months: List[str]
) -> bool:
    """
    累積型テーブルのロード処理

    全月のCSVを読み込み、source_folderカラムを追加して結合。
    キー毎にmax(source_folder)のデータを優先して重複を解消。

    Args:
        client: BigQueryクライアント
        storage_client: GCSクライアント
        table_name: テーブル名
        target_months: 対象年月リスト

    Returns:
        成功時True
    """
    print(f"\n📊 処理中（累積型）: {table_name}")

    config = CUMULATIVE_TABLE_CONFIG[table_name]
    unique_keys = config["unique_keys"]
    bucket = storage_client.bucket(LANDING_BUCKET)

    # 全月のCSVを読み込み、source_folderカラムを追加
    all_dfs = []
    for yyyymm in target_months:
        blob = bucket.blob(f"google-drive/proceed/{yyyymm}/{table_name}.csv")
        if blob.exists():
            csv_content = blob.download_as_string().decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_content))
            df["source_folder"] = int(yyyymm)
            all_dfs.append(df)
            print(f"   📁 {yyyymm}: {len(df)}行")

    if not all_dfs:
        print(f"   ⚠️  CSVファイルが見つかりません")
        return False

    # 全データを結合
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"   📊 結合後: {len(combined_df)}行")

    # キー毎にmax(source_folder)でフィルタ（最新フォルダを優先）
    idx = combined_df.groupby(unique_keys)["source_folder"].transform("max") == combined_df["source_folder"]
    deduped_df = combined_df[idx].drop_duplicates(subset=unique_keys, keep="last").reset_index(drop=True)
    print(f"   ✨ 重複除去後: {len(deduped_df)}行")

    # ゼロ日付をnullに変換（BigQueryにロードする前に変換）
    if table_name in ZERO_DATE_CONFIG:
        zero_date_patterns = ['0000/00/00', '0000-00-00', '0000/0/0', '0000-0-0']
        for col in ZERO_DATE_CONFIG[table_name]:
            if col in deduped_df.columns:
                for pattern in zero_date_patterns:
                    mask = deduped_df[col].astype(str).str.strip() == pattern
                    if mask.any():
                        count = mask.sum()
                        deduped_df.loc[mask, col] = None
                        print(f"   🔄 {col}: {count}件のゼロ日付をnullに変換")

    # 一時CSVに保存
    temp_csv = f"/tmp/{table_name}_cumulative.csv"
    deduped_df.to_csv(temp_csv, index=False)

    # GCSにアップロード
    temp_blob = bucket.blob(f"google-drive/temp/{table_name}_cumulative.csv")
    temp_blob.upload_from_filename(temp_csv)
    gcs_uri = f"gs://{LANDING_BUCKET}/google-drive/temp/{table_name}_cumulative.csv"

    # BigQueryにロード
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        # 累積型テーブルは全データを削除（CSVに全履歴が含まれるため）
        # 注: 単月型テーブルは2024/9以降のみ削除
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE TRUE
        """
        query_job = client.query(delete_query)
        query_job.result()
        deleted = query_job.num_dml_affected_rows or 0
        print(f"   🗑️  既存データ削除（全件）: {deleted}行")

        # スキーマを取得してsource_folderカラムを追加（存在しない場合）
        table = client.get_table(table_id)
        existing_schema = list(table.schema)
        has_source_folder = any(f.name == "source_folder" for f in existing_schema)

        if not has_source_folder:
            new_schema = existing_schema + [bigquery.SchemaField("source_folder", "INTEGER")]
            table.schema = new_schema
            client.update_table(table, ["schema"])
            print(f"   ➕ source_folderカラムを追加")

        # ロード
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
        )
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result(timeout=300)
        print(f"   ✅ ロード完了: {load_job.output_rows}行")

        # 一時ファイル削除
        temp_blob.delete()
        os.remove(temp_csv)

        # テーブルとカラムの説明を更新
        update_table_and_column_descriptions(client, table_name)

        return True

    except Exception as e:
        print(f"   ❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_spreadsheet_files_from_gcs() -> List[str]:
    """GCSのspreadsheet/proceed/フォルダから利用可能なテーブル名リストを取得"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)
    blobs = bucket.list_blobs(prefix=f"{SPREADSHEET_PROCEED_PATH}/")

    tables = []
    for blob in blobs:
        # spreadsheet/proceed/xxx.csv の形式からテーブル名を抽出
        if blob.name.endswith(".csv"):
            table_name = blob.name.split("/")[-1].replace(".csv", "")
            if table_name in SPREADSHEET_TABLE_CONFIG:
                tables.append(table_name)

    return sorted(list(set(tables)))


def load_spreadsheet_column_descriptions(table_name: str) -> Dict[str, str]:
    """
    GCSからスプレッドシートテーブルのカラム説明を読み込み

    Args:
        table_name: テーブル名（プレフィックスなし）

    Returns:
        {英語カラム名: 日本語カラム名}の辞書
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)
    blob_path = f"{SPREADSHEET_COLUMNS_PATH}/{table_name}.csv"
    blob = bucket.blob(blob_path)

    if not blob.exists():
        print(f"   ⚠️  カラム定義ファイルが見つかりません: gs://{LANDING_BUCKET}/{blob_path}")
        return {}

    try:
        csv_content = blob.download_as_string().decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_content))
        descriptions = {}
        for _, row in df.iterrows():
            en_name = row['en_name']
            jp_name = row['jp_name']
            descriptions[en_name] = jp_name
        return descriptions
    except Exception as e:
        print(f"   ⚠️  カラム定義の読み込みエラー: {e}")
        return {}


def load_spreadsheet_to_bigquery(
    client: bigquery.Client,
    table_name: str,
    gcs_uri: str
) -> bool:
    """
    スプレッドシートCSVをBigQueryにロード（WRITE_TRUNCATE）

    Args:
        client: BigQueryクライアント
        table_name: テーブル名（プレフィックスなし）
        gcs_uri: GCS上のCSVファイルURI

    Returns:
        成功時True
    """
    bq_table_name = f"{SPREADSHEET_TABLE_PREFIX}{table_name}"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_table_name}"

    try:
        # ジョブ設定（全データ洗い替え）
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,  # スキーマ自動検出
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 全データ洗い替え
            allow_quoted_newlines=True,
        )

        # ロードジョブの実行
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        print(f"   ⏳ ロード開始: {bq_table_name} (Job ID: {load_job.job_id})")

        # ジョブの完了を待機
        load_job.result(timeout=300)

        # ロード結果の確認
        destination_table = client.get_table(table_id)
        print(f"   ✅ ロード完了: {load_job.output_rows} 行")

        # テーブルの説明を設定
        config = SPREADSHEET_TABLE_CONFIG.get(table_name, {})
        table_description = config.get("description", "")
        if table_description:
            destination_table.description = table_description
            client.update_table(destination_table, ["description"])

        # カラムの説明を設定
        column_descriptions = load_spreadsheet_column_descriptions(table_name)
        if column_descriptions:
            new_schema = []
            for field in destination_table.schema:
                description = column_descriptions.get(field.name, field.description)
                new_field = bigquery.SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode=field.mode,
                    description=description,
                    fields=field.fields
                )
                new_schema.append(new_field)
            destination_table.schema = new_schema
            client.update_table(destination_table, ["schema"])
            print(f"   📝 {len(column_descriptions)}個のカラム説明を設定")

        return True

    except GoogleCloudError as e:
        print(f"   ❌ ロードエラー: {e}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"      詳細: {error}")
        return False
    except Exception as e:
        print(f"   ❌ 予期しないエラー: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_spreadsheet_tables() -> Dict[str, int]:
    """
    スプレッドシートテーブルのBigQueryロード処理

    Returns:
        {"success": 成功数, "error": エラー数, "skipped": スキップ数}
    """
    print("\n" + "=" * 60)
    print("スプレッドシート → BigQuery ロード処理")
    print(f"GCSパス: gs://{LANDING_BUCKET}/{SPREADSHEET_PROCEED_PATH}/")
    print(f"BigQueryプレフィックス: {SPREADSHEET_TABLE_PREFIX}")
    print("=" * 60)

    # GCSから利用可能なテーブル一覧を取得
    available_tables = get_spreadsheet_files_from_gcs()

    if not available_tables:
        print("\n⚠️  スプレッドシートCSVファイルが見つかりません")
        print(f"   パス: gs://{LANDING_BUCKET}/{SPREADSHEET_PROCEED_PATH}/")
        return {"success": 0, "error": 0, "skipped": len(SPREADSHEET_TABLE_CONFIG)}

    print(f"\n利用可能なテーブル: {', '.join(available_tables)}")

    client = create_bigquery_client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    success_count = 0
    error_count = 0
    skipped_count = 0

    for table_name, config in SPREADSHEET_TABLE_CONFIG.items():
        print(f"\n📊 処理中: {SPREADSHEET_TABLE_PREFIX}{table_name}")

        # CSVファイルの存在確認
        blob_path = f"{SPREADSHEET_PROCEED_PATH}/{table_name}.csv"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            print(f"   ⚠️  CSVファイルが存在しません: gs://{LANDING_BUCKET}/{blob_path}")
            skipped_count += 1
            continue

        gcs_uri = f"gs://{LANDING_BUCKET}/{blob_path}"

        if load_spreadsheet_to_bigquery(client, table_name, gcs_uri):
            success_count += 1
        else:
            error_count += 1

    # 結果サマリー
    print("\n" + "-" * 40)
    print(f"スプレッドシートテーブル処理完了:")
    print(f"  成功: {success_count}")
    print(f"  エラー: {error_count}")
    print(f"  スキップ: {skipped_count}")

    # 統計情報の表示
    if success_count > 0:
        print("\n📈 スプレッドシートテーブル統計:")
        for table_name in SPREADSHEET_TABLE_CONFIG.keys():
            try:
                bq_table_name = f"{SPREADSHEET_TABLE_PREFIX}{table_name}"
                table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_table_name}"
                table = client.get_table(table_id)
                print(f"   {bq_table_name}: {table.num_rows:,} 行")
            except Exception:
                pass

    return {"success": success_count, "error": error_count, "skipped": skipped_count}


def process_all_tables(yyyymm: str = None):
    """
    全テーブルのBigQueryロード処理

    デフォルト動作:
    - 2024/9以降のデータを全て削除
    - GCSのproceed/フォルダにある全年月のCSVをロード

    テーブルタイプ別処理:
    - 単月型: 各CSVをそのままロード
    - 累積型: 全CSVを結合してキー毎に最新フォルダを優先

    Args:
        yyyymm: 対象年月（省略時は2024/9以降の全年月）
    """
    # 対象年月リストを取得
    target_months = get_available_months_from_gcs()

    print("=" * 60)
    print(f"proceed/ → BigQuery ロード処理")
    print(f"対象年月: {', '.join(target_months)}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print(f"モード: REPLACE（2024/9以降のデータを全て削除して再ロード）")
    print("=" * 60)

    # 累積型テーブル一覧を表示
    print(f"\n累積型テーブル: {', '.join(CUMULATIVE_TABLE_CONFIG.keys())}")
    print("※ 累積型テーブルはキー毎に最新フォルダのデータを優先")

    # BigQueryクライアント作成
    client = create_bigquery_client()
    storage_client = storage.Client()

    success_count = 0
    error_count = 0

    # 各テーブルを処理
    for table_name in TABLE_CONFIG.keys():
        # テーブル存在確認
        if not check_table_exists(client, table_name):
            print(f"\n📊 処理中: {table_name}")
            print(f"   ❌ テーブルが存在しません: {table_name}")
            error_count += 1
            continue

        # 累積型テーブルは専用処理
        if table_name in CUMULATIVE_TABLE_CONFIG:
            if process_cumulative_table(client, storage_client, table_name, target_months):
                success_count += 1
            else:
                error_count += 1
            continue

        # 単月型テーブルの処理
        print(f"\n📊 処理中（単月型）: {table_name}")

        # 2024/9以降のデータを全て削除（テーブルごとに1回だけ）
        delete_all_data_since_fiscal_start(client, table_name)

        # 全年月のCSVをロード
        table_success = True
        bucket = storage_client.bucket(LANDING_BUCKET)

        for month in target_months:
            gcs_uri = f"gs://{LANDING_BUCKET}/google-drive/proceed/{month}/{table_name}.csv"
            blob_name = f"google-drive/proceed/{month}/{table_name}.csv"
            blob = bucket.blob(blob_name)

            if not blob.exists():
                print(f"   ⚠️  CSVファイルが存在しません: {gcs_uri}")
                continue

            # BigQueryへロード
            if not load_csv_to_bigquery(client, table_name, gcs_uri, month):
                table_success = False

        if table_success:
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
                cumulative_marker = "（累積型）" if table_name in CUMULATIVE_TABLE_CONFIG else ""
                print(f"   {table_name}{cumulative_marker}: {table.num_rows:,} 行")
            except:
                pass

    # スプレッドシートテーブルの処理
    process_spreadsheet_tables()


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
    # デフォルト: 2024/9以降の全データをREPLACEモードでロード
    # 引数なしで実行すると、GCSのproceed/にある全年月が対象

    if len(sys.argv) == 1:
        # 引数なし: デフォルト動作（2024/9以降の全データをREPLACE）
        print("引数なし: デフォルトモード（2024/9以降の全データをREPLACE）")
        process_all_tables()
    elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
        print("使用方法:")
        print("  python load_to_bigquery.py              # デフォルト: 全データをREPLACE（Drive + Spreadsheet）")
        print("  python load_to_bigquery.py YYYYMM       # 特定月のみロード（既存データに追加）")
        print("  python load_to_bigquery.py --spreadsheet # スプレッドシートテーブルのみロード")
        print("")
        print("デフォルト動作:")
        print("  - 2024/9以降のDriveデータを全て削除して再ロード")
        print("  - スプレッドシートテーブルを全て洗い替え")
        print("")
        print("スプレッドシートテーブル:")
        for table_name, config in SPREADSHEET_TABLE_CONFIG.items():
            print(f"  - {SPREADSHEET_TABLE_PREFIX}{table_name}: {config.get('description', '')}")
        sys.exit(0)
    elif sys.argv[1] == "--spreadsheet":
        # スプレッドシートテーブルのみロード
        print("スプレッドシートモード: スプレッドシートテーブルのみロード")
        process_spreadsheet_tables()
    else:
        # 特定年月のみロード（追加モード）
        yyyymm = sys.argv[1]
        print(f"特定月モード: {yyyymm} のデータをロード（追加）")
        # 旧動作との互換性のため、特定月指定時は追加モード
        # process_all_tablesは引数なしで全データREPLACEになっているので、
        # 特定月だけをロードする場合は個別処理
        from google.cloud import storage as storage_module

        client = create_bigquery_client()
        storage_client = storage_module.Client()

        print("=" * 60)
        print(f"proceed/ → BigQuery ロード処理（特定月）")
        print(f"対象年月: {yyyymm}")
        print(f"プロジェクト: {PROJECT_ID}")
        print(f"データセット: {DATASET_ID}")
        print("=" * 60)

        for table_name in TABLE_CONFIG.keys():
            print(f"\n📊 処理中: {table_name}")

            if not check_table_exists(client, table_name):
                print(f"   ❌ テーブルが存在しません: {table_name}")
                continue

            gcs_uri = f"gs://{LANDING_BUCKET}/google-drive/proceed/{yyyymm}/{table_name}.csv"
            bucket = storage_client.bucket(LANDING_BUCKET)
            blob = bucket.blob(f"google-drive/proceed/{yyyymm}/{table_name}.csv")

            if not blob.exists():
                print(f"   ⚠️  CSVファイルが存在しません: {gcs_uri}")
                continue

            # 累積型テーブルの場合は専用処理（source_folderカラム追加が必要）
            if table_name in CUMULATIVE_TABLE_CONFIG:
                print(f"   （累積型テーブル: 全期間で再処理）")
                target_months = get_available_months_from_gcs()
                process_cumulative_table(client, storage_client, table_name, target_months)
                continue

            # 単月型テーブルの処理
            # 既存の指定月データを削除
            delete_partition_data_by_csv(client, table_name, gcs_uri)

            # BigQueryへロード
            load_csv_to_bigquery(client, table_name, gcs_uri, yyyymm)

            # 説明を更新
            update_table_and_column_descriptions(client, table_name)

        print("\n処理完了")