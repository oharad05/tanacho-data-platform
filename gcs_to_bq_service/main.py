#!/usr/bin/env python3
"""
gcs-to-bq Cloud Run Service
GCS上のExcelファイルをCSVに変換し、BigQueryにロード
"""

import os
import io
import json
import base64
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, Any
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# 環境変数
PROJECT_ID = os.environ.get("GCP_PROJECT", "data-platform-prod-475201")
DATASET_ID = "corporate_data"
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "data-platform-landing-prod")
COLUMNS_PATH = "config/columns"
MAPPING_FILE = "config/mapping/excel_mapping.csv"
MONETARY_SCALE_FILE = "config/mapping/monetary_scale_conversion.csv"

# テーブル定義
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
        "partition_field": "slip_date",
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
        "partition_field": "slip_date",
        "clustering_fields": ["classification_type"]
    },
    "customer_sales_target_and_achievements": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["branch_code", "customer_code"]
    },
    "construction_progress_days_amount": {
        "partition_field": "property_period",
        "clustering_fields": ["branch_code", "property_number"]
    },
    "construction_progress_days_final_date": {
        "partition_field": "final_billing_sales_date",
        "clustering_fields": ["property_number"]
    }
}

# ============================================================
# Excel → CSV 変換処理
# ============================================================

def load_column_mapping(table_name: str) -> Dict[str, Dict[str, str]]:
    """カラムマッピング定義を読み込み"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    mapping_blob = bucket.blob(f"{COLUMNS_PATH}/{table_name}.csv")
    if not mapping_blob.exists():
        print(f"⚠️  マッピングファイルが見つかりません: {table_name}.csv")
        return {}

    csv_data = mapping_blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(csv_data))

    mapping = {}
    for _, row in df.iterrows():
        mapping[row['jp_name']] = {
            'en_name': row['en_name'],
            'type': row['type']
        }
    return mapping

def convert_date_format(value: Any, date_type: str, column_name: str = '') -> str:
    """日付フォーマットの変換"""
    if pd.isna(value) or value == '' or value is None:
        return ''

    # 無効な日付値を空文字列に変換
    value_str = str(value)
    if value_str in ['0000/00/00', '0000-00-00', '00/00/0000', '0', 'NaT']:
        return ''

    # 誤った形式の日付を修正 (例: "0223/03/25" → "2023/03/25")
    import re
    match = re.match(r'^0(\d{3})/(\d{2})/(\d{2})$', value_str)
    if match:
        value_str = f"2{match.group(1)}/{match.group(2)}/{match.group(3)}"

    # 数値の場合の処理
    if isinstance(value, (int, float)):
        # Excelのシリアル日付
        if value > 0 and value < 100000:
            try:
                excel_base = pd.Timestamp('1899-12-30')
                dt = excel_base + pd.Timedelta(days=int(value))
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass

        # Unixタイムスタンプ（ナノ秒）
        elif value > 1e15:
            try:
                dt = pd.to_datetime(value, unit='ns')
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass


    # 「年月」特殊処理（例: "2025年9月" → "2025-09-01"）
    if '年' in value_str and '月' in value_str:
        import re
        try:
            match = re.match(r'(\d{4})年(\d{1,2})月', value_str)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                return f"{year}-{month}-01"
        except:
            pass

    # DATE型の処理
    if date_type == 'DATE':
        # YYYY/MM形式の場合、1日を追加
        import re
        if re.match(r'^\d{4}/\d{1,2}$', value_str):
            try:
                dt = pd.to_datetime(value_str + '/01', format='%Y/%m/%d')
                return dt.strftime('%Y-%m-%d')
            except:
                pass

        try:
            dt = pd.to_datetime(value_str)
            return dt.strftime('%Y-%m-%d')
        except:
            print(f"⚠️  日付変換エラー: {value_str}")
            return value_str

    # DATETIME型の処理
    elif date_type == 'DATETIME':
        try:
            dt = pd.to_datetime(value_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            print(f"⚠️  日時変換エラー: {value_str}")
            return value_str

    return value_str

def apply_data_type_conversion(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """データ型変換を適用"""
    df = df.copy()

    for col in df.columns:
        if col not in column_mapping:
            continue

        data_type = column_mapping[col]['type']

        # DATE/DATETIME型
        if data_type in ['DATE', 'DATETIME']:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if data_type == 'DATE':
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                else:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df[col] = df[col].apply(lambda x: convert_date_format(x, data_type, col))

        # INT64型
        elif data_type == 'INT64':
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype('Int64')

        # NUMERIC型
        elif data_type == 'NUMERIC':
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # STRING型
        elif data_type == 'STRING':
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', '')

    return df

def rename_columns(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """カラム名を日本語から英語に変換"""
    rename_dict = {}

    for jp_col in df.columns:
        if jp_col in column_mapping:
            en_col = column_mapping[jp_col]['en_name']
            rename_dict[jp_col] = en_col
        else:
            print(f"⚠️  マッピング未定義のカラム: {jp_col}")
            rename_dict[jp_col] = jp_col

    return df.rename(columns=rename_dict)

def load_monetary_scale_config(storage_client: storage.Client) -> pd.DataFrame:
    """金額単位変換設定を読み込み"""
    try:
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(MONETARY_SCALE_FILE)

        if not blob.exists():
            print(f"⚠️  金額変換設定ファイルが見つかりません: {MONETARY_SCALE_FILE}")
            return pd.DataFrame()

        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
        return df
    except Exception as e:
        print(f"⚠️  金額変換設定の読み込みエラー: {e}")
        return pd.DataFrame()

def apply_monetary_scale_conversion(
    df: pd.DataFrame,
    table_name: str,
    storage_client: storage.Client
) -> pd.DataFrame:
    """
    金額単位変換を適用

    Args:
        df: 変換対象のDataFrame（英語カラム名に変換済み）
        table_name: テーブル名
        storage_client: Storage Client

    Returns:
        変換後のDataFrame
    """
    try:
        # 金額変換設定を読み込み
        config_df = load_monetary_scale_config(storage_client)

        if config_df.empty:
            return df

        # 対象テーブルの設定を取得
        target_config = config_df[config_df['file_name'] == table_name]

        if target_config.empty:
            print(f"   金額変換設定なし: {table_name}")
            return df

        df = df.copy()

        for _, config in target_config.iterrows():
            condition_col = config['condition_column_name']
            condition_values = eval(config['condition_column_value'])  # リスト文字列を評価
            object_columns = eval(config['object_column_name'])  # リスト文字列を評価
            convert_value = float(config['convert_value'])

            # 条件に一致する行をフィルタ
            if condition_col not in df.columns:
                print(f"⚠️  条件カラムが存在しません: {condition_col}")
                continue

            mask = df[condition_col].isin(condition_values)

            # 対象カラムを変換
            for col in object_columns:
                if col in df.columns:
                    # 条件に一致する行のみ変換
                    df.loc[mask, col] = pd.to_numeric(df.loc[mask, col], errors='coerce') * convert_value
                    print(f"   💰 {col} を{convert_value}倍に変換（条件: {condition_col} in {condition_values}）")
                else:
                    print(f"⚠️  変換対象カラムが存在しません: {col}")

        return df

    except Exception as e:
        print(f"⚠️  金額変換エラー: {e}")
        traceback.print_exc()
        return df

def transform_excel_to_csv(
    storage_client: storage.Client,
    table_name: str,
    yyyymm: str
) -> bool:
    """Excelファイルを読み込んでCSVに変換"""
    try:
        print(f"\n📄 処理中: {table_name}")

        bucket = storage_client.bucket(LANDING_BUCKET)

        # raw/ から読み込み
        raw_path = f"raw/{yyyymm}/{table_name}.xlsx"
        raw_blob = bucket.blob(raw_path)

        if not raw_blob.exists():
            print(f"⚠️  ファイルが存在しません: gs://{LANDING_BUCKET}/{raw_path}")
            return False

        # カラムマッピング読み込み
        column_mapping = load_column_mapping(table_name)
        if not column_mapping:
            print(f"❌ カラムマッピングが見つかりません: {table_name}")
            return False

        # Excelファイル読み込み
        excel_bytes = raw_blob.download_as_bytes()

        # profit_plan_termの場合は「東京支店目標103期」シートのみを読み込む
        if table_name == "profit_plan_term":
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='東京支店目標103期')
            print(f"   シート指定: 東京支店目標103期")
        else:
            df = pd.read_excel(io.BytesIO(excel_bytes))

        # カラム名の改行を除去
        df.columns = [col.replace('\n', '') if isinstance(col, str) else col for col in df.columns]

        print(f"   データ: {len(df)}行 × {len(df.columns)}列")

        # 日本語カラム名を英語に変換（型変換前）
        jp_column_mapping = {jp: info for jp, info in column_mapping.items()}

        # 日付列の事前処理
        for jp_col, info in jp_column_mapping.items():
            if jp_col in df.columns and info['type'] in ['DATE', 'DATETIME']:
                if pd.api.types.is_datetime64_any_dtype(df[jp_col]):
                    if info['type'] == 'DATE':
                        df[jp_col] = df[jp_col].dt.strftime('%Y-%m-%d')
                    else:
                        df[jp_col] = df[jp_col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # データ型変換
        df = apply_data_type_conversion(df, jp_column_mapping)

        # カラム名変換
        df = rename_columns(df, jp_column_mapping)

        # 金額単位変換（カラム名変換後に実行）
        df = apply_monetary_scale_conversion(df, table_name, storage_client)

        # CSV出力
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        # proceed/ に保存
        proceed_path = f"proceed/{yyyymm}/{table_name}.csv"
        proceed_blob = bucket.blob(proceed_path)
        proceed_blob.upload_from_file(csv_buffer, content_type='text/csv')

        print(f"   出力: gs://{LANDING_BUCKET}/{proceed_path}")
        print(f"✅ 変換完了: {table_name}")

        return True

    except Exception as e:
        print(f"❌ 変換エラー ({table_name}): {e}")
        traceback.print_exc()
        return False

# ============================================================
# BigQuery ロード処理
# ============================================================

def load_table_name_mapping(storage_client: storage.Client) -> Dict[str, str]:
    """テーブル名マッピング（日本語→英語）を読み込み"""
    try:
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(MAPPING_FILE)

        if not blob.exists():
            print(f"⚠️  マッピングファイルが見つかりません: {MAPPING_FILE}")
            return {}

        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))

        mapping = {}
        for _, row in df.iterrows():
            en_name = row['en_name']
            jp_name = row['jp_name'].replace('.xlsx', '')
            mapping[en_name] = jp_name

        return mapping
    except Exception as e:
        print(f"⚠️  マッピング読み込みエラー: {e}")
        return {}

def load_column_descriptions(storage_client: storage.Client, table_name: str) -> Dict[str, str]:
    """カラムの説明を読み込み"""
    try:
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(f"{COLUMNS_PATH}/{table_name}.csv")

        if not blob.exists():
            print(f"⚠️  カラム定義ファイルが見つかりません: {table_name}.csv")
            return {}

        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))

        descriptions = {}
        for _, row in df.iterrows():
            en_name = row['en_name']
            description = row['description']
            descriptions[en_name] = description

        return descriptions
    except Exception as e:
        print(f"⚠️  カラム説明読み込みエラー: {e}")
        return {}

def update_table_and_column_descriptions(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    table_name: str
) -> bool:
    """テーブルとカラムの説明を更新"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        table = bq_client.get_table(table_id)

        # テーブル名マッピングを読み込み
        table_mapping = load_table_name_mapping(storage_client)
        if table_name in table_mapping:
            table.description = table_mapping[table_name]
            print(f"   📝 テーブル説明を設定: {table_mapping[table_name]}")

        # カラムの説明を読み込み
        column_descriptions = load_column_descriptions(storage_client, table_name)

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
        table = bq_client.update_table(table, ["description", "schema"])
        print(f"   ✅ {len(column_descriptions)}個のカラム説明を設定")

        return True

    except Exception as e:
        print(f"   ⚠️  説明の更新に失敗: {e}")
        return False

def delete_partition_data(
    bq_client: bigquery.Client,
    table_name: str,
    yyyymm: str
) -> bool:
    """指定月のパーティションデータを削除"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    partition_field = TABLE_CONFIG[table_name]["partition_field"]

    year = yyyymm[:4]
    month = yyyymm[4:6]

    if table_name in ["ledger_income", "ledger_loss"]:
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE({partition_field}) = '{year}-{month}-01'
        """
    else:
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE {partition_field} = '{year}-{month}-01'
        """

    try:
        print(f"   🗑️  既存データ削除中: {year}-{month}")
        query_job = bq_client.query(delete_query)
        query_job.result()

        if query_job.num_dml_affected_rows:
            print(f"      削除: {query_job.num_dml_affected_rows} 行")
        else:
            print(f"      削除対象なし")

        return True

    except Exception as e:
        print(f"   ⚠️  削除処理スキップ: {e}")
        return True

def load_csv_to_bigquery(
    bq_client: bigquery.Client,
    table_name: str,
    yyyymm: str
) -> bool:
    """CSVファイルをBigQueryにロード"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    gcs_uri = f"gs://{LANDING_BUCKET}/proceed/{yyyymm}/{table_name}.csv"

    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=False,
            max_bad_records=0,
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        print(f"   ⏳ ロード開始: {table_name} (Job ID: {load_job.job_id})")

        load_job.result(timeout=300)

        destination_table = bq_client.get_table(table_id)
        print(f"   ✅ ロード完了: {load_job.output_rows} 行を追加")
        print(f"      総レコード数: {destination_table.num_rows:,} 行")

        return True

    except GoogleCloudError as e:
        print(f"   ❌ ロードエラー: {e}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"      詳細: {error}")
        return False
    except Exception as e:
        print(f"   ❌ 予期しないエラー: {e}")
        return False

# ============================================================
# Flask アプリケーション
# ============================================================

app = Flask(__name__)

@app.route("/transform", methods=["POST"])
def transform_endpoint():
    """
    Excel → CSV 変換エンドポイント

    リクエスト例:
    {
        "yyyymm": "202509",
        "tables": ["sales_target_and_achievements", "billing_balance"]
    }

    空の場合は全テーブル処理
    """
    try:
        payload = request.get_json(force=True, silent=True) or {}
        yyyymm = payload.get("yyyymm")
        tables = payload.get("tables", list(TABLE_CONFIG.keys()))

        if not yyyymm:
            return jsonify({"error": "yyyymm is required"}), 400

        print("=" * 60)
        print(f"raw/ → proceed/ 変換処理")
        print(f"対象年月: {yyyymm}")
        print("=" * 60)

        storage_client = storage.Client()

        success_count = 0
        error_count = 0
        results = []

        for table_name in tables:
            if transform_excel_to_csv(storage_client, table_name, yyyymm):
                success_count += 1
                results.append({"table": table_name, "status": "success"})
            else:
                error_count += 1
                results.append({"table": table_name, "status": "error"})

        print("=" * 60)
        print(f"処理完了: 成功 {success_count} / エラー {error_count}")
        print("=" * 60)

        return jsonify({
            "status": "completed",
            "yyyymm": yyyymm,
            "success": success_count,
            "error": error_count,
            "results": results
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/load", methods=["POST"])
def load_endpoint():
    """
    CSV → BigQuery ロードエンドポイント

    リクエスト例:
    {
        "yyyymm": "202509",
        "tables": ["sales_target_and_achievements"],
        "replace": true
    }
    """
    try:
        payload = request.get_json(force=True, silent=True) or {}
        yyyymm = payload.get("yyyymm")
        tables = payload.get("tables", list(TABLE_CONFIG.keys()))
        replace_existing = payload.get("replace", False)

        if not yyyymm:
            return jsonify({"error": "yyyymm is required"}), 400

        print("=" * 60)
        print(f"proceed/ → BigQuery ロード処理")
        print(f"対象年月: {yyyymm}")
        print(f"モード: {'REPLACE' if replace_existing else 'APPEND'}")
        print("=" * 60)

        bq_client = bigquery.Client(project=PROJECT_ID)
        storage_client = storage.Client()

        success_count = 0
        error_count = 0
        results = []

        for table_name in tables:
            print(f"\n📊 処理中: {table_name}")

            # 既存データの削除
            if replace_existing:
                delete_partition_data(bq_client, table_name, yyyymm)

            # BigQueryへロード
            if load_csv_to_bigquery(bq_client, table_name, yyyymm):
                # テーブルとカラムの説明を更新
                update_table_and_column_descriptions(bq_client, storage_client, table_name)
                success_count += 1
                results.append({"table": table_name, "status": "success"})
            else:
                error_count += 1
                results.append({"table": table_name, "status": "error"})

        print("\n" + "=" * 60)
        print(f"処理完了: 成功 {success_count} / エラー {error_count}")
        print("=" * 60)

        return jsonify({
            "status": "completed",
            "yyyymm": yyyymm,
            "success": success_count,
            "error": error_count,
            "results": results
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/pubsub", methods=["POST"])
def pubsub_endpoint():
    """
    Pub/Sub トリガーエンドポイント
    drive-to-gcs完了後に自動実行される

    メッセージ例:
    {
        "message": {
            "data": "eyJ5eXl5bW0iOiAiMjAyNTA5In0="  # base64: {"yyyymm": "202509"}
        }
    }
    """
    try:
        envelope = request.get_json(force=True, silent=True) or {}
        msg = envelope.get("message", {})
        data_b64 = msg.get("data")

        if not data_b64:
            return ("Bad Request: no message.data", 400)

        payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
        yyyymm = payload.get("yyyymm")
        tables = payload.get("tables", list(TABLE_CONFIG.keys()))
        replace_existing = payload.get("replace", True)

        if not yyyymm:
            return jsonify({"error": "yyyymm is required"}), 400

        print(f"[INFO] Pub/Sub triggered: yyyymm={yyyymm}")

        # Transform実行
        print("=" * 60)
        print(f"raw/ → proceed/ 変換処理")
        print(f"対象年月: {yyyymm}")
        print("=" * 60)

        storage_client = storage.Client()
        transform_success = 0
        transform_error = 0

        for table_name in tables:
            if transform_excel_to_csv(storage_client, table_name, yyyymm):
                transform_success += 1
            else:
                transform_error += 1

        print(f"変換完了: 成功 {transform_success} / エラー {transform_error}")

        # Load実行
        print("=" * 60)
        print(f"proceed/ → BigQuery ロード処理")
        print(f"対象年月: {yyyymm}")
        print("=" * 60)

        bq_client = bigquery.Client(project=PROJECT_ID)
        load_success = 0
        load_error = 0

        for table_name in tables:
            print(f"\n📊 処理中: {table_name}")

            if replace_existing:
                delete_partition_data(bq_client, table_name, yyyymm)

            if load_csv_to_bigquery(bq_client, table_name, yyyymm):
                update_table_and_column_descriptions(bq_client, storage_client, table_name)
                load_success += 1
            else:
                load_error += 1

        print(f"ロード完了: 成功 {load_success} / エラー {load_error}")

        return ("", 204)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def health():
    """ヘルスチェック"""
    return "gcs-to-bq service is running", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
