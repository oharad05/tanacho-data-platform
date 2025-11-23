#!/usr/bin/env python3
"""
スプレッドシート → GCS → BigQuery 連携サービス (Cloud Run)

このサービスは既存のDrive連携（run_service/main.py）とは完全に独立しています。
- GCSパス: gs://data-platform-landing-prod/spreadsheet/
- BQテーブル: corporate_data.ss_*

エンドポイント:
    POST /sync - 全スプレッドシートを同期
    GET /health - ヘルスチェック
"""

import os
import io
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from google.cloud import storage, bigquery
from google.auth import default as google_auth_default

# ===== Flask アプリ =====
app = Flask(__name__)

# ===== 環境変数（Cloud Run で設定） =====
PROJECT_ID = os.environ.get("GCP_PROJECT", "data-platform-prod-475201")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "data-platform-landing-prod")
GCS_BASE_PATH = "spreadsheet"  # Drive連携の /raw/, /proceed/ とは完全分離
BQ_DATASET = "corporate_data"
TABLE_PREFIX = "ss_"  # 既存テーブルと区別するプレフィックス

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/cloud-platform"
]


def load_mapping_from_gcs() -> pd.DataFrame:
    """GCSからマッピングファイルを読み込み"""
    client = storage.Client(project=PROJECT_ID)
    blob = client.bucket(LANDING_BUCKET).blob(f"{GCS_BASE_PATH}/config/mapping/mapping_files.csv")
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))
    print(f"[INFO] マッピングファイル読み込み完了: {len(df)}件")
    return df


def load_columns_mapping_from_gcs(table_name: str) -> pd.DataFrame:
    """GCSからカラムマッピングファイルを読み込み"""
    client = storage.Client(project=PROJECT_ID)
    blob = client.bucket(LANDING_BUCKET).blob(f"{GCS_BASE_PATH}/config/columns/{table_name}.csv")
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))
    print(f"[INFO] カラムマッピング読み込み完了: {table_name} ({len(df)}カラム)")
    return df


def fetch_spreadsheet_data(sheet_id: str, sheet_name: str) -> List[List]:
    """Sheets APIでスプレッドシートのデータを取得"""
    creds, _ = google_auth_default(scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # シート全体を取得
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{sheet_name}'!A:Z"
    ).execute()

    values = result.get('values', [])
    print(f"[INFO] スプレッドシート取得完了: {sheet_name} ({len(values)}行)")
    return values


def transform_data(raw_data: List[List], columns_mapping: pd.DataFrame) -> pd.DataFrame:
    """データ変換（カラム名変換、型変換）"""
    if not raw_data:
        return pd.DataFrame()

    # ヘッダーとデータを分離
    header = raw_data[0]
    data = raw_data[1:]

    # 各行をヘッダーの長さに合わせる（不足分は空文字で埋める）
    num_cols = len(header)
    normalized_data = []
    for row in data:
        if len(row) < num_cols:
            row = row + [''] * (num_cols - len(row))
        elif len(row) > num_cols:
            row = row[:num_cols]
        normalized_data.append(row)

    # DataFrameに変換
    df = pd.DataFrame(normalized_data, columns=header)

    # カラム名マッピングを辞書に変換
    jp_to_en = dict(zip(columns_mapping['jp_name'], columns_mapping['en_name']))
    type_map = dict(zip(columns_mapping['en_name'], columns_mapping['data_type']))

    # カラム名を英語に変換
    df = df.rename(columns=jp_to_en)

    # マッピングにあるカラムのみ抽出
    existing_cols = [col for col in jp_to_en.values() if col in df.columns]
    df = df[existing_cols]

    # 型変換
    for col in df.columns:
        if col in type_map:
            dtype = type_map[col]
            try:
                if dtype == 'DATE':
                    # 2025/02 形式 → 2025-02-01
                    df[col] = pd.to_datetime(df[col].str.strip(), format='%Y/%m', errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
                elif dtype == 'INTEGER':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                elif dtype == 'FLOAT':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif dtype == 'TIMESTAMP':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                # STRING はそのまま
            except Exception as e:
                print(f"[WARN] 型変換エラー [{col}]: {e}")

    print(f"[INFO] データ変換完了: {len(df)}行 x {len(df.columns)}列")
    return df


def save_to_gcs(df: pd.DataFrame, table_name: str) -> str:
    """DataFrameをCSVとしてGCSに保存"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(LANDING_BUCKET)

    gcs_path = f"{GCS_BASE_PATH}/raw/{table_name}.csv"
    blob = bucket.blob(gcs_path)

    csv_content = df.to_csv(index=False)
    blob.upload_from_string(csv_content, content_type='text/csv')

    full_path = f"gs://{LANDING_BUCKET}/{gcs_path}"
    print(f"[INFO] GCS保存完了: {full_path}")
    return full_path


def load_to_bigquery(gcs_path: str, table_name: str, columns_mapping: pd.DataFrame):
    """GCSからBigQueryにロード（洗い替え）"""
    client = bigquery.Client(project=PROJECT_ID)

    full_table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_PREFIX}{table_name}"

    # スキーマ生成
    type_mapping = {
        'STRING': bigquery.enums.SqlTypeNames.STRING,
        'INTEGER': bigquery.enums.SqlTypeNames.INTEGER,
        'FLOAT': bigquery.enums.SqlTypeNames.FLOAT64,
        'DATE': bigquery.enums.SqlTypeNames.DATE,
        'TIMESTAMP': bigquery.enums.SqlTypeNames.TIMESTAMP,
    }

    schema = []
    for _, row in columns_mapping.iterrows():
        bq_type = type_mapping.get(row['data_type'], bigquery.enums.SqlTypeNames.STRING)
        schema.append(bigquery.SchemaField(row['en_name'], bq_type))

    # ロードジョブ設定
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 洗い替え
    )

    # ロード実行
    load_job = client.load_table_from_uri(
        gcs_path,
        full_table_id,
        job_config=job_config
    )

    load_job.result()  # 完了待ち

    # 結果確認
    table = client.get_table(full_table_id)
    print(f"[INFO] BigQueryロード完了: {full_table_id} ({table.num_rows}行)")
    return table.num_rows


def sync_all_spreadsheets() -> dict:
    """全スプレッドシートを同期"""
    results = {
        "success": [],
        "failed": [],
        "skipped": [],
        "timestamp": datetime.now().isoformat()
    }

    # 1. マッピング読み込み
    mapping_df = load_mapping_from_gcs()

    for _, row in mapping_df.iterrows():
        sheet_id = row['sheet_id']
        sheet_name = row['sheet_name']
        table_name = row['en_name']

        print(f"\n[INFO] 処理開始: {table_name}")
        print(f"  シートID: {sheet_id}")
        print(f"  シート名: {sheet_name}")

        try:
            # 2. カラムマッピング読み込み
            columns_mapping = load_columns_mapping_from_gcs(table_name)

            # 3. スプレッドシートからデータ取得
            raw_data = fetch_spreadsheet_data(sheet_id, sheet_name)

            # 4. データ変換
            df = transform_data(raw_data, columns_mapping)

            if df.empty:
                print(f"[WARN] データが空のためスキップ: {table_name}")
                results["skipped"].append({
                    "table": table_name,
                    "reason": "empty data"
                })
                continue

            # 5. GCSに保存
            gcs_path = save_to_gcs(df, table_name)

            # 6. BigQueryにロード
            row_count = load_to_bigquery(gcs_path, table_name, columns_mapping)

            results["success"].append({
                "table": f"{TABLE_PREFIX}{table_name}",
                "rows": row_count,
                "gcs_path": gcs_path
            })
            print(f"[INFO] 完了: {table_name}")

        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] エラー: {table_name} - {error_msg}")
            traceback.print_exc()
            results["failed"].append({
                "table": table_name,
                "error": error_msg
            })

    return results


# ===== エンドポイント =====

@app.route('/health', methods=['GET'])
def health():
    """ヘルスチェック"""
    return jsonify({"status": "healthy", "service": "spreadsheet-to-bq"})


@app.route('/sync', methods=['POST'])
def sync():
    """全スプレッドシートを同期"""
    print("=" * 60)
    print("スプレッドシート → BigQuery 連携開始")
    print(f"実行日時: {datetime.now().isoformat()}")
    print("=" * 60)

    try:
        results = sync_all_spreadsheets()

        print("\n" + "=" * 60)
        print("スプレッドシート → BigQuery 連携完了")
        print(f"成功: {len(results['success'])}件")
        print(f"失敗: {len(results['failed'])}件")
        print(f"スキップ: {len(results['skipped'])}件")
        print("=" * 60)

        status_code = 200 if not results["failed"] else 207  # 207: Multi-Status
        return jsonify(results), status_code

    except Exception as e:
        error_msg = str(e)
        print(f"[ERROR] 同期処理失敗: {error_msg}")
        traceback.print_exc()
        return jsonify({
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }), 500


@app.route('/', methods=['GET', 'POST'])
def root():
    """ルートエンドポイント（Pub/Sub対応）"""
    if request.method == 'POST':
        # Pub/Subからのトリガーの場合はsyncを実行
        return sync()
    return jsonify({
        "service": "spreadsheet-to-bq",
        "endpoints": {
            "POST /sync": "全スプレッドシートを同期",
            "GET /health": "ヘルスチェック"
        }
    })


if __name__ == "__main__":
    # ローカル実行用
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
