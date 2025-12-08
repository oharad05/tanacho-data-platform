#!/usr/bin/env python3
"""
スプレッドシート → GCS → BigQuery 連携サービス (Cloud Run)

共有ドライブの「手入力用」フォルダからスプレッドシートを自動検出し、
指定シートのデータをGCS経由でBigQueryに連携します。

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
# 共有ドライブ内の「手入力用」フォルダID
MANUAL_INPUT_FOLDER_ID = os.environ.get("MANUAL_INPUT_FOLDER_ID", "1O4eUpl6AWgag1oMTyrtoA7sXEHX3mfxc")

GCS_BASE_PATH = "spreadsheet"  # Drive連携の /raw/, /proceed/ とは完全分離
BQ_DATASET = "corporate_data"
TABLE_PREFIX = "ss_"  # 既存テーブルと区別するプレフィックス

# Drive API + Sheets API + Cloud Platform
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/cloud-platform"
]

# スプレッドシートとシート名のマッピング定義
# 共有ドライブから検出したスプレッドシートに対して、どのシートをどのテーブルに連携するかを定義
SPREADSHEET_MAPPING = {
    # 損益計算書 入力シート（東京支店）
    "1eEiLA0MfDghuDqbss7xC-wdCqbC-Tj9MVgHisGgqwXA": [
        {"sheet_name": "システム利用シート_在庫損益・前受け金", "table_name": "inventory_advance_tokyo"},
    ],
    # 損益計算書 入力シート（福岡支店）
    "1yNJU2RCye5yFxKH91UEyKRszWOrT3M-tmk-6SWkTRJA": [
        {"sheet_name": "システム利用シート_GS売上粗利", "table_name": "gs_sales_profit"},
        {"sheet_name": "システム利用シート_在庫損益・前受け金", "table_name": "inventory_advance_fukuoka"},
    ],
    # 損益計算書 入力シート（長崎支店）
    "1pcZYlchm6bbowxdUqrmYIXiYSOx-dflmBPviFw4kuTo": [
        {"sheet_name": "システム利用シート_在庫損益・前受け金", "table_name": "inventory_advance_nagasaki"},
    ],
}


def _build_drive_service():
    """Drive APIサービスを構築"""
    creds, _ = google_auth_default(scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _build_sheets_service():
    """Sheets APIサービスを構築"""
    creds, _ = google_auth_default(scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_spreadsheets_in_folder(folder_id: str) -> List[Dict]:
    """
    共有ドライブのフォルダ内にあるスプレッドシートを一覧取得
    共有ドライブの場合はcorporaパラメータを使用
    """
    drive = _build_drive_service()

    # まずフォルダのdriveIdを取得
    try:
        folder_meta = drive.files().get(
            fileId=folder_id,
            fields="id,name,driveId",
            supportsAllDrives=True
        ).execute()
        drive_id = folder_meta.get("driveId")
        print(f"[INFO] フォルダ情報: {folder_meta.get('name')} (driveId: {drive_id})")
    except Exception as e:
        print(f"[ERROR] フォルダメタ取得エラー: {e}")
        drive_id = None

    # 検索クエリ
    q = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"

    # 共有ドライブの場合はcorporaを指定
    if drive_id:
        res = drive.files().list(
            q=q,
            fields="files(id,name)",
            corpora="drive",
            driveId=drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100
        ).execute()
    else:
        res = drive.files().list(
            q=q,
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100
        ).execute()

    files = res.get("files", [])
    print(f"[INFO] 「手入力用」フォルダ内のスプレッドシート: {len(files)}件")
    for f in files:
        print(f"  - {f['name']} (ID: {f['id']})")

    return files


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
    sheets = _build_sheets_service()

    # シート全体を取得
    result = sheets.spreadsheets().values().get(
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

    # 1. 共有ドライブの「手入力用」フォルダからスプレッドシートを検出
    spreadsheets = list_spreadsheets_in_folder(MANUAL_INPUT_FOLDER_ID)

    # 2. 検出したスプレッドシートをマッピングに基づいて処理
    for ss in spreadsheets:
        sheet_id = ss['id']
        ss_name = ss['name']

        # マッピングに存在するか確認
        if sheet_id not in SPREADSHEET_MAPPING:
            print(f"[SKIP] マッピング未定義: {ss_name} (ID: {sheet_id})")
            results["skipped"].append({
                "spreadsheet": ss_name,
                "reason": "mapping not defined"
            })
            continue

        # 各シートを処理
        for mapping in SPREADSHEET_MAPPING[sheet_id]:
            sheet_name = mapping["sheet_name"]
            table_name = mapping["table_name"]

            print(f"\n[INFO] 処理開始: {table_name}")
            print(f"  スプレッドシート: {ss_name}")
            print(f"  シート名: {sheet_name}")

            try:
                # 3. カラムマッピング読み込み
                columns_mapping = load_columns_mapping_from_gcs(table_name)

                # 4. スプレッドシートからデータ取得
                raw_data = fetch_spreadsheet_data(sheet_id, sheet_name)

                # 5. データ変換
                df = transform_data(raw_data, columns_mapping)

                if df.empty:
                    print(f"[WARN] データが空のためスキップ: {table_name}")
                    results["skipped"].append({
                        "table": table_name,
                        "reason": "empty data"
                    })
                    continue

                # 6. GCSに保存
                gcs_path = save_to_gcs(df, table_name)

                # 7. BigQueryにロード
                row_count = load_to_bigquery(gcs_path, table_name, columns_mapping)

                results["success"].append({
                    "table": f"{TABLE_PREFIX}{table_name}",
                    "rows": row_count,
                    "gcs_path": gcs_path,
                    "source": ss_name
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
    print(f"対象フォルダID: {MANUAL_INPUT_FOLDER_ID}")
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
        },
        "config": {
            "manual_input_folder_id": MANUAL_INPUT_FOLDER_ID,
            "gcs_base_path": f"gs://{LANDING_BUCKET}/{GCS_BASE_PATH}/",
            "bq_dataset": BQ_DATASET,
            "table_prefix": TABLE_PREFIX
        }
    })


if __name__ == "__main__":
    # ローカル実行用
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
