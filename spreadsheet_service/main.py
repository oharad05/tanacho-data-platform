#!/usr/bin/env python3
"""
スプレッドシート → GCS → BigQuery 連携サービス (Cloud Run)

共有ドライブの「手入力用」フォルダからスプレッドシートを自動検出し、
指定シートのデータをGCS経由でBigQueryに連携します。

- GCSパス: gs://data-platform-landing-prod/spreadsheet/
- BQテーブル: corporate_data.ss_*

バリデーション機能:
- カラム不整合チェック
- レコード0件チェック

結果はGoogle Cloud Loggingに出力され、後からSlack等に連携可能。

エンドポイント:
    POST /sync - 全スプレッドシートを同期
    GET /health - ヘルスチェック
"""

import os
import io
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from google.cloud import storage, bigquery
from google.auth import default as google_auth_default
from google.oauth2 import service_account

# ============================================================
# バリデーション設定
# ============================================================

VALIDATION_ENABLED = os.environ.get("VALIDATION_ENABLED", "true").lower() == "true"

# バリデーションログ用のlogger
validation_logger = logging.getLogger("spreadsheet-to-bq-validation")
if not validation_logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    validation_logger.addHandler(handler)
    validation_logger.setLevel(logging.INFO)

# ===== Flask アプリ =====
app = Flask(__name__)

# ===== 環境変数（Cloud Run で設定） =====
PROJECT_ID = os.environ.get("GCP_PROJECT", "data-platform-prod-475201")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "data-platform-landing-prod")
# 共有ドライブ内の「手入力用」フォルダID
MANUAL_INPUT_FOLDER_ID = os.environ.get("MANUAL_INPUT_FOLDER_ID", "1O4eUpl6AWgag1oMTyrtoA7sXEHX3mfxc")
# ドメイン全体の委任用
SERVICE_JSON = os.environ.get("SERVICE_JSON_PATH")        # サービスアカウントJSONキーファイルパス
IMPERSONATE_USER = os.environ.get("IMPERSONATE_USER")     # なりすますユーザーのメールアドレス

GCS_BASE_PATH = "spreadsheet"  # Drive連携の /raw/, /proceed/ とは完全分離
BQ_DATASET = "corporate_data"
TABLE_PREFIX = "ss_"  # 既存テーブルと区別するプレフィックス

# スコープ: 管理コンソールで登録したものと一致させる
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
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
            "service": "spreadsheet-to-bq",
            "table_name": result.get("table_name", "unknown"),
            "validation_type": result.get("validation_type", "unknown"),
            "status": result.get("status", "unknown")
        },
        "jsonPayload": result
    }

    if result.get("status") == "ERROR":
        validation_logger.error(json.dumps(log_entry, ensure_ascii=False))
    elif result.get("warnings"):
        validation_logger.warning(json.dumps(log_entry, ensure_ascii=False))
    else:
        validation_logger.info(json.dumps(log_entry, ensure_ascii=False))


def _format_validation_message(result: Dict[str, Any]) -> str:
    """ログメッセージを整形"""
    status = result.get("status", "UNKNOWN")
    table_name = result.get("table_name", "unknown")
    validation_type = result.get("validation_type", "validation")

    if status == "OK":
        row_count = result.get("row_count", 0)
        return f"[VALIDATION {status}] {table_name}: {validation_type} passed ({row_count} rows)"
    else:
        error_count = len(result.get("errors", []))
        return f"[VALIDATION {status}] {table_name}: {validation_type} failed ({error_count} errors)"


def validate_columns_and_rows(
    raw_data: List[List],
    table_name: str,
    expected_columns: List[str],
    sheet_name: str = None
) -> Dict[str, Any]:
    """
    スプレッドシートデータのカラム不整合とレコード0件をチェック

    Args:
        raw_data: スプレッドシートから取得した生データ（2次元リスト）
        table_name: テーブル名
        expected_columns: 期待されるカラム名リスト（日本語）
        sheet_name: シート名

    Returns:
        検証結果の辞書
    """
    errors = []
    warnings = []

    # ヘッダーとデータを分離
    if not raw_data:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "spreadsheet-to-bq",
            "validation_type": "column_and_row_check",
            "table_name": table_name,
            "sheet_name": sheet_name,
            "status": "ERROR",
            "row_count": 0,
            "errors": [{"type": "EMPTY_DATA", "message": "データが0件です（ヘッダーも含め空）"}]
        }

    actual_columns = [str(col).strip() for col in raw_data[0]]
    row_count = len(raw_data) - 1  # ヘッダーを除く

    # 1. カラム不整合チェック
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]

    if missing_columns:
        errors.append({
            "type": "MISSING_COLUMNS",
            "message": f"期待されるカラムが存在しません: {missing_columns}",
            "details": {"missing": missing_columns}
        })

    if extra_columns:
        warnings.append({
            "type": "EXTRA_COLUMNS",
            "message": f"定義外のカラムが存在します: {extra_columns}",
            "details": {"extra": extra_columns}
        })

    # 2. レコード0件チェック
    if row_count == 0:
        errors.append({
            "type": "EMPTY_DATA",
            "message": "データが0件です（ヘッダーのみ）"
        })

    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "spreadsheet-to-bq",
        "validation_type": "column_and_row_check",
        "table_name": table_name,
        "sheet_name": sheet_name,
        "status": "ERROR" if errors else "OK",
        "row_count": row_count,
        "column_count": len(actual_columns),
        "expected_column_count": len(expected_columns),
        "errors": errors,
        "warnings": warnings
    }

    return result


def _get_credentials():
    """
    認証情報を取得

    ドメイン全体の委任（Domain-wide Delegation）を使用する場合:
    - SERVICE_JSON_PATH: サービスアカウントのJSONキーファイルパス
    - IMPERSONATE_USER: なりすますユーザーのメールアドレス（例: fiby2@tanacho.com）

    これにより、サービスアカウントが指定ユーザーとしてDrive/Sheetsにアクセスできる
    """
    if SERVICE_JSON:
        creds = service_account.Credentials.from_service_account_file(SERVICE_JSON, scopes=SCOPES)
        # ドメイン全体の委任: ユーザーになりすまし
        if IMPERSONATE_USER:
            creds = creds.with_subject(IMPERSONATE_USER)
            print(f"[INFO] Domain-wide delegation enabled: impersonating {IMPERSONATE_USER}")
        return creds
    else:
        creds, _ = google_auth_default(scopes=SCOPES)
        print("[INFO] Using default credentials (no impersonation)")
        return creds


def _build_drive_service():
    """Drive APIサービスを構築"""
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _build_sheets_service():
    """Sheets APIサービスを構築"""
    creds = _get_credentials()
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

                # ============================================================
                # バリデーション: カラム不整合・レコード0件チェック
                # ============================================================
                if VALIDATION_ENABLED:
                    expected_columns = list(columns_mapping['jp_name'])
                    validation_result = validate_columns_and_rows(
                        raw_data=raw_data,
                        table_name=table_name,
                        expected_columns=expected_columns,
                        sheet_name=sheet_name
                    )
                    log_validation_result(validation_result)

                    if validation_result.get("status") == "ERROR":
                        for error in validation_result.get("errors", []):
                            print(f"  ⚠️  バリデーションエラー: {error.get('message')}")
                    else:
                        print(f"  ✅ バリデーションOK: カラム・レコード数チェック passed")

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
