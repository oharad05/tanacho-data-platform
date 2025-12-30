#!/usr/bin/env python3
"""
raw/ → proceed/ 変換サービス
Excel(.xlsx)ファイルをCSVに変換し、カラム名をマッピングして
BigQuery連携用のデータに整形する

Cloud Run Service として動作
"""

import os
import io
import re
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, Any, Tuple, List
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import logging as cloud_logging

# ============================================================
# 設定
# ============================================================
PROJECT_ID = os.environ.get("PROJECT_ID", "data-platform-prod-475201")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "data-platform-landing-prod")
GCS_RAW_PREFIX = "google-drive/raw"
GCS_PROCEED_PREFIX = "google-drive/proceed"
GCS_COLUMNS_PATH = "google-drive/config/columns"
GCS_MAPPING_PATH = "google-drive/config/mapping"

# Flask アプリ
app = Flask(__name__)

# Cloud Logging セットアップ
try:
    logging_client = cloud_logging.Client()
    logging_client.setup_logging()
except Exception as e:
    print(f"Cloud Logging setup failed: {e}")

# 標準ロガー
logger = logging.getLogger("raw_to_proceed")
logger.setLevel(logging.INFO)

# バリデーション用ロガー（構造化ログ）
validation_logger = logging.getLogger("validation_logger")
validation_logger.setLevel(logging.INFO)


# ============================================================
# テーブル定義
# ============================================================
TABLES = [
    "sales_target_and_achievements",
    "billing_balance",
    "ledger_income",
    "department_summary",
    "internal_interest",
    "profit_plan_term",
    "profit_plan_term_nagasaki",
    "profit_plan_term_fukuoka",
    "ledger_loss",
    "stocks",
    "ms_allocation_ratio",
    "customer_sales_target_and_achievements",
    "construction_progress_days_amount",
    "construction_progress_days_final_date",
]

# テーブル名とGCS上のファイル名（slug）のマッピング
# drive-to-gcsサービスが生成するファイル名に対応
TABLE_TO_SLUG = {
    "sales_target_and_achievements": ["sales_target_and_achievements", "1_1"],
    "billing_balance": ["billing_balance", "3"],
    "ledger_income": ["ledger_income", "4"],
    "department_summary": ["department_summary", "6"],  # 6_{yyyymm} 形式
    "internal_interest": ["internal_interest", "7"],
    "profit_plan_term": ["profit_plan_term", "12", "12_5"],
    "profit_plan_term_nagasaki": ["profit_plan_term", "12", "12_5"],
    "profit_plan_term_fukuoka": ["profit_plan_term", "12", "12_5"],
    "ledger_loss": ["ledger_loss", "16"],
    "stocks": ["stocks", "9"],
    "ms_allocation_ratio": ["ms_allocation_ratio", "10"],
    "customer_sales_target_and_achievements": ["customer_sales_target_and_achievements", "13_1"],
    "construction_progress_days_amount": ["construction_progress_days_amount", "14_v001", "14"],
    "construction_progress_days_final_date": ["construction_progress_days_final_date", "15_v001", "15"],
}

# シート名マッピング（複数シートを持つExcelファイル用）
TABLE_SHEET_MAPPING = {
    "profit_plan_term": "東京支店目標103期",
    "profit_plan_term_nagasaki": "長崎支店目標103期",
    "profit_plan_term_fukuoka": "福岡支店目標103期",
}

# 累積型テーブル（source_folderカラムを追加するテーブル）
# 各CSVが全期間のデータを含むため、どのフォルダから取得したかを追跡
CUMULATIVE_TABLES = [
    "billing_balance",
    "profit_plan_term",
    "profit_plan_term_nagasaki",
    "profit_plan_term_fukuoka",
    "ms_allocation_ratio",
    "construction_progress_days_amount",
    "construction_progress_days_final_date",
    "stocks",
]


# ============================================================
# ユーティリティ関数
# ============================================================
def log_validation_error(validation_type: str, details: dict):
    """バリデーションエラーを構造化ログとして出力"""
    log_entry = {
        "validation_type": validation_type,
        "status": "ERROR",
        "timestamp": datetime.utcnow().isoformat(),
        **details
    }
    validation_logger.error(json.dumps(log_entry, ensure_ascii=False))


def log_validation_warning(validation_type: str, details: dict):
    """バリデーション警告を構造化ログとして出力"""
    log_entry = {
        "validation_type": validation_type,
        "status": "WARNING",
        "timestamp": datetime.utcnow().isoformat(),
        **details
    }
    validation_logger.warning(json.dumps(log_entry, ensure_ascii=False))


def load_column_mapping_from_gcs(bucket, table_name: str) -> Dict[str, Dict[str, str]]:
    """GCSからカラムマッピング定義を読み込み"""
    blob_path = f"{GCS_COLUMNS_PATH}/{table_name}.csv"
    blob = bucket.blob(blob_path)

    if not blob.exists():
        logger.warning(f"マッピングファイルが見つかりません: {blob_path}")
        return {}

    content = blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))

    mapping = {}
    for _, row in df.iterrows():
        mapping[row['jp_name']] = {
            'en_name': row['en_name'],
            'type': row['type']
        }
    return mapping


def load_monetary_scale_config_from_gcs(bucket) -> pd.DataFrame:
    """GCSから金額単位変換設定を読み込み"""
    blob_path = f"{GCS_MAPPING_PATH}/monetary_scale_conversion.csv"
    blob = bucket.blob(blob_path)

    if not blob.exists():
        return pd.DataFrame()

    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content))


def load_zero_date_config_from_gcs(bucket) -> pd.DataFrame:
    """GCSからゼロ日付変換設定を読み込み"""
    blob_path = f"{GCS_MAPPING_PATH}/zero_date_to_null.csv"
    blob = bucket.blob(blob_path)

    if not blob.exists():
        return pd.DataFrame()

    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content))


# ============================================================
# ファイル検索
# ============================================================
def find_raw_file(bucket, table_name: str, yyyymm: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    GCS上のrawファイルを検索

    検索順序:
    1. テーブル名.xlsx で検索
    2. テーブル名_{yyyymm}.xlsx で検索（department_summary用）
    3. 短縮名（slug）で検索
    4. 短縮名_{yyyymm}.xlsx で検索

    Args:
        bucket: GCSバケット
        table_name: テーブル名（英語）
        yyyymm: 対象年月

    Returns:
        見つかったBlobとパス、見つからない場合は (None, None)
    """
    prefix = f"{GCS_RAW_PREFIX}/{yyyymm}/"

    # 候補ファイル名リストを生成
    candidates = []

    # テーブル名ベースの候補
    candidates.append(f"{table_name}.xlsx")
    candidates.append(f"{table_name}_{yyyymm}.xlsx")

    # slug ベースの候補
    slugs = TABLE_TO_SLUG.get(table_name, [])
    for slug in slugs:
        candidates.append(f"{slug}.xlsx")
        candidates.append(f"{slug}_{yyyymm}.xlsx")

    # 各候補を試す
    for candidate in candidates:
        raw_path = f"{prefix}{candidate}"
        blob = bucket.blob(raw_path)
        if blob.exists():
            logger.info(f"ファイル発見: {raw_path}")
            return blob, raw_path

    # 見つからない場合、フォルダ内のファイル一覧から検索
    blobs = list(bucket.list_blobs(prefix=prefix))

    # 番号プレフィックスでマッチング
    for slug in slugs:
        # 番号部分のみで検索（例: "6" → "6_202410.xlsx" にマッチ）
        number_prefix = slug.split('_')[0] if '_' in slug else slug
        for b in blobs:
            blob_name = b.name.replace(prefix, '')
            # 番号で始まるファイルを検索
            if blob_name.startswith(f"{number_prefix}_") or blob_name.startswith(f"{number_prefix}."):
                logger.info(f"ファイル発見（部分マッチ）: {b.name}")
                return b, b.name

    logger.warning(f"ファイルが見つかりません: table={table_name}, yyyymm={yyyymm}, 候補={candidates}")
    return None, None


# ============================================================
# データ変換
# ============================================================
def convert_date_format(value: Any, date_type: str, column_name: str = '') -> str:
    """日付フォーマットの変換"""
    if pd.isna(value) or value == '' or value is None:
        return ''

    # 数値の場合の処理
    if isinstance(value, (int, float)):
        # Excelのシリアル日付の場合
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

        # Unixタイムスタンプ（ナノ秒）の場合
        elif value > 1e15:
            try:
                dt = pd.to_datetime(value, unit='ns')
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass

    value_str = str(value)

    # 年月形式の特殊処理
    if '年' in value_str and '月' in value_str:
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
            return value_str

    # DATETIME型の処理
    elif date_type == 'DATETIME':
        try:
            dt = pd.to_datetime(value_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return value_str

    return value_str


def apply_data_type_conversion(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """データ型変換を適用"""
    df = df.copy()

    for col in df.columns:
        if col not in column_mapping:
            continue

        data_type = column_mapping[col]['type']

        if data_type in ['DATE', 'DATETIME']:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if data_type == 'DATE':
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                else:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df[col] = df[col].apply(lambda x: convert_date_format(x, data_type, col))

        elif data_type == 'INT64':
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round().astype('Int64')

        elif data_type == 'NUMERIC':
            df[col] = pd.to_numeric(df[col], errors='coerce')

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
            rename_dict[jp_col] = jp_col

    return df.rename(columns=rename_dict)


def apply_monetary_scale_conversion(df: pd.DataFrame, table_name: str, config_df: pd.DataFrame) -> pd.DataFrame:
    """金額単位変換を適用"""
    if config_df.empty:
        return df

    target_config = config_df[config_df['file_name'] == table_name]
    if target_config.empty:
        return df

    df = df.copy()

    for _, config in target_config.iterrows():
        try:
            condition_col = config['condition_column_name']
            condition_values = eval(config['condition_column_value'])
            object_columns = eval(config['object_column_name'])
            convert_value = float(config['convert_value'])

            if condition_col not in df.columns:
                continue

            mask = df[condition_col].isin(condition_values)

            for col in object_columns:
                if col in df.columns:
                    df.loc[mask, col] = pd.to_numeric(df.loc[mask, col], errors='coerce') * convert_value
        except Exception as e:
            logger.warning(f"金額変換エラー: {e}")

    return df


def reorder_columns_for_bigquery(df: pd.DataFrame, table_name: str, column_mapping: Dict) -> pd.DataFrame:
    """
    BigQueryスキーマ順序に合わせてカラムをリオーダー

    BigQueryテーブルのスキーマ順序はテーブルごとに異なるため、
    パーティションフィールドを先頭に配置する必要があるテーブルのみ処理する。

    Args:
        df: リオーダー対象のDataFrame（英語カラム名に変換済み）
        table_name: テーブル名
        column_mapping: カラムマッピング辞書

    Returns:
        カラム順序を調整したDataFrame
    """
    # パーティションフィールドを先頭に配置する必要があるテーブルのみ定義
    # （BigQueryスキーマでパーティションフィールドが先頭にあるテーブル）
    TABLES_WITH_PARTITION_FIRST = {
        "stocks": "year_month",
        "ms_allocation_ratio": "year_month",
    }

    partition_field = TABLES_WITH_PARTITION_FIRST.get(table_name)

    if not partition_field or partition_field not in df.columns:
        # 対象外テーブルまたはパーティションフィールドがない場合はそのまま返す
        return df

    # パーティションフィールドを先頭に移動
    cols = list(df.columns)
    if partition_field in cols:
        cols.remove(partition_field)
        cols = [partition_field] + cols

    return df[cols]


def apply_zero_date_to_null_conversion(df: pd.DataFrame, table_name: str, config_df: pd.DataFrame) -> pd.DataFrame:
    """ゼロ日付をnullに変換"""
    if config_df.empty:
        return df

    target_config = config_df[config_df['file_name'] == table_name]
    if target_config.empty:
        return df

    df = df.copy()
    zero_date_patterns = ['0000/00/00', '0000-00-00', '0000/0/0', '0000-0-0']

    for _, config in target_config.iterrows():
        column_name = config['condition_column_name']

        if column_name not in df.columns:
            continue

        for pattern in zero_date_patterns:
            mask = df[column_name].astype(str).str.strip() == pattern
            if mask.any():
                df.loc[mask, column_name] = None

    return df


def transform_excel_to_csv(
    excel_bytes: bytes,
    table_name: str,
    sheet_name: Optional[str],
    bucket,
    monetary_config: pd.DataFrame,
    zero_date_config: pd.DataFrame
) -> Tuple[bool, Optional[bytes], Optional[str]]:
    """
    Excelファイルを読み込んでCSVに変換

    Returns:
        (成功フラグ, CSVバイト列, エラーメッセージ)
    """
    try:
        # カラムマッピング読み込み
        column_mapping = load_column_mapping_from_gcs(bucket, table_name)
        if not column_mapping:
            error_msg = f"カラムマッピングが見つかりません: {table_name}"
            log_validation_error("column_mapping_missing", {
                "table_name": table_name,
                "message": error_msg
            })
            return False, None, error_msg

        # Excel読み込み
        excel_io = io.BytesIO(excel_bytes)
        if sheet_name:
            df = pd.read_excel(excel_io, sheet_name=sheet_name)
        else:
            df = pd.read_excel(excel_io)

        if isinstance(df, dict):
            df = list(df.values())[0]

        # カラム名の改行を除去
        df.columns = [col.replace('\n', '') if isinstance(col, str) else col for col in df.columns]

        logger.info(f"データ読み込み: {len(df)}行 × {len(df.columns)}列")

        # データ型変換
        df = apply_data_type_conversion(df, column_mapping)

        # カラム名変換
        df = rename_columns(df, column_mapping)

        # 金額単位変換
        df = apply_monetary_scale_conversion(df, table_name, monetary_config)

        # ゼロ日付変換
        df = apply_zero_date_to_null_conversion(df, table_name, zero_date_config)

        # BigQueryスキーマ順序に合わせてカラムをリオーダー
        # パーティションフィールドを先頭に配置（BigQueryテーブル作成時の順序と一致させる）
        df = reorder_columns_for_bigquery(df, table_name, column_mapping)

        # CSV出力
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_bytes = csv_buffer.getvalue().encode('utf-8')

        return True, csv_bytes, None

    except Exception as e:
        error_msg = f"変換エラー ({table_name}): {str(e)}"
        log_validation_error("transform_error", {
            "table_name": table_name,
            "error": str(e)
        })
        return False, None, error_msg


# ============================================================
# メイン処理
# ============================================================
def process_month(yyyymm: str, mode: str = "replace") -> dict:
    """
    指定月のraw → proceed変換を実行

    Args:
        yyyymm: 対象年月
        mode: 処理モード（replace/append）

    Returns:
        処理結果
    """
    logger.info(f"処理開始: yyyymm={yyyymm}, mode={mode}")

    client = storage.Client()
    bucket = client.bucket(LANDING_BUCKET)

    # 設定読み込み
    monetary_config = load_monetary_scale_config_from_gcs(bucket)
    zero_date_config = load_zero_date_config_from_gcs(bucket)

    results = {
        "yyyymm": yyyymm,
        "mode": mode,
        "success": [],
        "errors": [],
        "skipped": []
    }

    for table_name in TABLES:
        try:
            # シート名取得
            sheet_name = TABLE_SHEET_MAPPING.get(table_name)

            # rawファイル検索
            raw_blob, raw_path = find_raw_file(bucket, table_name, yyyymm)

            if raw_blob is None:
                log_validation_warning("file_not_found", {
                    "table_name": table_name,
                    "yyyymm": yyyymm,
                    "message": f"rawファイルが見つかりません"
                })
                results["skipped"].append({
                    "table": table_name,
                    "reason": "file_not_found"
                })
                continue

            # Excelダウンロード
            excel_bytes = raw_blob.download_as_bytes()

            # 変換
            success, csv_bytes, error_msg = transform_excel_to_csv(
                excel_bytes, table_name, sheet_name, bucket,
                monetary_config, zero_date_config
            )

            if not success:
                results["errors"].append({
                    "table": table_name,
                    "error": error_msg
                })
                continue

            # 累積型テーブルのみsource_folderカラムを追加
            if table_name in CUMULATIVE_TABLES:
                csv_df = pd.read_csv(io.BytesIO(csv_bytes))
                csv_df["source_folder"] = int(yyyymm)
                csv_buffer = io.StringIO()
                csv_df.to_csv(csv_buffer, index=False, encoding='utf-8')
                csv_bytes = csv_buffer.getvalue().encode('utf-8')
                logger.info(f"source_folder={yyyymm} を追加（累積型テーブル）")

            # proceedにアップロード
            proceed_path = f"{GCS_PROCEED_PREFIX}/{yyyymm}/{table_name}.csv"
            proceed_blob = bucket.blob(proceed_path)
            proceed_blob.upload_from_string(csv_bytes, content_type='text/csv')

            logger.info(f"変換完了: {table_name} → {proceed_path}")
            results["success"].append(table_name)

        except Exception as e:
            error_msg = f"処理エラー: {str(e)}"
            log_validation_error("process_error", {
                "table_name": table_name,
                "yyyymm": yyyymm,
                "error": str(e)
            })
            results["errors"].append({
                "table": table_name,
                "error": error_msg
            })

    # サマリログ
    logger.info(f"処理完了: 成功={len(results['success'])}, エラー={len(results['errors'])}, スキップ={len(results['skipped'])}")

    return results


def process_all_months(mode: str = "replace") -> dict:
    """
    全月のraw → proceed変換を実行

    Args:
        mode: 処理モード

    Returns:
        処理結果
    """
    logger.info(f"全月処理開始: mode={mode}")

    client = storage.Client()
    bucket = client.bucket(LANDING_BUCKET)

    # raw/フォルダから年月一覧を取得
    prefix = f"{GCS_RAW_PREFIX}/"
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

    # prefixesから年月フォルダを取得
    months = []
    for page in blobs.pages:
        for prefix_path in page.prefixes:
            # google-drive/raw/202409/ → 202409
            folder_name = prefix_path.rstrip('/').split('/')[-1]
            if re.match(r'^\d{6}$', folder_name):
                months.append(folder_name)

    months.sort()
    logger.info(f"処理対象月: {months}")

    all_results = {
        "mode": mode,
        "months_processed": [],
        "total_success": 0,
        "total_errors": 0,
        "total_skipped": 0,
        "details": {}
    }

    for yyyymm in months:
        result = process_month(yyyymm, mode)
        all_results["months_processed"].append(yyyymm)
        all_results["total_success"] += len(result["success"])
        all_results["total_errors"] += len(result["errors"])
        all_results["total_skipped"] += len(result["skipped"])
        all_results["details"][yyyymm] = result

    logger.info(f"全月処理完了: 成功={all_results['total_success']}, エラー={all_results['total_errors']}, スキップ={all_results['total_skipped']}")

    return all_results


# ============================================================
# Flask エンドポイント
# ============================================================
@app.route("/", methods=["GET"])
def health_check():
    """ヘルスチェック"""
    return jsonify({"status": "healthy", "service": "raw-to-proceed"})


@app.route("/transform", methods=["POST", "GET"])
def transform():
    """
    raw → proceed 変換エンドポイント

    Query Parameters:
        mode: replace（デフォルト）/ append
        target_month: 対象月（YYYYMM形式）。省略時は全月処理
    """
    try:
        mode = request.args.get("mode", "replace")
        target_month = request.args.get("target_month", "")

        logger.info(f"リクエスト受信: mode={mode}, target_month={target_month}")

        if target_month:
            # 特定月のみ処理
            if not re.match(r'^\d{6}$', target_month):
                return jsonify({
                    "status": "error",
                    "message": f"無効なtarget_month形式: {target_month}"
                }), 400

            result = process_month(target_month, mode)
        else:
            # 全月処理
            result = process_all_months(mode)

        # エラーがある場合は207 Multi-Status
        if result.get("errors") or result.get("total_errors", 0) > 0:
            return jsonify({
                "status": "partial_success",
                "result": result
            }), 207

        return jsonify({
            "status": "success",
            "result": result
        })

    except Exception as e:
        logger.error(f"エンドポイントエラー: {e}")
        log_validation_error("endpoint_error", {
            "error": str(e)
        })
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ============================================================
# エントリポイント
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
