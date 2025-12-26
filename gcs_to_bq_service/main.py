#!/usr/bin/env python3
"""
gcs-to-bq Cloud Run Service
GCS上のExcelファイルをCSVに変換し、BigQueryにロード

バリデーション機能:
- カラム不整合チェック
- レコード0件チェック
- 重複レコードチェック

結果はGoogle Cloud Loggingに出力され、後からSlack等に連携可能。
"""

import os
import io
import json
import traceback
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, Optional, Any, List


class DateTimeEncoder(json.JSONEncoder):
    """date/datetime オブジェクトを文字列に変換するJSONエンコーダー"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# ============================================================
# 統一ログ設定
# ============================================================

# バリデーション有効化フラグ
VALIDATION_ENABLED = os.environ.get("VALIDATION_ENABLED", "true").lower() == "true"

# パイプライン識別用の設定
PIPELINE_ID = "data-pipeline"
STEP_NAME = "gcs-to-bq"

# 統一ログ用のlogger
pipeline_logger = logging.getLogger("pipeline-logger")
if not pipeline_logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    pipeline_logger.addHandler(handler)
    pipeline_logger.setLevel(logging.INFO)


def get_execution_id() -> str:
    """
    実行IDを取得
    環境変数から取得、なければリクエストごとにタイムスタンプで生成
    """
    return os.environ.get("EXECUTION_ID", datetime.utcnow().strftime("%Y%m%d_%H%M%S"))


def log_pipeline_event(
    action: str,
    status: str = "INFO",
    message: str = "",
    table_name: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    execution_id: Optional[str] = None
) -> None:
    """
    統一形式でパイプラインログを出力

    Args:
        action: 実行中のアクション（例: "load", "transform", "validation"）
        status: ステータス（"INFO", "OK", "ERROR", "WARNING"）
        message: ログメッセージ
        table_name: 対象テーブル名（オプション）
        details: 詳細情報（オプション）
        execution_id: 実行ID（オプション、指定なければ自動生成）
    """
    severity = "ERROR" if status == "ERROR" else "WARNING" if status == "WARNING" else "INFO"
    exec_id = execution_id or get_execution_id()

    log_entry = {
        "severity": severity,
        "message": message,
        "labels": {
            "pipeline_id": PIPELINE_ID,
            "execution_id": exec_id,
            "step": STEP_NAME,
            "action": action,
            "status": status
        },
        "jsonPayload": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "pipeline_id": PIPELINE_ID,
            "execution_id": exec_id,
            "step": STEP_NAME,
            "action": action,
            "status": status,
            "message": message
        }
    }

    if table_name:
        log_entry["labels"]["table_name"] = table_name
        log_entry["jsonPayload"]["table_name"] = table_name

    if details:
        log_entry["jsonPayload"]["details"] = details

    if severity == "ERROR":
        pipeline_logger.error(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))
    elif severity == "WARNING":
        pipeline_logger.warning(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))
    else:
        pipeline_logger.info(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))


# 後方互換性のためのエイリアス
validation_logger = pipeline_logger

# テーブルごとのユニークキー定義（重複チェック用）
UNIQUE_KEYS_CONFIG = {
    "sales_target_and_achievements": ["sales_accounting_period", "branch_code", "department_code", "staff_code"],
    "billing_balance": ["sales_month", "branch_code", "branch_name", "source_folder"],
    "ledger_income": ["slip_date", "slip_number", "line_number"],
    "ledger_loss": ["accounting_month", "slip_number", "line_number"],
    "department_summary": ["sales_accounting_period", "code"],
    "internal_interest": ["year_month", "branch", "category"],
    "profit_plan_term": ["period", "item"],
    "profit_plan_term_nagasaki": ["period", "item"],
    "profit_plan_term_fukuoka": ["period", "item"],
    "stocks": ["year_month", "branch", "category"],
    "ms_allocation_ratio": ["year_month", "branch", "department", "category"],
    "customer_sales_target_and_achievements": ["sales_accounting_period", "branch_code", "customer_code"],
    "construction_progress_days_amount": ["property_period", "branch_code", "staff_code", "property_number", "customer_code", "contract_date"],
    "construction_progress_days_final_date": ["final_billing_sales_date", "property_number", "property_data_classification"],
}

# 環境変数
PROJECT_ID = os.environ.get("GCP_PROJECT", "data-platform-prod-475201")
DATASET_ID = "corporate_data"
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "data-platform-landing-prod")
COLUMNS_PATH = "google-drive/config/columns"
MAPPING_FILE = "google-drive/config/mapping/mapping_files.csv"
MONETARY_SCALE_FILE = "google-drive/config/mapping/monetary_scale_conversion.csv"
ZERO_DATE_FILE = "google-drive/config/mapping/zero_date_to_null.csv"

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
# 累積型テーブルの定義
# ============================================================
# 各CSVが全期間のデータを含むテーブル
# キー毎に最新フォルダ（max(source_folder)）のデータを優先してロード
CUMULATIVE_TABLE_CONFIG = {
    "profit_plan_term": {
        # ソース: 12_損益目標.xlsx（東京支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "profit_plan_term_nagasaki": {
        # ソース: 12_損益目標.xlsx（長崎支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "profit_plan_term_fukuoka": {
        # ソース: 12_損益目標.xlsx（福岡支店目標103期シート）
        "unique_keys": ["period", "item"],
    },
    "ms_allocation_ratio": {
        # ソース: 10_案分比率マスタ.xlsx
        "unique_keys": ["year_month", "branch", "department", "category"],
    },
    "construction_progress_days_amount": {
        # ソース: 工事進捗日数金額.xlsx
        "unique_keys": ["property_period", "branch_code", "staff_code", "property_number", "customer_code", "contract_date"],
    },
    "stocks": {
        # ソース: 9_在庫.xlsx
        "unique_keys": ["year_month", "branch", "department", "category"],
    },
    "construction_progress_days_final_date": {
        # ソース: 工事進捗日数最終日.xlsx
        "unique_keys": ["final_billing_sales_date", "property_number", "property_data_classification"],
    },
}

# ============================================================
# スプレッドシート連携テーブルの定義
# ============================================================
# スプレッドシートから連携されるテーブル
# パス: gs://data-platform-landing-prod/spreadsheet/proceed/
SPREADSHEET_PROCEED_PATH = "spreadsheet/proceed"
SPREADSHEET_COLUMNS_PATH = "spreadsheet/config/columns"
SPREADSHEET_TABLE_PREFIX = "ss_"

SPREADSHEET_TABLE_CONFIG = {
    "gs_sales_profit": {
        "description": "GS売上利益",
        "bq_table_name": "ss_gs_sales_profit",
    },
    "inventory_advance_tokyo": {
        "description": "東京在庫前払",
        "bq_table_name": "ss_inventory_advance_tokyo",
    },
    "inventory_advance_nagasaki": {
        "description": "長崎在庫前払",
        "bq_table_name": "ss_inventory_advance_nagasaki",
    },
    "inventory_advance_fukuoka": {
        "description": "福岡在庫前払",
        "bq_table_name": "ss_inventory_advance_fukuoka",
    },
}

# スプレッドシートテーブルのユニークキー定義（バリデーション用）
SPREADSHEET_UNIQUE_KEYS_CONFIG = {
    "ss_gs_sales_profit": [],  # ユニークキー未定義
    "ss_inventory_advance_tokyo": ["posting_month", "branch_name", "sales_office", "category"],
    "ss_inventory_advance_nagasaki": ["posting_month", "branch_name", "sales_office", "category"],
    "ss_inventory_advance_fukuoka": ["posting_month", "branch_name", "sales_office", "category"],
}

# ============================================================
# バリデーション関数
# ============================================================

def log_validation_result(result: Dict[str, Any]) -> None:
    """
    バリデーション結果をCloud Loggingに出力

    構造化ログとしてCloud Loggingで検索・フィルタリング可能。
    ログは以下のラベルでフィルタ可能:
    - labels.service: "gcs-to-bq"
    - labels.validation_type: "column_check" / "empty_check" / "duplicate_check"
    - labels.status: "OK" / "ERROR"
    """
    log_entry = {
        "severity": "ERROR" if result.get("status") == "ERROR" else "INFO",
        "message": _format_validation_message(result),
        "labels": {
            "service": "gcs-to-bq",
            "table_name": result.get("table_name", "unknown"),
            "validation_type": result.get("validation_type", "unknown"),
            "status": result.get("status", "unknown")
        },
        "jsonPayload": result
    }

    if result.get("status") == "ERROR":
        validation_logger.error(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))
    elif result.get("warnings"):
        validation_logger.warning(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))
    else:
        validation_logger.info(json.dumps(log_entry, ensure_ascii=False, cls=DateTimeEncoder))


def _format_validation_message(result: Dict[str, Any]) -> str:
    """ログメッセージを整形"""
    status = result.get("status", "UNKNOWN")
    table_name = result.get("table_name", "unknown")
    validation_type = result.get("validation_type", "validation")

    if status == "OK":
        row_count = result.get("row_count", result.get("total_rows", 0))
        return f"[VALIDATION {status}] {table_name}: {validation_type} passed ({row_count} rows)"
    else:
        error_count = len(result.get("errors", []))
        return f"[VALIDATION {status}] {table_name}: {validation_type} failed ({error_count} errors)"


def validate_columns_and_rows(
    df: pd.DataFrame,
    table_name: str,
    expected_columns: List[str],
    source_file: str = None
) -> Dict[str, Any]:
    """
    カラム不整合とレコード0件をチェック

    Args:
        df: 検証対象のDataFrame（日本語カラム名）
        table_name: テーブル名
        expected_columns: 期待されるカラム名リスト（日本語）
        source_file: ソースファイル名

    Returns:
        検証結果の辞書
    """
    errors = []
    warnings = []

    # カラム名の改行を除去してから比較
    actual_columns = [str(col).replace('\n', '') for col in df.columns]

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
    row_count = len(df)
    if row_count == 0:
        errors.append({
            "type": "EMPTY_DATA",
            "message": "データが0件です"
        })

    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "gcs-to-bq",
        "validation_type": "column_and_row_check",
        "table_name": table_name,
        "source_file": source_file,
        "status": "ERROR" if errors else "OK",
        "row_count": row_count,
        "column_count": len(actual_columns),
        "expected_column_count": len(expected_columns),
        "errors": errors,
        "warnings": warnings
    }

    return result


def validate_duplicates_in_bq(
    bq_client: bigquery.Client,
    table_name: str
) -> Dict[str, Any]:
    """
    BigQueryテーブルの重複をチェック

    Args:
        bq_client: BigQueryクライアント
        table_name: テーブル名

    Returns:
        検証結果の辞書
    """
    errors = []

    # ユニークキー定義を取得
    unique_keys = UNIQUE_KEYS_CONFIG.get(table_name)
    if not unique_keys:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "duplicate_check",
            "table_name": table_name,
            "status": "SKIPPED",
            "message": "ユニークキーが定義されていません"
        }

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    key_cols = ", ".join(unique_keys)

    # 重複チェッククエリ
    query = f"""
    SELECT {key_cols}, COUNT(*) as duplicate_count
    FROM `{table_id}`
    GROUP BY {key_cols}
    HAVING COUNT(*) > 1
    LIMIT 10
    """

    try:
        result = bq_client.query(query).result()
        duplicates = [dict(row) for row in result]
        duplicate_count = len(duplicates)

        if duplicate_count > 0:
            errors.append({
                "type": "DUPLICATE_RECORDS",
                "message": f"重複レコードが存在します（サンプル: {duplicate_count}件）",
                "details": {
                    "unique_keys": unique_keys,
                    "sample_duplicates": duplicates
                }
            })

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "duplicate_check",
            "table_name": table_name,
            "status": "ERROR" if errors else "OK",
            "unique_keys": unique_keys,
            "duplicate_sample_count": duplicate_count,
            "errors": errors
        }

    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "duplicate_check",
            "table_name": table_name,
            "status": "ERROR",
            "errors": [{
                "type": "QUERY_ERROR",
                "message": f"重複チェッククエリ実行エラー: {str(e)}"
            }]
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


def load_zero_date_config(storage_client: storage.Client) -> pd.DataFrame:
    """GCSからゼロ日付変換設定を読み込み"""
    try:
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(ZERO_DATE_FILE)

        if not blob.exists():
            print(f"⚠️  ゼロ日付変換設定ファイルが見つかりません: gs://{LANDING_BUCKET}/{ZERO_DATE_FILE}")
            return pd.DataFrame()

        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
        return df
    except Exception as e:
        print(f"⚠️  ゼロ日付変換設定の読み込みエラー: {e}")
        return pd.DataFrame()


def apply_zero_date_to_null_conversion(
    df: pd.DataFrame,
    table_name: str,
    storage_client: storage.Client
) -> pd.DataFrame:
    """
    ゼロ日付（0000/00/00）をnullに変換

    Args:
        df: 変換対象のDataFrame（英語カラム名に変換済み）
        table_name: テーブル名
        storage_client: Storage Client

    Returns:
        変換後のDataFrame
    """
    try:
        # ゼロ日付変換設定を読み込み
        config_df = load_zero_date_config(storage_client)

        if config_df.empty:
            return df

        # 対象テーブルの設定を取得
        target_config = config_df[config_df['file_name'] == table_name]

        if target_config.empty:
            return df

        df = df.copy()

        # ゼロ日付パターン（様々な形式に対応）
        zero_date_patterns = [
            '0000/00/00',
            '0000-00-00',
            '0000/0/0',
            '0000-0-0',
        ]

        for _, config in target_config.iterrows():
            column_name = config['condition_column_name']

            if column_name not in df.columns:
                print(f"⚠️  対象カラムが存在しません: {column_name}")
                continue

            # 変換前のnull以外の件数を記録
            non_null_before = df[column_name].notna().sum()

            # ゼロ日付をnullに変換
            for pattern in zero_date_patterns:
                mask = df[column_name].astype(str).str.strip() == pattern
                if mask.any():
                    df.loc[mask, column_name] = None

            # 変換後のnull以外の件数
            non_null_after = df[column_name].notna().sum()
            converted_count = non_null_before - non_null_after

            if converted_count > 0:
                print(f"   🔄 {column_name}: {converted_count}件のゼロ日付をnullに変換")

        return df

    except Exception as e:
        print(f"⚠️  ゼロ日付変換エラー: {e}")
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

        # google-drive/raw/ から読み込み
        raw_path = f"google-drive/raw/{yyyymm}/{table_name}.xlsx"
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

        # ============================================================
        # バリデーション: カラム不整合・レコード0件チェック
        # ============================================================
        if VALIDATION_ENABLED:
            expected_columns = list(column_mapping.keys())
            validation_result = validate_columns_and_rows(
                df=df,
                table_name=table_name,
                expected_columns=expected_columns,
                source_file=raw_path
            )
            log_validation_result(validation_result)

            # エラーがある場合は警告を出すが処理は続行
            if validation_result.get("status") == "ERROR":
                for error in validation_result.get("errors", []):
                    print(f"   ⚠️  バリデーションエラー: {error.get('message')}")
            else:
                print(f"   ✅ バリデーションOK: カラム・レコード数チェック passed")

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

        # ゼロ日付をnullに変換（金額変換後に実行）
        df = apply_zero_date_to_null_conversion(df, table_name, storage_client)

        # source_folderカラムを追加（どのフォルダから取得したかを識別）
        df["source_folder"] = int(yyyymm)
        print(f"   ➕ source_folder={yyyymm} を追加")

        # CSV出力
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        # google-drive/proceed/ に保存
        proceed_path = f"google-drive/proceed/{yyyymm}/{table_name}.csv"
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
    yyyymm: str = None  # 未使用（後方互換性のため残す）
) -> bool:
    """2024年9月以降のパーティションデータを全て削除（冪等性保証）"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    partition_field = TABLE_CONFIG[table_name]["partition_field"]

    # 期首: 2024年9月1日以降を全て削除
    start_date = "2024-09-01"

    # slip_date や final_billing_sales_date など、月の1日以外の日付が入るフィールドは
    # 日付比較で対応
    tables_with_non_first_day_dates = [
        "ledger_income",
        "ledger_loss",
        "construction_progress_days_final_date"
    ]

    if table_name in tables_with_non_first_day_dates:
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE {partition_field} >= '{start_date}'
        """
    else:
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE {partition_field} >= '{start_date}'
        """

    try:
        print(f"   🗑️  既存データ削除中: {start_date}以降")
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
    yyyymm: str,
    execution_id: str = None
) -> bool:
    """CSVファイルをBigQueryにロード"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    gcs_uri = f"gs://{LANDING_BUCKET}/google-drive/proceed/{yyyymm}/{table_name}.csv"
    exec_id = execution_id or get_execution_id()

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

        # 統一ログ出力
        log_pipeline_event(
            action="load_table",
            status="OK",
            message=f"テーブル {table_name} のロード完了",
            table_name=table_name,
            details={
                "yyyymm": yyyymm,
                "rows_added": load_job.output_rows,
                "total_rows": destination_table.num_rows,
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )

        return True

    except GoogleCloudError as e:
        # ファイルが存在しない場合はスキップ（エラーではなく警告）
        error_str = str(e)
        if "Not found" in error_str or "notFound" in error_str:
            print(f"   ⚠️  ファイルが存在しないためスキップ: {gcs_uri}")
            log_pipeline_event(
                action="load_table",
                status="SKIPPED",
                message=f"テーブル {table_name} のファイルが存在しないためスキップ",
                table_name=table_name,
                details={
                    "yyyymm": yyyymm,
                    "reason": "FILE_NOT_FOUND",
                    "gcs_uri": gcs_uri
                },
                execution_id=exec_id
            )
            return None  # スキップを示す

        print(f"   ❌ ロードエラー: {e}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"      詳細: {error}")

        # エラーログ出力
        log_pipeline_event(
            action="load_table",
            status="ERROR",
            message=f"テーブル {table_name} のロードに失敗",
            table_name=table_name,
            details={
                "yyyymm": yyyymm,
                "error": str(e),
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )
        return False
    except Exception as e:
        print(f"   ❌ 予期しないエラー: {e}")

        # エラーログ出力
        log_pipeline_event(
            action="load_table",
            status="ERROR",
            message=f"テーブル {table_name} のロードで予期しないエラー",
            table_name=table_name,
            details={
                "yyyymm": yyyymm,
                "error": str(e),
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )
        return False


def process_cumulative_table(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    table_name: str,
    target_months: list,
    execution_id: str = None
) -> bool:
    """
    累積型テーブルのロード処理

    全月のCSVを読み込み、source_folderカラムを追加して結合。
    キー毎にmax(source_folder)のデータを優先して重複を解消。

    Args:
        bq_client: BigQueryクライアント
        storage_client: GCSクライアント
        table_name: テーブル名
        target_months: 対象年月リスト
        execution_id: 実行ID（オプション）

    Returns:
        成功時True
    """
    exec_id = execution_id or get_execution_id()
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

    # 一時CSVに保存
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_csv = f.name
        deduped_df.to_csv(f, index=False)

    # GCSにアップロード
    temp_blob = bucket.blob(f"google-drive/temp/{table_name}_cumulative.csv")
    temp_blob.upload_from_filename(temp_csv)
    gcs_uri = f"gs://{LANDING_BUCKET}/google-drive/temp/{table_name}_cumulative.csv"

    # BigQueryにロード
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        # 累積型テーブルは全データを削除（CSVに全履歴が含まれるため）
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE TRUE
        """
        query_job = bq_client.query(delete_query)
        query_job.result()
        deleted = query_job.num_dml_affected_rows or 0
        print(f"   🗑️  既存データ削除（全件）: {deleted}行")

        # スキーマを取得してsource_folderカラムを追加（存在しない場合）
        table = bq_client.get_table(table_id)
        existing_schema = list(table.schema)
        has_source_folder = any(f.name == "source_folder" for f in existing_schema)

        if not has_source_folder:
            new_schema = existing_schema + [bigquery.SchemaField("source_folder", "INTEGER")]
            table.schema = new_schema
            bq_client.update_table(table, ["schema"])
            print(f"   ➕ source_folderカラムを追加")

        # ロード
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
        )
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result(timeout=300)
        print(f"   ✅ ロード完了: {load_job.output_rows}行")

        # 一時ファイル削除
        temp_blob.delete()
        import os
        os.remove(temp_csv)

        # テーブルとカラムの説明を更新
        update_table_and_column_descriptions(bq_client, storage_client, table_name)

        # 統一ログ出力
        log_pipeline_event(
            action="load_cumulative_table",
            status="OK",
            message=f"累積型テーブル {table_name} のロード完了",
            table_name=table_name,
            details={
                "target_months": target_months,
                "rows_loaded": load_job.output_rows,
                "unique_keys": unique_keys
            },
            execution_id=exec_id
        )

        return True

    except Exception as e:
        print(f"   ❌ エラー: {e}")
        traceback.print_exc()

        # エラーログ出力
        log_pipeline_event(
            action="load_cumulative_table",
            status="ERROR",
            message=f"累積型テーブル {table_name} のロードに失敗",
            table_name=table_name,
            details={
                "target_months": target_months,
                "error": str(e)
            },
            execution_id=exec_id
        )
        return False


# ============================================================
# スプレッドシート → BigQuery ロード処理
# ============================================================

def load_spreadsheet_column_schema(
    storage_client: storage.Client,
    table_name: str
) -> List[bigquery.SchemaField]:
    """
    スプレッドシートのカラム定義からBigQueryスキーマを生成

    Args:
        storage_client: GCSクライアント
        table_name: テーブル名（ss_プレフィックスなし）

    Returns:
        BigQueryスキーマフィールドのリスト
    """
    try:
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(f"{SPREADSHEET_COLUMNS_PATH}/{table_name}.csv")

        if not blob.exists():
            print(f"⚠️  スプレッドシートカラム定義が見つかりません: {table_name}.csv")
            return []

        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))

        # data_type → BigQuery型のマッピング
        type_mapping = {
            "STRING": "STRING",
            "INTEGER": "INTEGER",
            "INT64": "INTEGER",
            "FLOAT": "FLOAT",
            "NUMERIC": "NUMERIC",
            "DATE": "DATE",
            "DATETIME": "DATETIME",
            "TIMESTAMP": "TIMESTAMP",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
        }

        schema = []
        for _, row in df.iterrows():
            en_name = row['en_name']
            data_type = row.get('data_type', 'STRING')
            bq_type = type_mapping.get(data_type.upper(), 'STRING')
            jp_name = row.get('jp_name', en_name)

            schema.append(bigquery.SchemaField(
                name=en_name,
                field_type=bq_type,
                mode="NULLABLE",
                description=jp_name
            ))

        return schema

    except Exception as e:
        print(f"⚠️  スプレッドシートスキーマ読み込みエラー: {e}")
        return []


def load_spreadsheet_to_bigquery(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    table_name: str,
    execution_id: str = None
) -> bool:
    """
    スプレッドシートCSVをBigQueryにロード（全データ洗い替え）

    Args:
        bq_client: BigQueryクライアント
        storage_client: GCSクライアント
        table_name: テーブル名（ss_プレフィックスなし）
        execution_id: 実行ID

    Returns:
        成功時True
    """
    exec_id = execution_id or get_execution_id()
    config = SPREADSHEET_TABLE_CONFIG.get(table_name)

    if not config:
        print(f"⚠️  未定義のスプレッドシートテーブル: {table_name}")
        return False

    bq_table_name = config["bq_table_name"]
    description = config["description"]
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_table_name}"
    gcs_uri = f"gs://{LANDING_BUCKET}/{SPREADSHEET_PROCEED_PATH}/{table_name}.csv"

    print(f"\n📊 スプレッドシート処理中: {table_name} → {bq_table_name}")

    try:
        # GCSファイルの存在確認
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(f"{SPREADSHEET_PROCEED_PATH}/{table_name}.csv")

        if not blob.exists():
            print(f"   ⚠️  CSVファイルが見つかりません: {gcs_uri}")
            log_pipeline_event(
                action="load_spreadsheet",
                status="WARNING",
                message=f"スプレッドシートCSVが見つかりません",
                table_name=bq_table_name,
                details={"gcs_uri": gcs_uri},
                execution_id=exec_id
            )
            return False

        # CSVデータを読み込んでバリデーション
        csv_content = blob.download_as_string().decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_content))
        row_count = len(df)

        print(f"   📁 データ: {row_count}行 × {len(df.columns)}列")

        # カラム・レコード数バリデーション
        if VALIDATION_ENABLED:
            # スキーマからカラム名リストを取得
            schema = load_spreadsheet_column_schema(storage_client, table_name)
            expected_columns = [field.name for field in schema]

            if expected_columns:
                validation_result = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "service": "gcs-to-bq",
                    "validation_type": "spreadsheet_column_check",
                    "table_name": bq_table_name,
                    "source_file": gcs_uri,
                    "status": "OK",
                    "row_count": row_count,
                    "column_count": len(df.columns),
                    "expected_column_count": len(expected_columns),
                    "errors": [],
                    "warnings": []
                }

                actual_columns = list(df.columns)
                missing_columns = [col for col in expected_columns if col not in actual_columns]
                extra_columns = [col for col in actual_columns if col not in expected_columns]

                if missing_columns:
                    validation_result["errors"].append({
                        "type": "MISSING_COLUMNS",
                        "message": f"期待されるカラムが存在しません: {missing_columns}",
                        "details": {"missing": missing_columns}
                    })
                    validation_result["status"] = "ERROR"

                if extra_columns:
                    validation_result["warnings"].append({
                        "type": "EXTRA_COLUMNS",
                        "message": f"定義外のカラムが存在します: {extra_columns}",
                        "details": {"extra": extra_columns}
                    })

                if row_count == 0:
                    validation_result["errors"].append({
                        "type": "EMPTY_DATA",
                        "message": "データが0件です"
                    })
                    validation_result["status"] = "ERROR"

                log_validation_result(validation_result)

                if validation_result.get("status") == "ERROR":
                    for error in validation_result.get("errors", []):
                        print(f"   ⚠️  バリデーションエラー: {error.get('message')}")
                else:
                    print(f"   ✅ バリデーションOK: カラム・レコード数チェック passed")

        # スキーマを取得
        schema = load_spreadsheet_column_schema(storage_client, table_name)

        # BigQueryジョブ設定
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 全データ洗い替え
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=False,
            max_bad_records=0,
        )

        # スキーマがある場合は設定
        if schema:
            job_config.schema = schema
        else:
            job_config.autodetect = True

        # BigQueryにロード
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        print(f"   ⏳ ロード開始: {bq_table_name} (Job ID: {load_job.job_id})")

        load_job.result(timeout=300)

        destination_table = bq_client.get_table(table_id)
        print(f"   ✅ ロード完了: {load_job.output_rows} 行")

        # テーブルの説明を設定
        destination_table.description = description
        bq_client.update_table(destination_table, ["description"])
        print(f"   📝 テーブル説明を設定: {description}")

        # 統一ログ出力
        log_pipeline_event(
            action="load_spreadsheet",
            status="OK",
            message=f"スプレッドシートテーブル {bq_table_name} のロード完了",
            table_name=bq_table_name,
            details={
                "source_table": table_name,
                "rows_loaded": load_job.output_rows,
                "total_rows": destination_table.num_rows,
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )

        return True

    except GoogleCloudError as e:
        print(f"   ❌ ロードエラー: {e}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"      詳細: {error}")

        log_pipeline_event(
            action="load_spreadsheet",
            status="ERROR",
            message=f"スプレッドシートテーブル {bq_table_name} のロードに失敗",
            table_name=bq_table_name,
            details={
                "source_table": table_name,
                "error": str(e),
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )
        return False

    except Exception as e:
        print(f"   ❌ 予期しないエラー: {e}")
        traceback.print_exc()

        log_pipeline_event(
            action="load_spreadsheet",
            status="ERROR",
            message=f"スプレッドシートテーブル {bq_table_name} のロードで予期しないエラー",
            table_name=bq_table_name,
            details={
                "source_table": table_name,
                "error": str(e),
                "gcs_uri": gcs_uri
            },
            execution_id=exec_id
        )
        return False


def validate_spreadsheet_duplicates_in_bq(
    bq_client: bigquery.Client,
    bq_table_name: str
) -> Dict[str, Any]:
    """
    スプレッドシートテーブルの重複をチェック

    Args:
        bq_client: BigQueryクライアント
        bq_table_name: BigQueryテーブル名（ss_プレフィックス付き）

    Returns:
        検証結果の辞書
    """
    errors = []

    # ユニークキー定義を取得
    unique_keys = SPREADSHEET_UNIQUE_KEYS_CONFIG.get(bq_table_name, [])
    if not unique_keys:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "spreadsheet_duplicate_check",
            "table_name": bq_table_name,
            "status": "SKIPPED",
            "message": "ユニークキーが定義されていません"
        }

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_table_name}"
    key_cols = ", ".join(unique_keys)

    # 重複チェッククエリ
    query = f"""
    SELECT {key_cols}, COUNT(*) as duplicate_count
    FROM `{table_id}`
    GROUP BY {key_cols}
    HAVING COUNT(*) > 1
    LIMIT 10
    """

    try:
        result = bq_client.query(query).result()
        duplicates = [dict(row) for row in result]
        duplicate_count = len(duplicates)

        if duplicate_count > 0:
            errors.append({
                "type": "DUPLICATE_RECORDS",
                "message": f"重複レコードが存在します（サンプル: {duplicate_count}件）",
                "details": {
                    "unique_keys": unique_keys,
                    "sample_duplicates": duplicates
                }
            })

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "spreadsheet_duplicate_check",
            "table_name": bq_table_name,
            "status": "ERROR" if errors else "OK",
            "unique_keys": unique_keys,
            "duplicate_sample_count": duplicate_count,
            "errors": errors
        }

    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "gcs-to-bq",
            "validation_type": "spreadsheet_duplicate_check",
            "table_name": bq_table_name,
            "status": "ERROR",
            "errors": [{
                "type": "QUERY_ERROR",
                "message": f"重複チェッククエリ実行エラー: {str(e)}"
            }]
        }


def process_spreadsheet_tables(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    tables: List[str] = None,
    execution_id: str = None
) -> Dict[str, Any]:
    """
    スプレッドシートテーブルを一括処理

    Args:
        bq_client: BigQueryクライアント
        storage_client: GCSクライアント
        tables: 処理対象テーブルリスト（省略時は全テーブル）
        execution_id: 実行ID

    Returns:
        処理結果の辞書
    """
    exec_id = execution_id or get_execution_id()
    target_tables = tables or list(SPREADSHEET_TABLE_CONFIG.keys())

    print("\n" + "=" * 60)
    print(f"スプレッドシート → BigQuery ロード処理")
    print(f"対象テーブル: {', '.join(target_tables)}")
    print("=" * 60)

    # 処理開始ログ
    log_pipeline_event(
        action="spreadsheet_load_start",
        status="INFO",
        message=f"スプレッドシートロード処理を開始",
        details={
            "tables": target_tables,
            "table_count": len(target_tables)
        },
        execution_id=exec_id
    )

    success_count = 0
    error_count = 0
    results = []

    for table_name in target_tables:
        config = SPREADSHEET_TABLE_CONFIG.get(table_name)
        if not config:
            print(f"⚠️  未定義のテーブル: {table_name}")
            error_count += 1
            results.append({"table": table_name, "status": "error", "reason": "undefined"})
            continue

        bq_table_name = config["bq_table_name"]

        # ロード実行
        if load_spreadsheet_to_bigquery(bq_client, storage_client, table_name, exec_id):
            # 重複チェック
            if VALIDATION_ENABLED:
                dup_result = validate_spreadsheet_duplicates_in_bq(bq_client, bq_table_name)
                log_validation_result(dup_result)

                if dup_result.get("status") == "ERROR":
                    for error in dup_result.get("errors", []):
                        print(f"   ⚠️  重複チェックエラー: {error.get('message')}")
                elif dup_result.get("status") == "SKIPPED":
                    print(f"   ⏭️  重複チェックスキップ: ユニークキー未定義")
                else:
                    print(f"   ✅ バリデーションOK: 重複チェック passed")

            success_count += 1
            results.append({"table": table_name, "bq_table": bq_table_name, "status": "success"})
        else:
            error_count += 1
            results.append({"table": table_name, "bq_table": bq_table_name, "status": "error"})

    print("\n" + "=" * 60)
    print(f"スプレッドシート処理完了: 成功 {success_count} / エラー {error_count}")
    print("=" * 60)

    # 処理完了ログ
    final_status = "OK" if error_count == 0 else "WARNING"
    log_pipeline_event(
        action="spreadsheet_load_complete",
        status=final_status,
        message=f"スプレッドシートロード処理が完了",
        details={
            "success_count": success_count,
            "error_count": error_count,
            "results": results
        },
        execution_id=exec_id
    )

    return {
        "success_count": success_count,
        "error_count": error_count,
        "results": results
    }


# ============================================================
# ユーティリティ関数
# ============================================================

# 期首（データ開始日）
FISCAL_START_YYYYMM = "202409"  # 2024年9月

def get_available_months_from_gcs(storage_client: storage.Client) -> list:
    """GCSのgoogle-drive/proceed/フォルダから利用可能な年月リストを取得（2024/9以降）"""
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
        "yyyymm": "202509",  # 省略時は2024/9以降の全年月を処理
        "tables": ["sales_target_and_achievements"],
        "replace": true
    }

    注意: 冪等性を保証するため、2024/9以降のデータは全て削除されてから追加されます。
    """
    exec_id = get_execution_id()

    try:
        payload = request.get_json(force=True, silent=True) or {}
        yyyymm = payload.get("yyyymm")  # 省略可能
        tables = payload.get("tables", list(TABLE_CONFIG.keys()))

        bq_client = bigquery.Client(project=PROJECT_ID)
        storage_client = storage.Client()

        # 対象年月リストを決定
        if yyyymm:
            # 特定月が指定された場合でも、2024/9以降の全データを処理
            target_months = get_available_months_from_gcs(storage_client)
            print(f"指定月: {yyyymm}（ただし2024/9以降の全データを処理）")
        else:
            # 省略時は2024/9以降の全年月
            target_months = get_available_months_from_gcs(storage_client)

        print("=" * 60)
        print(f"proceed/ → BigQuery ロード処理")
        print(f"対象年月: {', '.join(target_months)}")
        print(f"モード: REPLACE（2024/9以降のデータを全て削除して再ロード）")
        print("=" * 60)

        # 処理開始ログ
        log_pipeline_event(
            action="load_start",
            status="INFO",
            message=f"GCS → BigQueryロード処理を開始",
            details={
                "target_months": target_months,
                "tables": tables,
                "table_count": len(tables)
            },
            execution_id=exec_id
        )

        success_count = 0
        error_count = 0
        results = []

        for table_name in tables:
            # 累積型テーブルかどうかで処理を分岐
            if table_name in CUMULATIVE_TABLE_CONFIG:
                # 累積型テーブル: 専用処理（source_folder追加、重複除去）
                table_success = process_cumulative_table(
                    bq_client, storage_client, table_name, target_months, exec_id
                )
            else:
                # 単月型テーブル: 従来の処理
                print(f"\n📊 処理中（単月型）: {table_name}")

                # 2024/9以降のデータを全て削除（テーブルごとに1回だけ）
                delete_partition_data(bq_client, table_name)

                # 全年月のCSVをロード
                table_success = True
                for month in target_months:
                    result = load_csv_to_bigquery(bq_client, table_name, month, exec_id)
                    if result is False:  # None（スキップ）は成功として扱う
                        table_success = False

                if table_success:
                    # テーブルとカラムの説明を更新
                    update_table_and_column_descriptions(bq_client, storage_client, table_name)

            if table_success:
                # ============================================================
                # バリデーション: 重複チェック
                # ============================================================
                if VALIDATION_ENABLED:
                    dup_result = validate_duplicates_in_bq(bq_client, table_name)
                    log_validation_result(dup_result)

                    if dup_result.get("status") == "ERROR":
                        for error in dup_result.get("errors", []):
                            print(f"   ⚠️  重複チェックエラー: {error.get('message')}")
                    elif dup_result.get("status") == "SKIPPED":
                        print(f"   ⏭️  重複チェックスキップ: ユニークキー未定義")
                    else:
                        print(f"   ✅ バリデーションOK: 重複チェック passed")

                success_count += 1
                results.append({"table": table_name, "status": "success"})
            else:
                error_count += 1
                results.append({"table": table_name, "status": "error"})

        print("\n" + "=" * 60)
        print(f"Drive処理完了: 成功 {success_count} / エラー {error_count}")
        print("=" * 60)

        # ============================================================
        # スプレッドシートテーブルのロード処理
        # ============================================================
        spreadsheet_result = process_spreadsheet_tables(
            bq_client, storage_client, execution_id=exec_id
        )

        # 全体の結果を集計
        total_success = success_count + spreadsheet_result["success_count"]
        total_error = error_count + spreadsheet_result["error_count"]

        # 処理完了ログ
        final_status = "OK" if total_error == 0 else "WARNING"
        log_pipeline_event(
            action="load_complete",
            status=final_status,
            message=f"GCS → BigQueryロード処理が完了",
            details={
                "target_months": target_months,
                "drive_success_count": success_count,
                "drive_error_count": error_count,
                "spreadsheet_success_count": spreadsheet_result["success_count"],
                "spreadsheet_error_count": spreadsheet_result["error_count"],
                "total_success_count": total_success,
                "total_error_count": total_error,
                "drive_results": results,
                "spreadsheet_results": spreadsheet_result["results"]
            },
            execution_id=exec_id
        )

        return jsonify({
            "status": "completed",
            "target_months": target_months,
            "drive": {
                "success": success_count,
                "error": error_count,
                "results": results
            },
            "spreadsheet": spreadsheet_result,
            "total_success": total_success,
            "total_error": total_error
        }), 200

    except Exception as e:
        traceback.print_exc()

        # エラーログ
        log_pipeline_event(
            action="load_complete",
            status="ERROR",
            message=f"GCS → BigQueryロード処理でエラーが発生",
            details={"error": str(e)},
            execution_id=exec_id
        )

        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def health():
    """ヘルスチェック"""
    return "gcs-to-bq service is running", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
