#!/usr/bin/env python3
"""
データバリデーションモジュール

Drive/スプレッドシートからGCSへの連携時のデータ検証を行う。
結果はGoogle Cloud Loggingに出力され、後からSlack等に連携可能。

使用方法:
    from common.validation import DataValidator

    validator = DataValidator(service_name="drive-to-gcs")
    result = validator.validate_dataframe(df, table_name, expected_columns)
    validator.log_result(result)
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

# Google Cloud Logging用のフォーマット
# 構造化ログとしてCloud Loggingで検索・フィルタリング可能

class DataValidator:
    """データバリデーションクラス"""

    # アラートの重要度
    SEVERITY_OK = "INFO"
    SEVERITY_WARNING = "WARNING"
    SEVERITY_ERROR = "ERROR"

    def __init__(self, service_name: str):
        """
        Args:
            service_name: サービス名（drive-to-gcs, spreadsheet-to-bq, gcs-to-bq）
        """
        self.service_name = service_name
        self.logger = logging.getLogger(service_name)

        # Cloud Run環境ではCloud Loggingに自動送信される
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(message)s'  # JSON形式で出力するためシンプルに
            ))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def validate_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        expected_columns: List[str],
        source_file: str = None
    ) -> Dict[str, Any]:
        """
        DataFrameのバリデーションを実行

        Args:
            df: 検証対象のDataFrame
            table_name: テーブル名
            expected_columns: 期待されるカラム名リスト（日本語）
            source_file: ソースファイル名（オプション）

        Returns:
            検証結果の辞書
        """
        errors = []
        warnings = []

        # 1. カラム不整合チェック
        actual_columns = list(df.columns)
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

        # 結果を構築
        has_errors = len(errors) > 0
        result = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": self.service_name,
            "validation_type": "data_ingestion",
            "table_name": table_name,
            "source_file": source_file,
            "status": "ERROR" if has_errors else "OK",
            "row_count": row_count,
            "column_count": len(actual_columns),
            "expected_column_count": len(expected_columns),
            "errors": errors,
            "warnings": warnings
        }

        return result

    def validate_duplicates(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_keys: List[str]
    ) -> Dict[str, Any]:
        """
        重複チェックを実行

        Args:
            df: 検証対象のDataFrame
            table_name: テーブル名
            unique_keys: ユニークキーとなるカラム名リスト

        Returns:
            検証結果の辞書
        """
        errors = []

        # ユニークキーが存在するか確認
        missing_keys = [key for key in unique_keys if key not in df.columns]
        if missing_keys:
            return {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "service": self.service_name,
                "validation_type": "duplicate_check",
                "table_name": table_name,
                "status": "ERROR",
                "errors": [{
                    "type": "MISSING_UNIQUE_KEYS",
                    "message": f"ユニークキーカラムが存在しません: {missing_keys}"
                }]
            }

        # 重複チェック
        duplicates = df[df.duplicated(subset=unique_keys, keep=False)]
        duplicate_count = len(duplicates)

        if duplicate_count > 0:
            # 重複のサンプルを取得（最大5件）
            sample_duplicates = duplicates.head(5)[unique_keys].to_dict('records')
            errors.append({
                "type": "DUPLICATE_RECORDS",
                "message": f"{duplicate_count}件の重複レコードが存在します",
                "details": {
                    "duplicate_count": duplicate_count,
                    "unique_keys": unique_keys,
                    "sample_duplicates": sample_duplicates
                }
            })

        result = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": self.service_name,
            "validation_type": "duplicate_check",
            "table_name": table_name,
            "status": "ERROR" if errors else "OK",
            "total_rows": len(df),
            "duplicate_count": duplicate_count,
            "unique_keys": unique_keys,
            "errors": errors
        }

        return result

    def log_result(self, result: Dict[str, Any]) -> None:
        """
        バリデーション結果をCloud Loggingに出力

        Args:
            result: バリデーション結果の辞書
        """
        # 構造化ログとして出力（Cloud Loggingで検索可能）
        log_entry = {
            "severity": self.SEVERITY_ERROR if result.get("status") == "ERROR" else self.SEVERITY_OK,
            "message": self._format_message(result),
            "labels": {
                "service": self.service_name,
                "table_name": result.get("table_name", "unknown"),
                "validation_type": result.get("validation_type", "unknown"),
                "status": result.get("status", "unknown")
            },
            "jsonPayload": result
        }

        # JSON形式で出力（Cloud Loggingが自動パース）
        if result.get("status") == "ERROR":
            self.logger.error(json.dumps(log_entry, ensure_ascii=False))
        elif result.get("warnings"):
            self.logger.warning(json.dumps(log_entry, ensure_ascii=False))
        else:
            self.logger.info(json.dumps(log_entry, ensure_ascii=False))

    def _format_message(self, result: Dict[str, Any]) -> str:
        """ログメッセージを整形"""
        status = result.get("status", "UNKNOWN")
        table_name = result.get("table_name", "unknown")
        validation_type = result.get("validation_type", "validation")

        if status == "OK":
            row_count = result.get("row_count", result.get("total_rows", 0))
            return f"[{status}] {table_name}: {validation_type} passed ({row_count} rows)"
        else:
            error_count = len(result.get("errors", []))
            return f"[{status}] {table_name}: {validation_type} failed ({error_count} errors)"


class DataMartValidator:
    """DataMart用バリデーションクラス"""

    def __init__(self):
        self.service_name = "datamart-check"
        self.logger = logging.getLogger(self.service_name)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def check_sonota_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        secondary_department='その他' の値が0より大きい場合をチェック

        Args:
            df: management_documents_all_period_all のDataFrame

        Returns:
            検証結果の辞書
        """
        errors = []

        # 'その他' の行をフィルタ
        if 'secondary_department' not in df.columns or 'value' not in df.columns:
            return {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "service": self.service_name,
                "validation_type": "sonota_check",
                "status": "ERROR",
                "errors": [{
                    "type": "MISSING_COLUMNS",
                    "message": "必要なカラム（secondary_department, value）が存在しません"
                }]
            }

        sonota_rows = df[
            (df['secondary_department'] == 'その他') &
            (df['value'] > 0)
        ]

        if len(sonota_rows) > 0:
            # サンプルを取得
            sample_cols = ['date', 'main_category', 'secondary_category', 'value']
            available_cols = [c for c in sample_cols if c in sonota_rows.columns]
            sample_data = sonota_rows.head(10)[available_cols].to_dict('records')

            errors.append({
                "type": "SONOTA_NON_ZERO",
                "message": f"secondary_department='その他' で value > 0 のレコードが {len(sonota_rows)} 件あります",
                "details": {
                    "count": len(sonota_rows),
                    "sample_records": sample_data
                }
            })

        result = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": self.service_name,
            "validation_type": "sonota_check",
            "status": "ERROR" if errors else "OK",
            "sonota_non_zero_count": len(sonota_rows),
            "errors": errors
        }

        return result

    def log_result(self, result: Dict[str, Any]) -> None:
        """バリデーション結果をCloud Loggingに出力"""
        log_entry = {
            "severity": "ERROR" if result.get("status") == "ERROR" else "INFO",
            "message": self._format_message(result),
            "labels": {
                "service": self.service_name,
                "validation_type": result.get("validation_type", "unknown"),
                "status": result.get("status", "unknown")
            },
            "jsonPayload": result
        }

        if result.get("status") == "ERROR":
            self.logger.error(json.dumps(log_entry, ensure_ascii=False))
        else:
            self.logger.info(json.dumps(log_entry, ensure_ascii=False))

    def _format_message(self, result: Dict[str, Any]) -> str:
        """ログメッセージを整形"""
        status = result.get("status", "UNKNOWN")
        validation_type = result.get("validation_type", "validation")

        if status == "OK":
            return f"[{status}] DataMart: {validation_type} passed"
        else:
            count = result.get("sonota_non_zero_count", 0)
            return f"[{status}] DataMart: {validation_type} failed ({count} records with その他 > 0)"
