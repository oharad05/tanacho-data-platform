"""
パイプラインアラート用ログユーティリティ

Cloud Logging で検知・Cloud Monitoring でアラートを発火するための
構造化ログを出力します。

使用方法:
    from common.alert_logger import log_alert, AlertType

    # エラーログ
    log_alert(
        alert_type=AlertType.EMPTY_DATA,
        service="drive-to-gcs",
        target="sales_target.xlsx",
        message="取り込み件数が0件です"
    )

    # 成功ログ
    log_success(
        service="drive-to-gcs",
        message="同期処理が正常に完了しました",
        details={"processed": 10, "skipped": 2}
    )
"""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

# ロガー設定
alert_logger = logging.getLogger("pipeline-alert")
if not alert_logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    alert_logger.addHandler(handler)
    alert_logger.setLevel(logging.INFO)


class AlertType(Enum):
    """アラートタイプ"""
    # 共通
    EMPTY_DATA = "EMPTY_DATA"              # 取り込み件数0件
    LOAD_ERROR = "LOAD_ERROR"              # 取り込みエラー

    # drive-to-gcs / spreadsheet-to-bq
    FILE_ERROR = "FILE_ERROR"              # ファイル処理エラー
    SHEET_ERROR = "SHEET_ERROR"            # シート処理エラー

    # gcs-to-bq
    COLUMN_MISMATCH = "COLUMN_MISMATCH"    # カラム不整合
    TABLE_LOAD_ERROR = "TABLE_LOAD_ERROR"  # テーブルロードエラー

    # dwh-datamart-update
    SONOTA_NON_ZERO = "SONOTA_NON_ZERO"    # main_category='その他'に値あり
    DUPLICATE_KEY = "DUPLICATE_KEY"        # 重複キー検出
    SQL_ERROR = "SQL_ERROR"                # SQL実行エラー

    # 汎用
    VALIDATION_ERROR = "VALIDATION_ERROR"  # バリデーションエラー
    UNKNOWN_ERROR = "UNKNOWN_ERROR"        # 不明なエラー


def log_alert(
    alert_type: AlertType,
    service: str,
    target: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    アラートログを出力

    Args:
        alert_type: アラートタイプ
        service: サービス名（drive-to-gcs, spreadsheet-to-bq, gcs-to-bq, dwh-datamart-update）
        target: 対象（ファイル名、シート名、テーブル名など）
        message: エラーメッセージ
        details: 追加の詳細情報
    """
    log_entry = {
        "severity": "ERROR",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "alert_type": alert_type.value,
        "service": service,
        "target": target,
        "message": message,
        "labels": {
            "service": service,
            "alert_type": alert_type.value
        }
    }

    if details:
        log_entry["details"] = details

    alert_logger.error(json.dumps(log_entry, ensure_ascii=False))


def log_warning(
    service: str,
    target: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    警告ログを出力

    Args:
        service: サービス名
        target: 対象
        message: 警告メッセージ
        details: 追加の詳細情報
    """
    log_entry = {
        "severity": "WARNING",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": service,
        "target": target,
        "message": message,
        "labels": {
            "service": service
        }
    }

    if details:
        log_entry["details"] = details

    alert_logger.warning(json.dumps(log_entry, ensure_ascii=False))


def log_success(
    service: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    成功ログを出力

    Args:
        service: サービス名
        message: 成功メッセージ
        details: 追加の詳細情報
    """
    log_entry = {
        "severity": "INFO",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": service,
        "message": message,
        "labels": {
            "service": service,
            "status": "SUCCESS"
        }
    }

    if details:
        log_entry["details"] = details

    alert_logger.info(json.dumps(log_entry, ensure_ascii=False))


def log_pipeline_completion(
    workflow_id: str,
    steps_completed: list,
    total_duration_seconds: float,
    errors: Optional[list] = None
) -> None:
    """
    パイプライン完了ログを出力

    Args:
        workflow_id: ワークフロー実行ID
        steps_completed: 完了したステップのリスト
        total_duration_seconds: 総実行時間（秒）
        errors: エラーがあれば記録
    """
    status = "SUCCESS" if not errors else "COMPLETED_WITH_ERRORS"

    log_entry = {
        "severity": "INFO" if not errors else "WARNING",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "cloud-workflows",
        "message": f"パイプライン {status}",
        "workflow_id": workflow_id,
        "steps_completed": steps_completed,
        "total_duration_seconds": total_duration_seconds,
        "labels": {
            "service": "cloud-workflows",
            "status": status
        }
    }

    if errors:
        log_entry["errors"] = errors

    alert_logger.info(json.dumps(log_entry, ensure_ascii=False))
