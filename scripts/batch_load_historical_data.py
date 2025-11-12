#!/usr/bin/env python3
"""
過去データ一括取り込みバッチ
Transform → Load を一括実行
"""

import sys
import logging
import os
from pathlib import Path

# スクリプトのディレクトリをパスに追加
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))
sys.path.insert(0, str(script_dir / "manual"))

from google.cloud import logging as cloud_logging

# Cloud Logging設定
logging_client = cloud_logging.Client()
logging_client.setup_logging()
logger = logging.getLogger(__name__)

# 処理対象テーブルリスト
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
    "ms_department_category"
]


def main(start_yyyymm: str, end_yyyymm: str, skip_transform: bool = False):
    """
    過去データ一括取り込みメイン処理

    Args:
        start_yyyymm: 開始月（例: '202409'）
        end_yyyymm: 終了月（例: '202509'）
        skip_transform: Transformをスキップする場合はTrue
    """
    logger.info("=" * 80)
    logger.info("過去データ一括取り込みバッチ開始")
    logger.info(f"対象期間: {start_yyyymm} ～ {end_yyyymm}")
    logger.info("=" * 80)

    # Step 1: Transform (raw → proceed)
    if not skip_transform:
        logger.info("\n" + "=" * 80)
        logger.info("Step 1: Transform処理開始 (raw → proceed)")
        logger.info("=" * 80)

        try:
            # transform_raw_to_proceed.pyをインポート
            from transform_raw_to_proceed import process_multiple_months

            transform_result = process_multiple_months(start_yyyymm, end_yyyymm)

            if transform_result["error"] > 0:
                logger.warning(f"Transform処理でエラーが発生しました（{transform_result['error']}ヶ月）")
                logger.info("BigQuery LOADは続行します（エラー月はスキップされます）")

        except Exception as e:
            logger.error(f"Transform処理失敗: {e}")
            import traceback
            traceback.print_exc()
            logger.info("BigQuery LOADは続行します")
    else:
        logger.info("Step 1: Transform処理をスキップ (--skip-transform指定)")

    # Step 2: Load to BigQuery
    logger.info("\n" + "=" * 80)
    logger.info("Step 2: BigQuery LOAD処理開始")
    logger.info(f"対象テーブル数: {len(TABLES)}")
    logger.info("=" * 80)

    try:
        from load_to_bigquery import BigQueryLoader

        loader = BigQueryLoader()
        success_tables = []
        error_tables = []

        for i, table_name in enumerate(TABLES, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"[{i}/{len(TABLES)}] {table_name} を処理中...")
            logger.info(f"{'='*80}")

            try:
                result = loader.load_table_with_dedup(table_name, start_yyyymm, end_yyyymm)

                if result:
                    success_tables.append(table_name)
                else:
                    error_tables.append(table_name)

            except Exception as e:
                logger.error(f"テーブル {table_name} の処理中にエラー: {e}")
                import traceback
                traceback.print_exc()
                error_tables.append(table_name)

        # 最終結果
        logger.info("\n" + "=" * 80)
        logger.info("過去データ一括取り込みバッチ完了")
        logger.info("=" * 80)
        logger.info(f"成功: {len(success_tables)} テーブル")
        if success_tables:
            for table in success_tables:
                logger.info(f"  ✅ {table}")

        logger.info(f"\n失敗: {len(error_tables)} テーブル")
        if error_tables:
            for table in error_tables:
                logger.error(f"  ❌ {table}")

        logger.info("=" * 80)

        # Cloud Loggingで重複ログを確認するクエリを表示
        logger.info("\n📊 重複ログの確認方法:")
        logger.info("Cloud Console → Logging → ログエクスプローラー")
        logger.info('フィルタ: severity="WARNING" AND jsonPayload.duplicate_count>0')

        return len(error_tables) == 0

    except Exception as e:
        logger.error(f"BigQuery LOAD処理で予期しないエラー: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("使用方法: python batch_load_historical_data.py <start_yyyymm> <end_yyyymm> [--skip-transform]")
        print("")
        print("例:")
        print("  全処理実行:        python batch_load_historical_data.py 202409 202509")
        print("  Transformスキップ: python batch_load_historical_data.py 202409 202509 --skip-transform")
        print("")
        print("オプション:")
        print("  --skip-transform: Transform処理をスキップしてBigQuery LOADのみ実行")
        sys.exit(1)

    start_yyyymm = sys.argv[1]
    end_yyyymm = sys.argv[2]
    skip_transform = "--skip-transform" in sys.argv

    success = main(start_yyyymm, end_yyyymm, skip_transform)
    sys.exit(0 if success else 1)
