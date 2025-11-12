#!/usr/bin/env python3
"""
BigQuery LOAD処理
一時テーブル + SWAP方式で安全にデータを投入
重複判定: 全レコードが同一の場合
"""

import time
import yaml
import logging
from typing import List, Dict, Optional
from google.cloud import bigquery
from datetime import datetime
from pathlib import Path

# Logging設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定数
PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
LANDING_BUCKET = "data-platform-landing-prod"
PRIMARY_KEYS_CONFIG = "config/primary_keys.yaml"

class BigQueryLoader:
    """BigQuery LOAD処理クラス"""

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self.primary_keys_config = self._load_primary_keys_config()

    def _load_primary_keys_config(self) -> Dict:
        """主キー設定を読み込み（参考情報）"""
        config_path = Path(PRIMARY_KEYS_CONFIG)
        if not config_path.exists():
            logger.warning(f"主キー設定ファイルが見つかりません: {PRIMARY_KEYS_CONFIG}")
            return {}

        with open(config_path) as f:
            return yaml.safe_load(f)

    def generate_month_range(self, start_yyyymm: str, end_yyyymm: str) -> List[str]:
        """月範囲生成"""
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        start = datetime.strptime(start_yyyymm, '%Y%m')
        end = datetime.strptime(end_yyyymm, '%Y%m')

        months = []
        current = start
        while current <= end:
            months.append(current.strftime('%Y%m'))
            current += relativedelta(months=1)

        return months

    def create_temp_table(self, table_name: str) -> str:
        """
        一時テーブルを作成

        Returns:
            一時テーブル名（例: sales_actual_temp_1234567890）
        """
        temp_table_name = f"{table_name}_temp_{int(time.time())}"
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"

        # 元テーブルのスキーマをコピー
        source_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        try:
            source_table = self.client.get_table(source_table_id)
            temp_table = bigquery.Table(temp_table_id, schema=source_table.schema)
            temp_table = self.client.create_table(temp_table)

            logger.info(f"一時テーブル作成: {temp_table_id}")
            return temp_table_name
        except Exception as e:
            logger.error(f"一時テーブル作成エラー: {e}")
            raise

    def load_csv_to_temp_table(
        self,
        temp_table_name: str,
        csv_uri: str
    ):
        """
        CSVを一時テーブルにLOAD

        Args:
            temp_table_name: 一時テーブル名
            csv_uri: CSV URI (gs://bucket/path/to/file.csv)
        """
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # ヘッダー行をスキップ
            autodetect=False,  # スキーマは既存テーブルから取得
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 追記モード
            allow_jagged_rows=True,  # 列数が不一致でも許容
            allow_quoted_newlines=True,  # 引用符内の改行を許容
        )

        load_job = self.client.load_table_from_uri(
            csv_uri,
            temp_table_id,
            job_config=job_config
        )

        load_job.result()  # 完了を待つ

        logger.info(f"CSV LOAD完了: {csv_uri} → {temp_table_id}")

    def count_total_rows(self, temp_table_name: str) -> int:
        """一時テーブルの総行数を取得"""
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"

        query = f"SELECT COUNT(*) as total FROM `{temp_table_id}`"
        query_job = self.client.query(query)
        results = list(query_job.result())

        return results[0]["total"] if results else 0

    def count_distinct_rows(self, temp_table_name: str) -> int:
        """一時テーブルのユニーク行数を取得"""
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"

        query = f"SELECT COUNT(*) as total FROM (SELECT DISTINCT * FROM `{temp_table_id}`)"
        query_job = self.client.query(query)
        results = list(query_job.result())

        return results[0]["total"] if results else 0

    def detect_duplicates(self, temp_table_name: str) -> Dict:
        """
        重複検知（全レコード同一判定）

        Returns:
            {"total_rows": 総行数, "unique_rows": ユニーク行数, "duplicate_count": 重複件数}
        """
        total_rows = self.count_total_rows(temp_table_name)
        unique_rows = self.count_distinct_rows(temp_table_name)
        duplicate_count = total_rows - unique_rows

        return {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count
        }

    def log_duplicates(
        self,
        table_name: str,
        duplicates: Dict,
        start_yyyymm: str,
        end_yyyymm: str
    ):
        """重複をCloud Loggingに記録"""
        if duplicates["duplicate_count"] == 0:
            logger.info(f"✅ 重複なし: {table_name} (総行数: {duplicates['total_rows']})")
            return

        logger.warning(
            f"⚠️  重複検知: {table_name}",
            extra={
                "labels": {
                    "table_name": table_name,
                    "severity": "WARNING",
                    "component": "bigquery_load"
                },
                "jsonPayload": {
                    "total_rows": duplicates["total_rows"],
                    "unique_rows": duplicates["unique_rows"],
                    "duplicate_count": duplicates["duplicate_count"],
                    "duplicate_ratio": f"{duplicates['duplicate_count'] / duplicates['total_rows'] * 100:.2f}%",
                    "period": f"{start_yyyymm}-{end_yyyymm}",
                    "action_taken": "removed_all_duplicates_using_distinct",
                    "timestamp": datetime.now().isoformat()
                }
            }
        )

        print(f"⚠️  重複検知: {table_name}")
        print(f"   総行数: {duplicates['total_rows']}")
        print(f"   ユニーク行数: {duplicates['unique_rows']}")
        print(f"   重複件数: {duplicates['duplicate_count']} ({duplicates['duplicate_count'] / duplicates['total_rows'] * 100:.2f}%)")

    def create_deduped_table(
        self,
        temp_table_name: str,
        target_table_name: str
    ):
        """
        重複排除して本番テーブルを作成（DELETE + INSERT）

        Strategy: DISTINCT で全レコード同一の重複を排除
        """
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
        target_table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table_name}"

        # 既存データを削除
        delete_query = f"DELETE FROM `{target_table_id}` WHERE TRUE"
        delete_job = self.client.query(delete_query)
        delete_job.result()

        # DISTINCT で重複排除してINSERT
        query = f"""
        INSERT INTO `{target_table_id}`
        SELECT DISTINCT *
        FROM `{temp_table_id}`
        """

        query_job = self.client.query(query)
        query_job.result()

        # 最終的な行数を確認
        final_count_query = f"SELECT COUNT(*) as total FROM `{target_table_id}`"
        final_count_job = self.client.query(final_count_query)
        final_count = list(final_count_job.result())[0]["total"]

        logger.info(f"✅ 重複排除完了、本番テーブルにINSERT: {target_table_id} ({final_count}行)")
        print(f"   最終行数: {final_count}行")

    def delete_temp_table(self, temp_table_name: str):
        """一時テーブル削除"""
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
        self.client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"一時テーブル削除: {temp_table_id}")

    def load_table_with_dedup(
        self,
        table_name: str,
        start_yyyymm: str,
        end_yyyymm: str
    ) -> bool:
        """
        一時テーブル + SWAP方式でテーブルをロード

        Args:
            table_name: テーブル名
            start_yyyymm: 開始月
            end_yyyymm: 終了月

        Returns:
            成功時True、失敗時False
        """
        logger.info("=" * 80)
        logger.info(f"テーブルLOAD開始: {table_name}")
        logger.info(f"対象期間: {start_yyyymm} ～ {end_yyyymm}")
        logger.info("=" * 80)

        # 1. 一時テーブル作成
        try:
            temp_table_name = self.create_temp_table(table_name)
        except Exception as e:
            logger.error(f"❌ 一時テーブル作成失敗: {e}")
            return False

        try:
            # 2. 古い順にCSV LOAD
            months = self.generate_month_range(start_yyyymm, end_yyyymm)

            loaded_count = 0
            for yyyymm in sorted(months):  # 古い順
                csv_uri = f"gs://{LANDING_BUCKET}/proceed/{yyyymm}/{table_name}.csv"
                logger.info(f"LOAD中: {yyyymm} ({csv_uri})")

                try:
                    self.load_csv_to_temp_table(temp_table_name, csv_uri)
                    loaded_count += 1
                except Exception as e:
                    logger.warning(f"⚠️  CSV LOAD失敗（スキップ）: {csv_uri} - {e}")
                    continue

            if loaded_count == 0:
                logger.error(f"❌ {table_name}: 1つもCSVをLOADできませんでした")
                return False

            logger.info(f"CSV LOAD完了: {loaded_count}/{len(months)} ファイル成功")

            # 3. 重複検知
            logger.info("重複検知中...")
            duplicates = self.detect_duplicates(temp_table_name)

            # 4. ログ記録
            self.log_duplicates(table_name, duplicates, start_yyyymm, end_yyyymm)

            # 5. 重複排除してSWAP
            logger.info("重複排除 & 本番テーブル作成中...")
            self.create_deduped_table(temp_table_name, table_name)

            logger.info(f"✅ {table_name} LOAD完了")
            return True

        except Exception as e:
            logger.error(f"❌ {table_name} LOAD失敗: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            # 6. 一時テーブル削除
            self.delete_temp_table(temp_table_name)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("使用方法: python load_to_bigquery.py <table_name> <start_yyyymm> <end_yyyymm>")
        print("例: python load_to_bigquery.py sales_target_and_achievements 202409 202509")
        sys.exit(1)

    table_name = sys.argv[1]
    start_yyyymm = sys.argv[2]
    end_yyyymm = sys.argv[3]

    loader = BigQueryLoader()
    success = loader.load_table_with_dedup(table_name, start_yyyymm, end_yyyymm)

    sys.exit(0 if success else 1)
