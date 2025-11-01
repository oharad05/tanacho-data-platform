#!/bin/bash

# ============================================================
# DataMart更新スクリプト
# ============================================================
# 目的: corporate_data_dmデータセット内の経営資料（当月）テーブルを更新
# 実行タイミング: DWH更新後
# 必要な権限: BigQuery Editor以上
# 前提条件: update_dwh.shの実行完了
# ============================================================

set -e  # エラー時に即座に終了

PROJECT_ID="data-platform-prod-475201"
DATASET_DM="corporate_data_dm"
SQL_DIR="$(dirname "$0")/../split_dwh_dm"

echo "========================================="
echo "DataMart更新処理を開始します"
echo "プロジェクト: ${PROJECT_ID}"
echo "データセット: ${DATASET_DM}"
echo "========================================="

echo ""
echo "経営資料（当月）を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  --replace \
  --destination_table="${DATASET_DM}.management_documents_current_month_tbl" \
  "$(cat ${SQL_DIR}/datamart_management_report_vertical.sql)"

echo ""
echo "========================================="
echo "DataMart更新処理が完了しました"
echo "Looker Studioで最新データを確認できます"
echo "========================================="
