#!/bin/bash

# ============================================================
# DataMart更新スクリプト
# ============================================================
# 目的: corporate_data_dmデータセット内の経営資料テーブルを更新
# 実行タイミング: DWH更新後
# 必要な権限: BigQuery Editor以上
# 前提条件: update_dwh.shの実行完了
#
# 処理フロー:
#   1. 東京支店DataMart作成 → management_documents_all_period
#   2. 長崎支店DataMart作成 → management_documents_all_period_nagasaki
#   3. 統合DataMart作成     → management_documents_all_period_all
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

# ============================================================
# 1. 東京支店DataMart作成
# ============================================================
echo ""
echo "1/3: 東京支店DataMartを作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/datamart_management_report_tokyo.sql"

if [ $? -eq 0 ]; then
  echo "✓ 東京支店DataMart作成完了"
else
  echo "✗ 東京支店DataMart作成失敗"
  exit 1
fi

# ============================================================
# 2. 長崎支店DataMart作成
# ============================================================
echo ""
echo "2/3: 長崎支店DataMartを作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/datamart_management_report_nagasaki.sql"

if [ $? -eq 0 ]; then
  echo "✓ 長崎支店DataMart作成完了"
else
  echo "✗ 長崎支店DataMart作成失敗"
  exit 1
fi

# ============================================================
# 3. 統合DataMart作成
# ============================================================
echo ""
echo "3/3: 統合DataMartを作成中（東京支店 + 長崎支店）..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/datamart_management_report_all.sql"

if [ $? -eq 0 ]; then
  echo "✓ 統合DataMart作成完了"
else
  echo "✗ 統合DataMart作成失敗"
  exit 1
fi

echo ""
echo "========================================="
echo "DataMart更新処理が完了しました"
echo "作成されたテーブル:"
echo "  - ${DATASET_DM}.management_documents_all_period (東京支店)"
echo "  - ${DATASET_DM}.management_documents_all_period_nagasaki (長崎支店)"
echo "  - ${DATASET_DM}.management_documents_all_period_all (統合)"
echo ""
echo "Looker Studioで最新データを確認できます"
echo "========================================="
