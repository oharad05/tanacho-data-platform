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
#   0. 中間テーブル作成       → aggregated_metrics_all_branches (DWH層)
#   1. 東京支店DataMart作成 → management_documents_all_period_tokyo
#   2. 長崎支店DataMart作成 → management_documents_all_period_nagasaki
#   3. 福岡支店DataMart作成 → management_documents_all_period_fukuoka
#   4. 統合DataMart作成     → management_documents_all_period_all
#   5. 表示用DataMart作成   → management_documents_all_period_all_for_display
#   6. 累計DataMart作成     → cumulative_management_documents_all_period_all
#
# 重要:
#   - aggregated_metrics_all_branchesは個別支店DataMartの前提テーブル
#   - 統合DataMartは個別支店DataMartをUNIONするため、必ず個別支店DataMart作成後に実行
# ============================================================

set -e  # エラー時に即座に終了

PROJECT_ID="data-platform-prod-475201"
DATASET_DM="corporate_data_dm"
DATASET_DWH="corporate_data_dwh"
SQL_DIR="$(dirname "$0")/../split_dwh_dm"

echo "========================================="
echo "DataMart更新処理を開始します"
echo "プロジェクト: ${PROJECT_ID}"
echo "データセット: ${DATASET_DM}"
echo "========================================="

# ============================================================
# 0. 中間テーブル作成 (aggregated_metrics_all_branches)
# ============================================================
echo ""
echo "0/4: 中間テーブル(aggregated_metrics_all_branches)を作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/aggregated_metrics_all_branches.sql"

if [ $? -eq 0 ]; then
  echo "✓ 中間テーブル作成完了"
else
  echo "✗ 中間テーブル作成失敗"
  exit 1
fi

# ============================================================
# 1. 東京支店DataMart作成
# ============================================================
echo ""
echo "1/4: 東京支店DataMartを作成中..."
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
echo "2/4: 長崎支店DataMartを作成中..."
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
# 3. 福岡支店DataMart作成
# ============================================================
echo ""
echo "3/4: 福岡支店DataMartを作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/datamart_management_report_fukuoka.sql"

if [ $? -eq 0 ]; then
  echo "✓ 福岡支店DataMart作成完了"
else
  echo "✗ 福岡支店DataMart作成失敗"
  exit 1
fi

# ============================================================
# 4. 統合DataMart作成
# ============================================================
echo ""
echo "4/6: 統合DataMartを作成中（東京支店 + 長崎支店 + 福岡支店）..."
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

# ============================================================
# 5. 表示用DataMart作成
# ============================================================
echo ""
echo "5/6: 表示用DataMartを作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/datamart_management_report_all_for_display.sql"

if [ $? -eq 0 ]; then
  echo "✓ 表示用DataMart作成完了"
else
  echo "✗ 表示用DataMart作成失敗"
  exit 1
fi

# ============================================================
# 6. 累計DataMart作成
# ============================================================
echo ""
echo "6/6: 累計DataMartを作成中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/cumulative_management_documents_all_period_all.sql"

if [ $? -eq 0 ]; then
  echo "✓ 累計DataMart作成完了"
else
  echo "✗ 累計DataMart作成失敗"
  exit 1
fi

echo ""
echo "========================================="
echo "DataMart更新処理が完了しました"
echo "作成されたテーブル:"
echo "  - ${DATASET_DM}.management_documents_all_period_tokyo (東京支店)"
echo "  - ${DATASET_DM}.management_documents_all_period_nagasaki (長崎支店)"
echo "  - ${DATASET_DM}.management_documents_all_period_fukuoka (福岡支店)"
echo "  - ${DATASET_DM}.management_documents_all_period_all (統合)"
echo "  - ${DATASET_DM}.management_documents_all_period_all_for_display (表示用)"
echo "  - ${DATASET_DM}.cumulative_management_documents_all_period_all (累計)"
echo ""
echo "Looker Studioで最新データを確認できます"
echo "========================================="
