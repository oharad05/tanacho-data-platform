#!/bin/bash

# ============================================================
# DWH更新スクリプト
# ============================================================
# 目的: corporate_data_dwhデータセット内の全中間テーブルを更新
# 実行タイミング: 月次（前月データが揃った後）
# 必要な権限: BigQuery Editor以上
# ============================================================

set -e  # エラー時に即座に終了

PROJECT_ID="data-platform-prod-475201"
DATASET_DWH="corporate_data_dwh"
SQL_DIR="$(dirname "$0")/../split_dwh_dm"

echo "========================================="
echo "DWH更新処理を開始します"
echo "プロジェクト: ${PROJECT_ID}"
echo "データセット: ${DATASET_DWH}"
echo "========================================="

# 各DWHテーブルを順番に更新
# 注: SQLファイル内にCREATE OR REPLACE TABLE文が含まれているため、
#     --destination_tableオプションは使用せず、直接実行します。

echo ""
echo "[1/14] 売上実績を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/dwh_sales_actual.sql"

echo "[2/14] 売上実績（前年）を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/dwh_sales_actual_prev_year.sql"

echo "[3/14] 売上目標を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/dwh_sales_target.sql"

echo "[4/14] 営業経費を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/operating_expenses.sql"

echo "[5/14] 営業外収入を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/non_operating_income.sql"

echo "[6/14] 営業外費用を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/non_operating_expenses.sql"

echo "[7/14] 営業外費用（長崎支店）を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/non_operating_expenses_nagasaki.sql"

echo "[8/14] 営業外費用（福岡支店）を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/non_operating_expenses_fukuoka.sql"

echo "[9/14] 雑損失を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/miscellaneous_loss.sql"

echo "[10/14] 本店管理費を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/head_office_expenses.sql"

echo "[11/14] 経常利益目標を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/dwh_recurring_profit_target.sql"

echo "[12/14] 営業経費目標を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/operating_expenses_target.sql"

echo "[13/14] 営業利益目標を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/operating_income_target.sql"

echo "[14/14] 損益計算書（全支店統合）を更新中..."
bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  < "${SQL_DIR}/dwh_profit_plan_term_all.sql"

echo ""
echo "========================================="
echo "DWH更新処理が完了しました"
echo "========================================="
