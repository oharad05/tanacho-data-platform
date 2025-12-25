#!/bin/bash
# ============================================================
# SQL ファイル GCS アップロードスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/upload_sql_to_gcs.sh
#
# 概要:
#   sql/split_dwh_dm/ 配下の SQL ファイルを GCS にアップロード
#   Cloud Run Job (dwh-datamart-update) がこれらの SQL を読み込んで実行
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
GCS_BUCKET="gs://data-platform-landing-prod"
SQL_LOCAL_DIR="sql/split_dwh_dm"
SQL_GCS_PREFIX="sql/split_dwh_dm"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "============================================================"
echo "SQL ファイル GCS アップロード"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "GCS: ${GCS_BUCKET}/${SQL_GCS_PREFIX}/"
echo "Local: ${PROJECT_ROOT}/${SQL_LOCAL_DIR}/"
echo "============================================================"

# ローカルディレクトリの存在確認
if [ ! -d "${PROJECT_ROOT}/${SQL_LOCAL_DIR}" ]; then
  echo "[ERROR] SQL ディレクトリが見つかりません: ${PROJECT_ROOT}/${SQL_LOCAL_DIR}"
  exit 1
fi

# SQL ファイル数を確認
SQL_COUNT=$(find "${PROJECT_ROOT}/${SQL_LOCAL_DIR}" -name "*.sql" | wc -l | tr -d ' ')
echo ""
echo "[Step 1] アップロード対象: ${SQL_COUNT} ファイル"

# GCS にアップロード
echo ""
echo "[Step 2] GCS にアップロード中..."
gsutil -m cp -r "${PROJECT_ROOT}/${SQL_LOCAL_DIR}/"*.sql "${GCS_BUCKET}/${SQL_GCS_PREFIX}/"

# アップロード結果を確認
echo ""
echo "[Step 3] アップロード結果確認..."
UPLOADED_COUNT=$(gsutil ls "${GCS_BUCKET}/${SQL_GCS_PREFIX}/"*.sql 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "============================================================"
echo "アップロード完了"
echo "============================================================"
echo "  ローカル: ${SQL_COUNT} ファイル"
echo "  GCS: ${UPLOADED_COUNT} ファイル"
echo ""
echo "アップロード先:"
gsutil ls "${GCS_BUCKET}/${SQL_GCS_PREFIX}/"*.sql 2>/dev/null | head -10
if [ "${UPLOADED_COUNT}" -gt 10 ]; then
  echo "  ... 他 $((UPLOADED_COUNT - 10)) ファイル"
fi
echo "============================================================"
