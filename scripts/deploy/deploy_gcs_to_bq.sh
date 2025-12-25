#!/bin/bash
# ============================================================
# gcs-to-bq Cloud Run サービス デプロイスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/deploy_gcs_to_bq.sh
#
# 概要:
#   gcs-to-bq サービスをデプロイ
#   GCS から BigQuery へのデータロードを行うサービス
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
REGION="asia-northeast1"
SERVICE_NAME="gcs-to-bq"
SERVICE_ACCOUNT="sa-data-platform@${PROJECT_ID}.iam.gserviceaccount.com"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE_DIR="${PROJECT_ROOT}/gcs_to_bq_service"

echo "============================================================"
echo "gcs-to-bq Cloud Run サービス デプロイ"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE_NAME}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "Source: ${SOURCE_DIR}"
echo "============================================================"

# ソースディレクトリの存在確認
if [ ! -d "${SOURCE_DIR}" ]; then
  echo "[ERROR] ソースディレクトリが見つかりません: ${SOURCE_DIR}"
  exit 1
fi

# Cloud Run へデプロイ（ソースからビルド）
echo ""
echo "[Step 1] Cloud Run サービスをデプロイ中..."
gcloud run deploy "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --source="${SOURCE_DIR}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --set-env-vars "LANDING_BUCKET=data-platform-landing-prod" \
  --set-env-vars "VALIDATION_ENABLED=true" \
  --memory=2Gi \
  --timeout=900 \
  --allow-unauthenticated

echo ""
echo "============================================================"
echo "デプロイ完了"
echo "============================================================"
echo ""
echo "サービスURL確認:"
gcloud run services describe "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format='value(status.url)'
echo ""
echo "============================================================"
