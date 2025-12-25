#!/bin/bash
# ============================================================
# Cloud Run サービス デプロイスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/deploy_cloud_run.sh
#
# 概要:
#   drive-to-gcs サービスをデプロイ
#   Google Drive から GCS へのデータ同期を行うサービス
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
REGION="asia-northeast1"
SERVICE_NAME="drive-to-gcs"
SERVICE_ACCOUNT="sa-data-platform@${PROJECT_ID}.iam.gserviceaccount.com"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE_DIR="${PROJECT_ROOT}/run_service"

echo "============================================================"
echo "Cloud Run サービス デプロイ"
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
  --set-env-vars "DRIVE_FOLDER_ID=1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6" \
  --set-env-vars "LANDING_BUCKET=data-platform-landing-prod" \
  --set-env-vars "MAPPING_GCS_PATH=google-drive/config/mapping_files.csv" \
  --set-env-vars "SERVICE_JSON_GCS_PATH=gs://data-platform-landing-prod/config/sa-data-platform-key.json" \
  --set-env-vars "IMPERSONATE_USER=fiby2@tanacho.com" \
  --memory=1Gi \
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
