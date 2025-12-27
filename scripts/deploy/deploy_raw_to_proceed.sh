#!/bin/bash
# ============================================================
# raw-to-proceed Cloud Run Service Deploy Script
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
REGION="asia-northeast1"
SERVICE_NAME="raw-to-proceed"
SERVICE_ACCOUNT="sa-data-platform@${PROJECT_ID}.iam.gserviceaccount.com"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE_DIR="${PROJECT_ROOT}/raw_to_proceed_service"

echo "============================================================"
echo "raw-to-proceed Cloud Run Service Deploy"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE_NAME}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "Source: ${SOURCE_DIR}"
echo "============================================================"

if [ ! -d "${SOURCE_DIR}" ]; then
  echo "[ERROR] Source directory not found: ${SOURCE_DIR}"
  exit 1
fi

echo ""
echo "[Step 1] Deploying Cloud Run service..."
gcloud run deploy "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --source="${SOURCE_DIR}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --set-env-vars "LANDING_BUCKET=data-platform-landing-prod" \
  --memory=2Gi \
  --timeout=1800 \
  --allow-unauthenticated

echo ""
echo "============================================================"
echo "Deploy completed"
echo "============================================================"
echo ""
echo "Service URL:"
gcloud run services describe "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format='value(status.url)'
echo ""
echo "============================================================"
