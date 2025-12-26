#!/bin/bash
# ============================================================
# Cloud Run Job デプロイスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/deploy_cloud_run_job.sh
#
# 概要:
#   dwh-datamart-update ジョブをデプロイ
#   DWH/DataMart の更新処理を行うジョブ
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
REGION="asia-northeast1"
JOB_NAME="dwh-datamart-update"
SERVICE_ACCOUNT="sa-data-platform@${PROJECT_ID}.iam.gserviceaccount.com"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE_DIR="${PROJECT_ROOT}/dwh_datamart_job"

echo "============================================================"
echo "Cloud Run Job デプロイ"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Job: ${JOB_NAME}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "Source: ${SOURCE_DIR}"
echo "============================================================"

# ソースディレクトリの存在確認
if [ ! -d "${SOURCE_DIR}" ]; then
  echo "[ERROR] ソースディレクトリが見つかりません: ${SOURCE_DIR}"
  exit 1
fi

# Cloud Run Job へデプロイ（ソースからビルド）
echo ""
echo "[Step 1] Cloud Run Job をデプロイ中..."
gcloud run jobs deploy "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --source="${SOURCE_DIR}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --set-env-vars "UPDATE_TYPE=all" \
  --set-env-vars "ENABLE_BACKUP=true" \
  --set-env-vars "VALIDATION_ENABLED=true" \
  --memory=2Gi \
  --cpu=2 \
  --task-timeout=3600 \
  --max-retries=0

echo ""
echo "============================================================"
echo "デプロイ完了"
echo "============================================================"
echo ""
echo "ジョブ情報:"
gcloud run jobs describe "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format='table(metadata.name,status.latestCreatedExecution.name,spec.template.spec.template.spec.containers[0].image)'
echo ""
echo "手動実行:"
echo "  gcloud run jobs execute ${JOB_NAME} --region=${REGION}"
echo ""
echo "============================================================"
