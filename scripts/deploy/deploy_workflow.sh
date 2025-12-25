#!/bin/bash
# ============================================================
# Cloud Workflows デプロイスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/deploy_workflow.sh
#
# 概要:
#   data-pipeline ワークフローをデプロイ
#   Drive→GCS→BigQuery のパイプライン全体を制御
#
# ワークフロー実行方法:
#   # replaceモード（全データ洗い替え）
#   gcloud workflows run data-pipeline --location=asia-northeast1
#
#   # appendモード（指定月のみ追加）
#   gcloud workflows run data-pipeline --location=asia-northeast1 \
#     --data='{"mode": "append", "target_month": "202511"}'
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
REGION="asia-northeast1"
WORKFLOW_NAME="data-pipeline"
SERVICE_ACCOUNT="sa-data-platform@${PROJECT_ID}.iam.gserviceaccount.com"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKFLOW_SOURCE="${PROJECT_ROOT}/workflows/data_pipeline.yaml"

echo "============================================================"
echo "Cloud Workflows デプロイ"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Workflow: ${WORKFLOW_NAME}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "Source: ${WORKFLOW_SOURCE}"
echo "============================================================"

# ワークフローファイルの存在確認
if [ ! -f "${WORKFLOW_SOURCE}" ]; then
  echo "[ERROR] ワークフローファイルが見つかりません: ${WORKFLOW_SOURCE}"
  exit 1
fi

# Workflows APIの有効化（まだの場合）
echo ""
echo "[Step 1] Workflows API の確認..."
gcloud services enable workflows.googleapis.com --project="${PROJECT_ID}" 2>/dev/null || true

# サービスアカウントに必要な権限を付与
echo ""
echo "[Step 2] サービスアカウントの権限確認..."

# Cloud Run Invoker権限（Cloud Runサービスを呼び出すため）
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/run.invoker" \
  --condition=None \
  --quiet 2>/dev/null || true

# Cloud Run Admin権限（Cloud Run Jobを実行するため）
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/run.admin" \
  --condition=None \
  --quiet 2>/dev/null || true

# Workflows Invoker権限（サブワークフローを呼び出すため）
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/workflows.invoker" \
  --condition=None \
  --quiet 2>/dev/null || true

echo "  権限付与完了"

# ワークフローのデプロイ
echo ""
echo "[Step 3] ワークフローのデプロイ..."
gcloud workflows deploy "${WORKFLOW_NAME}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --source="${WORKFLOW_SOURCE}" \
  --service-account="${SERVICE_ACCOUNT}"

echo ""
echo "============================================================"
echo "デプロイ完了"
echo "============================================================"
echo ""
echo "ワークフロー実行方法:"
echo ""
echo "  # replaceモード（全データ洗い替え）"
echo "  gcloud workflows run ${WORKFLOW_NAME} --location=${REGION}"
echo ""
echo "  # appendモード（指定月のみ追加）"
echo "  gcloud workflows run ${WORKFLOW_NAME} --location=${REGION} \\"
echo "    --data='{\"mode\": \"append\", \"target_month\": \"202511\"}'"
echo ""
echo "  # 実行状況の確認"
echo "  gcloud workflows executions list ${WORKFLOW_NAME} --location=${REGION}"
echo ""
echo "============================================================"
