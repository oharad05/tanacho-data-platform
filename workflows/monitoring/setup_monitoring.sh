#!/bin/bash
# ============================================================
# Cloud Monitoring 設定スクリプト
# ============================================================
# 使用方法:
#   bash workflows/monitoring/setup_monitoring.sh
#
# 設定内容:
#   1. 通知チャンネル（Email）の作成
#   2. ログベースのメトリクスの作成
#   3. アラートポリシーの作成
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
NOTIFICATION_EMAIL="fiby2@tanacho.com"
COMPLETION_EMAIL="y.tanaka@tanacho.com"

echo "============================================================"
echo "Cloud Monitoring 設定開始"
echo "Project: ${PROJECT_ID}"
echo "通知先（アラート）: ${NOTIFICATION_EMAIL}"
echo "通知先（完了）: ${COMPLETION_EMAIL}"
echo "============================================================"

# ============================================================
# 1. 通知チャンネルの作成
# ============================================================
echo ""
echo "[Step 1] 通知チャンネルの作成..."

# アラート用チャンネル
cat > /tmp/notification_channel_alert.json << EOF
{
  "type": "email",
  "displayName": "Pipeline Alert - ${NOTIFICATION_EMAIL}",
  "labels": {
    "email_address": "${NOTIFICATION_EMAIL}"
  }
}
EOF

ALERT_CHANNEL_ID=$(gcloud alpha monitoring channels create \
  --project="${PROJECT_ID}" \
  --channel-content-from-file=/tmp/notification_channel_alert.json \
  --format="value(name)" 2>/dev/null || echo "")

if [ -z "${ALERT_CHANNEL_ID}" ]; then
  echo "  通知チャンネル（アラート）は既に存在するか、作成に失敗しました"
  # 既存のチャンネルを取得
  ALERT_CHANNEL_ID=$(gcloud alpha monitoring channels list \
    --project="${PROJECT_ID}" \
    --filter="displayName='Pipeline Alert - ${NOTIFICATION_EMAIL}'" \
    --format="value(name)" 2>/dev/null | head -1)
fi
echo "  アラート通知チャンネル: ${ALERT_CHANNEL_ID}"

# 完了通知用チャンネル
cat > /tmp/notification_channel_completion.json << EOF
{
  "type": "email",
  "displayName": "Pipeline Completion - ${COMPLETION_EMAIL}",
  "labels": {
    "email_address": "${COMPLETION_EMAIL}"
  }
}
EOF

COMPLETION_CHANNEL_ID=$(gcloud alpha monitoring channels create \
  --project="${PROJECT_ID}" \
  --channel-content-from-file=/tmp/notification_channel_completion.json \
  --format="value(name)" 2>/dev/null || echo "")

if [ -z "${COMPLETION_CHANNEL_ID}" ]; then
  echo "  通知チャンネル（完了）は既に存在するか、作成に失敗しました"
  COMPLETION_CHANNEL_ID=$(gcloud alpha monitoring channels list \
    --project="${PROJECT_ID}" \
    --filter="displayName='Pipeline Completion - ${COMPLETION_EMAIL}'" \
    --format="value(name)" 2>/dev/null | head -1)
fi
echo "  完了通知チャンネル: ${COMPLETION_CHANNEL_ID}"

# ============================================================
# 2. ログベースのメトリクスの作成
# ============================================================
echo ""
echo "[Step 2] ログベースのメトリクスの作成..."

# パイプラインエラーメトリクス
gcloud logging metrics create pipeline-errors \
  --project="${PROJECT_ID}" \
  --description="Pipeline error alerts from Cloud Run services" \
  --log-filter='resource.type="cloud_run_revision" AND (severity>=ERROR OR jsonPayload.alert_type!="")' \
  2>/dev/null || echo "  メトリクス 'pipeline-errors' は既に存在します"

# 取り込み件数0件メトリクス
gcloud logging metrics create pipeline-empty-data \
  --project="${PROJECT_ID}" \
  --description="Empty data alerts (0 records loaded)" \
  --log-filter='resource.type="cloud_run_revision" AND jsonPayload.alert_type="EMPTY_DATA"' \
  2>/dev/null || echo "  メトリクス 'pipeline-empty-data' は既に存在します"

# カラム不整合メトリクス
gcloud logging metrics create pipeline-column-mismatch \
  --project="${PROJECT_ID}" \
  --description="Column mismatch alerts" \
  --log-filter='resource.type="cloud_run_revision" AND jsonPayload.alert_type="COLUMN_MISMATCH"' \
  2>/dev/null || echo "  メトリクス 'pipeline-column-mismatch' は既に存在します"

# 重複キーメトリクス
gcloud logging metrics create pipeline-duplicate-key \
  --project="${PROJECT_ID}" \
  --description="Duplicate key alerts" \
  --log-filter='resource.type="cloud_run_revision" AND jsonPayload.alert_type="DUPLICATE_KEY"' \
  2>/dev/null || echo "  メトリクス 'pipeline-duplicate-key' は既に存在します"

echo "  ログベースメトリクスの作成完了"

# ============================================================
# 3. アラートポリシーの作成
# ============================================================
echo ""
echo "[Step 3] アラートポリシーの作成..."

if [ -n "${ALERT_CHANNEL_ID}" ]; then
  cat > /tmp/alert_policy.json << EOF
{
  "displayName": "Data Pipeline Error Alert",
  "documentation": {
    "content": "データパイプラインでエラーが発生しました。\n\nCloud Loggingで詳細を確認してください。",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "Pipeline Error Count",
      "conditionThreshold": {
        "filter": "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/pipeline-errors\"",
        "comparison": "COMPARISON_GT",
        "thresholdValue": 0,
        "duration": "0s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_COUNT"
          }
        ]
      }
    }
  ],
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": ["${ALERT_CHANNEL_ID}"],
  "alertStrategy": {
    "autoClose": "604800s"
  }
}
EOF

  gcloud alpha monitoring policies create \
    --project="${PROJECT_ID}" \
    --policy-from-file=/tmp/alert_policy.json \
    2>/dev/null || echo "  アラートポリシーは既に存在するか、作成に失敗しました"
  echo "  アラートポリシーの作成完了"
else
  echo "  警告: 通知チャンネルIDが取得できないため、アラートポリシーは作成されませんでした"
fi

# ============================================================
# クリーンアップ
# ============================================================
rm -f /tmp/notification_channel_alert.json
rm -f /tmp/notification_channel_completion.json
rm -f /tmp/alert_policy.json

echo ""
echo "============================================================"
echo "Cloud Monitoring 設定完了"
echo ""
echo "作成されたリソース:"
echo "  - 通知チャンネル（アラート）: ${ALERT_CHANNEL_ID}"
echo "  - 通知チャンネル（完了）: ${COMPLETION_CHANNEL_ID}"
echo "  - ログベースメトリクス: pipeline-errors, pipeline-empty-data, pipeline-column-mismatch, pipeline-duplicate-key"
echo "  - アラートポリシー: Data Pipeline Error Alert"
echo ""
echo "Cloud Monitoring コンソールで確認:"
echo "  https://console.cloud.google.com/monitoring/alerting?project=${PROJECT_ID}"
echo "============================================================"
