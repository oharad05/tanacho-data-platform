#!/bin/bash
# ============================================================
# 全コンポーネント一括デプロイスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/deploy_all.sh
#
# 概要:
#   以下のコンポーネントを順番にデプロイ:
#   1. SQL ファイルを GCS にアップロード
#   2. Cloud Run サービス (drive-to-gcs)
#   3. Cloud Run サービス (raw-to-proceed)
#   4. Cloud Run サービス (gcs-to-bq)
#   5. Cloud Run Job (dwh-datamart-update)
#   6. Cloud Workflows (data-pipeline)
#
# オプション:
#   --skip-sql       SQL アップロードをスキップ
#   --skip-config    設定ファイル アップロードをスキップ
#   --skip-run       Cloud Run サービスのデプロイをスキップ
#   --skip-job       Cloud Run Job のデプロイをスキップ
#   --skip-workflow  Workflow のデプロイをスキップ
#   --sql-only       SQL アップロードのみ実行
#   --config-only    設定ファイル アップロードのみ実行
# ============================================================

set -e

# スクリプトのディレクトリ
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# オプション解析
SKIP_SQL=false
SKIP_CONFIG=false
SKIP_RUN=false
SKIP_JOB=false
SKIP_WORKFLOW=false
SQL_ONLY=false
CONFIG_ONLY=false

for arg in "$@"; do
  case $arg in
    --skip-sql)
      SKIP_SQL=true
      ;;
    --skip-config)
      SKIP_CONFIG=true
      ;;
    --skip-run)
      SKIP_RUN=true
      ;;
    --skip-job)
      SKIP_JOB=true
      ;;
    --skip-workflow)
      SKIP_WORKFLOW=true
      ;;
    --sql-only)
      SQL_ONLY=true
      ;;
    --config-only)
      CONFIG_ONLY=true
      ;;
  esac
done

echo "============================================================"
echo "全コンポーネント一括デプロイ"
echo "============================================================"
echo ""
echo "デプロイ対象:"
if [ "${SQL_ONLY}" = true ]; then
  echo "  [x] SQL ファイル GCS アップロード"
  echo "  [ ] 設定ファイル GCS アップロード (スキップ)"
  echo "  [ ] Cloud Run サービス (スキップ)"
  echo "  [ ] Cloud Run Job (スキップ)"
  echo "  [ ] Cloud Workflows (スキップ)"
elif [ "${CONFIG_ONLY}" = true ]; then
  echo "  [ ] SQL ファイル GCS アップロード (スキップ)"
  echo "  [x] 設定ファイル GCS アップロード"
  echo "  [ ] Cloud Run サービス (スキップ)"
  echo "  [ ] Cloud Run Job (スキップ)"
  echo "  [ ] Cloud Workflows (スキップ)"
else
  [ "${SKIP_SQL}" = true ] && echo "  [ ] SQL ファイル GCS アップロード (スキップ)" || echo "  [x] SQL ファイル GCS アップロード"
  [ "${SKIP_CONFIG}" = true ] && echo "  [ ] 設定ファイル GCS アップロード (スキップ)" || echo "  [x] 設定ファイル GCS アップロード"
  [ "${SKIP_RUN}" = true ] && echo "  [ ] Cloud Run サービス (drive-to-gcs, raw-to-proceed, gcs-to-bq) (スキップ)" || echo "  [x] Cloud Run サービス (drive-to-gcs, raw-to-proceed, gcs-to-bq)"
  [ "${SKIP_JOB}" = true ] && echo "  [ ] Cloud Run Job (スキップ)" || echo "  [x] Cloud Run Job"
  [ "${SKIP_WORKFLOW}" = true ] && echo "  [ ] Cloud Workflows (スキップ)" || echo "  [x] Cloud Workflows"
fi
echo ""
echo "============================================================"

# 成功/失敗カウント
SUCCESS_COUNT=0
FAIL_COUNT=0
SKIPPED_COUNT=0

# ------------------------------------------------------------
# 1. SQL ファイル GCS アップロード
# ------------------------------------------------------------
if [ "${SKIP_SQL}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 1/5: SQL ファイル GCS アップロード"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/upload_sql_to_gcs.sh"; then
    echo "[OK] SQL アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] SQL アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 1/5: SQL ファイル GCS アップロード"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# SQL のみモードの場合はここで終了
if [ "${SQL_ONLY}" = true ]; then
  echo ""
  echo "============================================================"
  echo "デプロイ完了 (SQL のみ)"
  echo "============================================================"
  echo "  成功: ${SUCCESS_COUNT}"
  echo "  失敗: ${FAIL_COUNT}"
  echo "============================================================"
  exit 0
fi

# ------------------------------------------------------------
# 1.5. 設定ファイル GCS アップロード
# ------------------------------------------------------------
if [ "${SKIP_CONFIG}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 1.5/6: 設定ファイル GCS アップロード"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/upload_config_to_gcs.sh"; then
    echo "[OK] 設定ファイル アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] 設定ファイル アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 1.5/6: 設定ファイル GCS アップロード"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# 設定ファイルのみモードの場合はここで終了
if [ "${CONFIG_ONLY}" = true ]; then
  echo ""
  echo "============================================================"
  echo "デプロイ完了 (設定ファイルのみ)"
  echo "============================================================"
  echo "  成功: ${SUCCESS_COUNT}"
  echo "  失敗: ${FAIL_COUNT}"
  echo "============================================================"
  exit 0
fi

# ------------------------------------------------------------
# 2. Cloud Run サービス (drive-to-gcs)
# ------------------------------------------------------------
if [ "${SKIP_RUN}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 2/5: Cloud Run サービス (drive-to-gcs)"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/deploy_cloud_run.sh"; then
    echo "[OK] drive-to-gcs デプロイ成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] drive-to-gcs デプロイ失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 2/5: Cloud Run サービス (drive-to-gcs)"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# ------------------------------------------------------------
# 3. Cloud Run サービス (raw-to-proceed)
# ------------------------------------------------------------
if [ "${SKIP_RUN}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 3/6: Cloud Run サービス (raw-to-proceed)"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/deploy_raw_to_proceed.sh"; then
    echo "[OK] raw-to-proceed デプロイ成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] raw-to-proceed デプロイ失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 3/6: Cloud Run サービス (raw-to-proceed)"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# ------------------------------------------------------------
# 4. Cloud Run サービス (gcs-to-bq)
# ------------------------------------------------------------
if [ "${SKIP_RUN}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 4/6: Cloud Run サービス (gcs-to-bq)"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/deploy_gcs_to_bq.sh"; then
    echo "[OK] gcs-to-bq デプロイ成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] gcs-to-bq デプロイ失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 4/6: Cloud Run サービス (gcs-to-bq)"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# ------------------------------------------------------------
# 5. Cloud Run Job
# ------------------------------------------------------------
if [ "${SKIP_JOB}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 5/6: Cloud Run Job (dwh-datamart-update)"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/deploy_cloud_run_job.sh"; then
    echo "[OK] Cloud Run Job デプロイ成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] Cloud Run Job デプロイ失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 5/6: Cloud Run Job"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# ------------------------------------------------------------
# 6. Cloud Workflows
# ------------------------------------------------------------
if [ "${SKIP_WORKFLOW}" = false ]; then
  echo ""
  echo "########################################################"
  echo "# 6/6: Cloud Workflows (data-pipeline)"
  echo "########################################################"
  if bash "${SCRIPT_DIR}/deploy_workflow.sh"; then
    echo "[OK] Cloud Workflows デプロイ成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "[NG] Cloud Workflows デプロイ失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo ""
  echo "[SKIP] 6/6: Cloud Workflows"
  SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
fi

# ------------------------------------------------------------
# 結果サマリー
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo "デプロイ完了"
echo "============================================================"
echo "  成功: ${SUCCESS_COUNT}"
echo "  失敗: ${FAIL_COUNT}"
echo "  スキップ: ${SKIPPED_COUNT}"
echo ""
if [ "${FAIL_COUNT}" -gt 0 ]; then
  echo "[WARNING] 一部のデプロイに失敗しました"
  exit 1
else
  echo "[SUCCESS] 全てのデプロイが完了しました"
fi
echo "============================================================"
