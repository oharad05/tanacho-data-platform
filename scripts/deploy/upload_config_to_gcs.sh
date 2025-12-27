#!/bin/bash
# ============================================================
# 設定ファイル GCS アップロードスクリプト
# ============================================================
# 使用方法:
#   bash scripts/deploy/upload_config_to_gcs.sh
#
# 概要:
#   ローカルの設定ファイルを GCS にアップロード
#   - config/ → google-drive/config/ (columns, mapping)
#   - common/table_unique_keys.yml → config/table_unique_keys.yml
#   - spreadsheet_service/config/ → spreadsheet/config/
# ============================================================

set -e

PROJECT_ID="data-platform-prod-475201"
GCS_BUCKET="gs://data-platform-landing-prod"

# スクリプトのディレクトリを基準にプロジェクトルートを取得
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "============================================================"
echo "設定ファイル GCS アップロード"
echo "============================================================"
echo "Project: ${PROJECT_ID}"
echo "GCS Bucket: ${GCS_BUCKET}"
echo "============================================================"

# 成功/失敗カウント
SUCCESS_COUNT=0
FAIL_COUNT=0

# ------------------------------------------------------------
# 1. config/columns/ → google-drive/config/columns/
# ------------------------------------------------------------
echo ""
echo "[1/4] config/columns/ → google-drive/config/columns/"
LOCAL_COLUMNS="${PROJECT_ROOT}/config/columns"
GCS_COLUMNS="google-drive/config/columns"

if [ -d "${LOCAL_COLUMNS}" ]; then
  FILE_COUNT=$(find "${LOCAL_COLUMNS}" -name "*.csv" | wc -l | tr -d ' ')
  echo "  アップロード対象: ${FILE_COUNT} ファイル"

  if gsutil -m cp -r "${LOCAL_COLUMNS}"/*.csv "${GCS_BUCKET}/${GCS_COLUMNS}/" 2>/dev/null; then
    echo "  [OK] アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "  [NG] アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo "  [SKIP] ディレクトリが存在しません: ${LOCAL_COLUMNS}"
fi

# ------------------------------------------------------------
# 2. config/mapping/ → google-drive/config/mapping/
# ------------------------------------------------------------
echo ""
echo "[2/4] config/mapping/ → google-drive/config/mapping/"
LOCAL_MAPPING="${PROJECT_ROOT}/config/mapping"
GCS_MAPPING="google-drive/config/mapping"

if [ -d "${LOCAL_MAPPING}" ]; then
  FILE_COUNT=$(find "${LOCAL_MAPPING}" -name "*.csv" | wc -l | tr -d ' ')
  echo "  アップロード対象: ${FILE_COUNT} ファイル"

  if gsutil -m cp -r "${LOCAL_MAPPING}"/*.csv "${GCS_BUCKET}/${GCS_MAPPING}/" 2>/dev/null; then
    echo "  [OK] アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "  [NG] アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo "  [SKIP] ディレクトリが存在しません: ${LOCAL_MAPPING}"
fi

# ------------------------------------------------------------
# 3. common/table_unique_keys.yml → config/table_unique_keys.yml
# ------------------------------------------------------------
echo ""
echo "[3/4] common/table_unique_keys.yml → config/table_unique_keys.yml"
LOCAL_UNIQUE_KEYS="${PROJECT_ROOT}/common/table_unique_keys.yml"
GCS_UNIQUE_KEYS="config/table_unique_keys.yml"

if [ -f "${LOCAL_UNIQUE_KEYS}" ]; then
  if gsutil cp "${LOCAL_UNIQUE_KEYS}" "${GCS_BUCKET}/${GCS_UNIQUE_KEYS}" 2>/dev/null; then
    echo "  [OK] アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "  [NG] アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo "  [SKIP] ファイルが存在しません: ${LOCAL_UNIQUE_KEYS}"
fi

# ------------------------------------------------------------
# 4. spreadsheet_service/config/ → spreadsheet/config/
# ------------------------------------------------------------
echo ""
echo "[4/4] spreadsheet_service/config/ → spreadsheet/config/"
LOCAL_SS_CONFIG="${PROJECT_ROOT}/spreadsheet_service/config"
GCS_SS_CONFIG="spreadsheet/config"

if [ -d "${LOCAL_SS_CONFIG}" ]; then
  SS_UPLOAD_SUCCESS=true

  # columns
  if [ -d "${LOCAL_SS_CONFIG}/columns" ]; then
    COLS_COUNT=$(find "${LOCAL_SS_CONFIG}/columns" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    if [ "${COLS_COUNT}" -gt 0 ]; then
      echo "  columns: ${COLS_COUNT} ファイル"
      gsutil -m cp "${LOCAL_SS_CONFIG}/columns"/*.csv "${GCS_BUCKET}/${GCS_SS_CONFIG}/columns/" 2>/dev/null || SS_UPLOAD_SUCCESS=false
    else
      echo "  columns: 0 ファイル (スキップ)"
    fi
  fi

  # mapping
  if [ -d "${LOCAL_SS_CONFIG}/mapping" ]; then
    MAP_COUNT=$(find "${LOCAL_SS_CONFIG}/mapping" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    if [ "${MAP_COUNT}" -gt 0 ]; then
      echo "  mapping: ${MAP_COUNT} ファイル"
      gsutil -m cp "${LOCAL_SS_CONFIG}/mapping"/*.csv "${GCS_BUCKET}/${GCS_SS_CONFIG}/mapping/" 2>/dev/null || SS_UPLOAD_SUCCESS=false
    else
      echo "  mapping: 0 ファイル (スキップ)"
    fi
  fi

  if [ "${SS_UPLOAD_SUCCESS}" = true ]; then
    echo "  [OK] アップロード成功"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "  [NG] 一部アップロード失敗"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  echo "  [SKIP] ディレクトリが存在しません: ${LOCAL_SS_CONFIG}"
fi

# ------------------------------------------------------------
# 結果サマリー
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo "アップロード完了"
echo "============================================================"
echo "  成功: ${SUCCESS_COUNT}"
echo "  失敗: ${FAIL_COUNT}"
echo ""

if [ "${FAIL_COUNT}" -gt 0 ]; then
  echo "[WARNING] 一部のアップロードに失敗しました"
  exit 1
fi

echo "GCS 設定ファイル一覧:"
echo ""
echo "  google-drive/config/columns/:"
gsutil ls "${GCS_BUCKET}/google-drive/config/columns/"*.csv 2>/dev/null | wc -l | xargs -I {} echo "    {} ファイル"
echo ""
echo "  google-drive/config/mapping/:"
gsutil ls "${GCS_BUCKET}/google-drive/config/mapping/"*.csv 2>/dev/null | wc -l | xargs -I {} echo "    {} ファイル"
echo ""
echo "  config/:"
gsutil ls "${GCS_BUCKET}/config/"*.yml 2>/dev/null | wc -l | xargs -I {} echo "    {} ファイル"
echo ""
echo "  spreadsheet/config/:"
gsutil ls "${GCS_BUCKET}/spreadsheet/config/"**/*.csv 2>/dev/null | wc -l | xargs -I {} echo "    {} ファイル"
echo ""
echo "============================================================"
