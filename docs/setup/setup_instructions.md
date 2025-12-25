# Google Drive → GCS 連携セットアップ手順

## 概要
Google Drive共有フォルダの月次フォルダ内の.xlsxファイルをGCSに連携する手順です。

## 固定値設定
- **サービスアカウント**: `102847004309-compute@developer.gserviceaccount.com`
- **フォルダID**: `1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6` (02_データソース)
- **GCSバケット**: `data-platform-landing-prod`
- **プロジェクトID**: `data-platform-prod-475201`

## 1. 必要な権限設定

### 1.1 Google Cloud Console での設定

1. **Drive APIを有効化**
   ```bash
   gcloud services enable drive.googleapis.com --project=data-platform-prod-475201
   ```

2. **サービスアカウントのGCS権限**
   ```bash
   # GCSバケットへの書き込み権限
   gsutil iam ch serviceAccount:102847004309-compute@developer.gserviceaccount.com:roles/storage.objectAdmin gs://data-platform-landing-prod_NAME
   ```

### 1.2 Google Drive での権限設定

1. **Google Driveで共有フォルダを開く**
2. **右クリック → 「共有」を選択**
3. **サービスアカウントのメールアドレスを追加**:
   - メール: `102847004309-compute@developer.gserviceaccount.com`
   - 権限: 「閲覧者」以上
4. **「送信」をクリック**

**重要**: 共有ドライブの場合は、共有ドライブ自体のメンバーとして追加する必要があります。

## 2. 動作確認済み環境

- **フォルダID**: `1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6` (固定)
- **対象フォルダ**: 02_データソース/202509
- **ファイル数**: 7個のExcelファイル

## 3. 環境変数の設定

### ローカルテスト用
```bash
# 固定値（変更不要）
export DRIVE_FOLDER_ID="1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6"
export LANDING_BUCKET="data-platform-landing-prod"
export GCP_PROJECT="data-platform-prod-475201"
```

### Cloud Run用の環境変数
```yaml
env:
  - name: DRIVE_FOLDER_ID
    value: "1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6"  # 固定値
  - name: LANDING_BUCKET
    value: "data-platform-landing-prod"  # 固定値
  - name: MAPPING_GCS_PATH
    value: "config/mapping_files.csv"
```

## 4. マッピングファイルの配置

```bash
# マッピングファイルをGCSにアップロード（実行済み）
gsutil cp mapping/excel_mapping.csv gs://data-platform-landing-prod/config/mapping_files.csv
```

## 5. 接続テスト

### 5.1 テストスクリプトの実行
```bash
# 認証設定（Drive APIスコープを含む）
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform

# テスト実行
python test_drive_connection.py
```

### 5.2 確認項目
- ✅ サービスアカウント認証成功
- ✅ Drive API接続成功  
- ✅ フォルダアクセス成功
- ✅ 202509フォルダの発見
- ✅ .xlsxファイルの一覧表示

## 6. Cloud Runへのデプロイ

### 6.1 Dockerイメージのビルド
```bash
cd run_service

# Cloud Build を使用
gcloud builds submit --tag gcr.io/data-platform-prod-475201/drive-to-gcs

# または、ローカルでビルド
docker build -t gcr.io/data-platform-prod-475201/drive-to-gcs .
docker push gcr.io/data-platform-prod-475201/drive-to-gcs
```

### 6.2 Cloud Runデプロイ
```bash
gcloud run deploy drive-to-gcs \
  --image gcr.io/data-platform-prod-475201/drive-to-gcs \
  --platform managed \
  --region asia-northeast1 \
  --service-account 102847004309-compute@developer.gserviceaccount.com \
  --set-env-vars "DRIVE_FOLDER_ID=1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6,LANDING_BUCKET=data-platform-landing-prod,MAPPING_GCS_PATH=config/mapping_files.csv" \
  --memory 1Gi \
  --timeout 540
```

## 7. Cloud Workflowsの設定

### 7.1 Cloud Workflowsのデプロイ
```bash
# ワークフローをデプロイ
gcloud workflows deploy data-pipeline \
  --location=asia-northeast1 \
  --source=workflows/data_pipeline.yaml \
  --service-account=sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
```

### 7.2 ワークフローの確認
```bash
# デプロイ済みワークフローを確認
gcloud workflows list --location=asia-northeast1
```

## 8. 動作確認

### 8.1 手動実行テスト
```bash
# Cloud Workflowsで一括実行
gcloud workflows run data-pipeline \
  --location=asia-northeast1 \
  --data='{"mode": "replace"}'
```

### 8.2 GCS確認
```bash
# アップロードされたファイルを確認
gsutil ls -l gs://data-platform-landing-prod/raw/202509/
```

実行結果（確認済み）:
```
gs://data-platform-landing-prod/raw/202509/sales_target_and_achievements.xlsx (38KB)
gs://data-platform-landing-prod/raw/202509/billing_balance.xlsx (21KB)
gs://data-platform-landing-prod/raw/202509/ledger_income.xlsx (14KB)
gs://data-platform-landing-prod/raw/202509/department_summary.xlsx (22KB)
gs://data-platform-landing-prod/raw/202509/internal_interest.xlsx (10KB)
gs://data-platform-landing-prod/raw/202509/profit_plan_term.xlsx (28KB)
gs://data-platform-landing-prod/raw/202509/ledger_loss.xlsx (10KB)
```

## 9. トラブルシューティング

### 権限エラーの場合
```bash
# サービスアカウントの権限確認
gcloud projects get-iam-policy data-platform-prod-475201 \
  --flatten="bindings[].members" \
  --filter="bindings.members:102847004309-compute@developer.gserviceaccount.com"

# Drive API有効化確認
gcloud services list --enabled | grep drive
```

### フォルダが見つからない場合
1. フォルダIDが正しいか確認
2. サービスアカウントに共有されているか確認
3. 202509フォルダが存在するか確認

### デバッグ用エンドポイント
```bash
# フォルダ情報の確認
curl "${SERVICE_URL}/debug/folder?id=YOUR_FOLDER_ID"
```

## 10. ローカル実行スクリプト

ローカル環境から直接実行する場合:
```bash
# 依存ライブラリインストール
pip install google-cloud-storage pandas openpyxl google-api-python-client

# 実行
python sync_drive_to_gcs.py 202509
```

## 11. raw/ → proceed/ 変換処理

### 概要
Excelファイル(.xlsx)をCSVに変換し、カラム名を日本語から英語にマッピングして、BigQuery連携用のデータに整形します。

### 実装済み機能
- Excel → CSV変換
- 日本語カラム名 → 英語カラム名マッピング  
- BigQuery用データ型変換
  - DATE型: '2025/09' → '2025-09-01'
  - DATETIME型: タイムスタンプ形式
  - INT64/NUMERIC/STRING型: 適切な型変換

### 実行方法

#### ローカル実行
```bash
# 必要なライブラリ
pip install pandas openpyxl google-cloud-storage

# 変換処理実行
python transform_raw_to_proceed.py 202509

# ローカルテスト（サンプルファイル使用）
python transform_raw_to_proceed.py 202509 --local
```

#### 実行結果の確認
```bash
# 変換されたCSVファイルを確認
gsutil ls -l gs://data-platform-landing-prod/proceed/202509/

# CSVの内容確認
gsutil cat gs://data-platform-landing-prod/proceed/202509/sales_target_and_achievements.csv | head
```

### 処理フロー
```
1. raw/{yyyymm}/{table}.xlsx をGCSから読み込み
2. columns/{table}.csv のマッピング定義を適用
3. データ型変換（DATE, INT64, STRING等）
4. proceed/{yyyymm}/{table}.csv としてGCSに保存
```

### 実行結果（202509）
全7テーブルの変換に成功:
- sales_target_and_achievements.csv (55KB, 395行)
- billing_balance.csv (21KB, 340行)
- ledger_income.csv (6KB, 21行)
- department_summary.csv (13KB, 113行)
- internal_interest.csv (3KB, 41行)
- profit_plan_term.csv (7KB, 72行)
- ledger_loss.csv (1KB, 2行)

## 12. Cloud Scheduler による定期実行（オプション）

```bash
# 毎月1日の9時に実行（Cloud Workflowsを呼び出し）
gcloud scheduler jobs create http monthly-pipeline-sync \
  --schedule="0 9 1 * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/data-platform-prod-475201/locations/asia-northeast1/workflows/data-pipeline/executions" \
  --http-method=POST \
  --message-body='{"argument":"{\"mode\":\"replace\"}"}' \
  --oauth-service-account-email=sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com \
  --time-zone="Asia/Tokyo" \
  --location=asia-northeast1
```