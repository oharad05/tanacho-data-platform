# GCP ドメイン全体の委任（Domain-wide Delegation）設定手順

## 概要

第三者でもCloud RunからGoogle Drive・スプレッドシートにアクセスできるようにするため、サービスアカウントのドメイン全体の委任を設定する。

## 対象サービス

| サービス名 | Cloud Run名 | 用途 |
|-----------|-------------|------|
| run_service | drive-to-gcs | DriveからGCSへファイル転送 |
| spreadsheet_service | spreadsheet-to-gcs | スプレッドシートからBigQueryへ連携 |

## サービスアカウント情報

| 項目 | 値 |
|------|-----|
| サービスアカウント名 | sa-data-platform |
| メールアドレス | sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com |
| 一意のID（Client ID） | **111248363106241056764** |

## 設定手順

### 手順1: JSONキーファイルの準備

サービスアカウント `sa-data-platform` のJSONキーファイルを用意する。
既存のキーID: `cc9ec1272c4560e2549292f0812f08325a6a496a`

### 手順2: Google管理コンソールでの設定（情シス担当者が実施）

1. 管理コンソール（admin.google.com）にログイン
2. [セキュリティ] > [アクセスとデータ管理] > [API の制御] に移動
3. 画面下部の「ドメイン全体の委任を管理」をクリック
4. [新しく追加] をクリック
5. 以下を入力:
   - **クライアント ID**: `111248363106241056764`
   - **OAuth スコープ**: `https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive`
6. [承認] をクリック

### 手順3: Secret ManagerにJSONキーをアップロード

```bash
# シークレットは既に作成済み
# JSONキーファイルをアップロード
gcloud secrets versions add sa-data-platform-key \
  --project=data-platform-prod-475201 \
  --data-file=/path/to/your/key.json
```

### 手順4: Cloud Runサービスのデプロイ

#### drive-to-gcs のデプロイ

```bash
gcloud run deploy drive-to-gcs \
  --project=data-platform-prod-475201 \
  --region=asia-northeast1 \
  --image=asia-northeast1-docker.pkg.dev/data-platform-prod-475201/cloud-run-source-deploy/drive-to-gcs \
  --set-env-vars="GCP_PROJECT=data-platform-prod-475201,DRIVE_FOLDER_ID=1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6,LANDING_BUCKET=data-platform-landing-prod,MAPPING_GCS_PATH=config/mapping_files.csv,IMPERSONATE_USER=fiby2@tanacho.com" \
  --set-secrets="/secrets/sa-key.json=sa-data-platform-key:latest" \
  --update-env-vars="SERVICE_JSON_PATH=/secrets/sa-key.json"
```

#### spreadsheet-to-gcs のデプロイ

```bash
# まずイメージをビルド
cd spreadsheet_service
gcloud builds submit \
  --tag asia-northeast1-docker.pkg.dev/data-platform-prod-475201/cloud-run-source-deploy/spreadsheet-to-gcs \
  --project=data-platform-prod-475201

# デプロイ
gcloud run deploy spreadsheet-to-gcs \
  --project=data-platform-prod-475201 \
  --region=asia-northeast1 \
  --image=asia-northeast1-docker.pkg.dev/data-platform-prod-475201/cloud-run-source-deploy/spreadsheet-to-gcs \
  --set-env-vars="GCP_PROJECT=data-platform-prod-475201,LANDING_BUCKET=data-platform-landing-prod,MANUAL_INPUT_FOLDER_ID=1O4eUpl6AWgag1oMTyrtoA7sXEHX3mfxc,IMPERSONATE_USER=fiby2@tanacho.com" \
  --set-secrets="/secrets/sa-key.json=sa-data-platform-key:latest" \
  --update-env-vars="SERVICE_JSON_PATH=/secrets/sa-key.json"
```

## コード変更内容

### run_service/main.py

```python
# 追加された環境変数
IMPERSONATE_USER = os.environ.get("IMPERSONATE_USER")

# スコープを変更（readonlyを外す）
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

# _build_drive_service() に .with_subject() を追加
def _build_drive_service():
    if SERVICE_JSON:
        creds = service_account.Credentials.from_service_account_file(SERVICE_JSON, scopes=SCOPES)
        if IMPERSONATE_USER:
            creds = creds.with_subject(IMPERSONATE_USER)  # ★追加
            print(f"[INFO] Domain-wide delegation enabled: impersonating {IMPERSONATE_USER}")
    else:
        creds, _ = google_auth_default(scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)
```

### spreadsheet_service/main.py

同様の変更を `_get_credentials()` 関数として共通化。

## 環境変数一覧

| 変数名 | 説明 | 例 |
|--------|------|-----|
| SERVICE_JSON_PATH | JSONキーファイルのパス | /secrets/sa-key.json |
| IMPERSONATE_USER | なりすますユーザーのメールアドレス | fiby2@tanacho.com |

## 注意事項

1. **IMPERSONATE_USER** に指定するユーザーは、対象のDrive・スプレッドシートへのアクセス権限が必要
2. 可能であれば専用のシステムユーザーアカウント（例: `system-etl@tanacho.com`）を使用することを推奨
3. スコープは管理コンソールで登録したものと完全一致させる必要がある

## 現在のステータス

- [x] コード修正完了（run_service/main.py, spreadsheet_service/main.py）
- [x] Secret Manager API有効化
- [x] シークレット `sa-data-platform-key` 作成
- [x] drive-to-gcs イメージビルド完了
- [ ] JSONキーファイルをSecret Managerにアップロード
- [ ] 管理コンソールでドメイン全体の委任を設定
- [ ] Cloud Runサービスのデプロイ（シークレットマウント付き）
- [ ] 動作確認
