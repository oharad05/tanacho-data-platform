# GCS アクセス設定ガイド

本ドキュメントでは、サービスアカウント `sa-data-platform` を使用してGCSにアクセスするための設定手順を説明します。

---

## 概要

### 対象者
- y.tanaka@tanacho.com

### 使用するサービスアカウント
- `sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com`

### アクセス先
- `gs://data-platform-landing-prod/`

### 認証方式
- 権限借用（Impersonation）

---

## 権限借用とは

各担当者は自分のGoogleアカウントでログインし、必要な時だけサービスアカウントの権限を借りる方式です。

```
y.tanaka@tanacho.com（個人アカウント）
    │
    │ 権限借用（Impersonate）
    ▼
sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
    │
    │ サービスアカウントの権限で実行
    ▼
  GCS（data-platform-landing-prod）
```

### メリット
- 鍵ファイルの共有が不要（セキュリティ向上）
- 監査ログで誰が操作したか追跡可能
- 権限の付与・剥奪が容易

---

## 管理者側の設定手順

### Step 1: 権限借用の許可を付与

```bash
gcloud iam service-accounts add-iam-policy-binding \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com \
  --member="user:y.tanaka@tanacho.com" \
  --role="roles/iam.serviceAccountTokenCreator"
```

### Step 2: 設定の確認

```bash
gcloud iam service-accounts get-iam-policy \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
```

出力例：
```yaml
bindings:
- members:
  - user:y.tanaka@tanacho.com
  role: roles/iam.serviceAccountTokenCreator
```

### 権限を削除する場合

```bash
gcloud iam service-accounts remove-iam-policy-binding \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com \
  --member="user:y.tanaka@tanacho.com" \
  --role="roles/iam.serviceAccountTokenCreator"
```

---

## 担当者側の設定手順

### Step 1: gcloud CLI のインストール

1. 以下のURLからgcloud CLIをダウンロード・インストール
   - https://cloud.google.com/sdk/docs/install

2. インストール後、ターミナル（コマンドプロンプト）を再起動

### Step 2: Googleアカウントでログイン

```bash
gcloud auth login
```

ブラウザが開くので、`y.tanaka@tanacho.com` でログインします。

### Step 3: プロジェクトの設定

```bash
gcloud config set project data-platform-prod-475201
```

### Step 4: サービスアカウントの権限借用を設定

```bash
gcloud config set auth/impersonate_service_account \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
```

### Step 5: 設定の確認

```bash
gcloud config list
```

出力に以下が含まれていればOK：
```
[auth]
impersonate_service_account = sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
```

---

## GCS操作コマンド

### ファイル一覧の表示

```bash
# rawフォルダの一覧
gsutil ls gs://data-platform-landing-prod/raw/

# 特定月のファイル一覧
gsutil ls gs://data-platform-landing-prod/raw/202511/
```

### ファイルのアップロード

```bash
# 単一ファイル
gsutil cp ./売上データ.xlsx gs://data-platform-landing-prod/raw/202511/

# 複数ファイル
gsutil cp ./*.xlsx gs://data-platform-landing-prod/raw/202511/
```

### ファイルのダウンロード

```bash
# 単一ファイル
gsutil cp gs://data-platform-landing-prod/raw/202511/売上データ.xlsx ./

# フォルダごと
gsutil -m cp -r gs://data-platform-landing-prod/raw/202511/ ./
```

### ファイルの削除

```bash
gsutil rm gs://data-platform-landing-prod/raw/202511/不要ファイル.xlsx
```

---

## トラブルシューティング

### エラー: "does not have serviceusage.services.use access"

権限借用が正しく設定されていません。管理者に連絡してください。

### エラー: "Cannot impersonate service account"

権限借用の許可が付与されていません。管理者に連絡してください。

### 権限借用を一時的に解除したい場合

```bash
gcloud config unset auth/impersonate_service_account
```

再度有効にする場合：
```bash
gcloud config set auth/impersonate_service_account \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com
```

---

## 担当者追加時の手順

新しい担当者を追加する場合、管理者が以下を実行：

```bash
# 新しい担当者のメールアドレスを設定
NEW_USER="new.user@tanacho.com"

gcloud iam service-accounts add-iam-policy-binding \
  sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com \
  --member="user:${NEW_USER}" \
  --role="roles/iam.serviceAccountTokenCreator"
```

---

## 関連情報

- サービスアカウント: `sa-data-platform@data-platform-prod-475201.iam.gserviceaccount.com`
- GCSバケット: `gs://data-platform-landing-prod/`
- GCPプロジェクト: `data-platform-prod-475201`

---

## 更新履歴

| 日付 | 内容 |
|-----|------|
| 2025-12-11 | 初版作成 |
