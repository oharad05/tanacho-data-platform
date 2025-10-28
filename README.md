# Tanacho Data Platform

Google Drive → GCS → BigQuery データパイプライン

## 概要

月次でGoogle Drive共有フォルダからExcelファイルを取得し、GCS経由でBigQueryへ連携するデータパイプラインです。

## アーキテクチャ

### 本番環境（Cloud Run）
```
Google Drive (共有フォルダ)
    ↓
[Pub/Sub: drive-monthly]
    ↓
[Cloud Run: drive-to-gcs]
    ↓
GCS (raw/)  - Excelファイル保存
    ↓
[Pub/Sub: transform-trigger]
    ↓
[Cloud Run: gcs-to-bq]
    ├─ /transform: Excel → CSV変換
    └─ /load: CSV → BigQuery
    ↓
GCS (proceed/) - CSV変換・カラムマッピング済み
    ↓
BigQuery (パーティション化テーブル)
```

### ローカル実行（開発・テスト用）
```
Google Drive (共有フォルダ)
    ↓ [1. sync_drive_to_gcs.py]
GCS (raw/)  - Excelファイル保存
    ↓ [2. transform_raw_to_proceed.py]
GCS (proceed/) - CSV変換・カラムマッピング済み
    ↓ [3. load_to_bigquery.py]
BigQuery (パーティション化テーブル)
```

## 環境設定

### 固定値
- **プロジェクトID**: `data-platform-prod-475201`
- **GCSバケット**: `data-platform-landing-prod`
- **共有フォルダID**: `1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6`
- **サービスアカウント**: `102847004309-compute@developer.gserviceaccount.com`

## ディレクトリ構成

```
├── mapping/              # 設定ファイル（Git管理対象、GCSと同期）
│   ├── mapping_files.csv         # ファイル名マッピング（日本語→英語）
│   ├── monetary_scale_conversion.csv # 金額単位変換設定
│   └── columns/                   # カラム定義ファイル
│       ├── stocks.csv
│       ├── ms_allocation_ratio.csv
│       ├── ms_department_category.csv
│       └── ... (他のテーブル定義)
├── columns/              # カラムマッピング定義（ローカル実行用）
├── run_service/          # Cloud Run用サービス
├── function/             # Cloud Functions用（旧版）
├── sync_drive_to_gcs.py  # Drive → GCS連携
├── transform_raw_to_proceed.py # Excel → CSV変換
├── load_to_bigquery.py   # CSV → BigQuery連携
└── setup_instructions.md # セットアップ手順書
```

## 実行方法

### 本番環境（Pub/Sub経由）

#### 全体フローの実行
```bash
# drive-monthly トピックにメッセージを送信
gcloud pubsub topics publish drive-monthly \
  --message='{"yyyymm":"202509"}' \
  --project=data-platform-prod-475201

# 自動的に以下が実行されます:
# 1. drive-to-gcs: Drive → GCS (raw/)
# 2. gcs-to-bq: raw/ → proceed/ → BigQuery
```

#### 個別エンドポイントの実行
```bash
# Transform のみ実行
curl -X POST https://gcs-to-bq-102847004309.asia-northeast1.run.app/transform \
  -H "Content-Type: application/json" \
  -d '{"yyyymm":"202509"}'

# Load のみ実行
curl -X POST https://gcs-to-bq-102847004309.asia-northeast1.run.app/load \
  -H "Content-Type: application/json" \
  -d '{"yyyymm":"202509", "replace":true}'
```

### ローカル実行（開発・テスト用）

#### 1. Google Drive → GCS (raw/)
```bash
python sync_drive_to_gcs.py 202509
```

#### 2. Excel → CSV変換 (raw/ → proceed/)
```bash
python transform_raw_to_proceed.py 202509
```

#### 3. CSV → BigQuery連携
```bash
python load_to_bigquery.py 202509
# --replaceオプションで既存データを置換
python load_to_bigquery.py 202509 --replace
```

### 4. 結果確認
```bash
# raw/ の確認
gsutil ls -l gs://data-platform-landing-prod/raw/202509/

# proceed/ の確認
gsutil ls -l gs://data-platform-landing-prod/proceed/202509/

# BigQuery確認
bq ls --project_id=data-platform-prod-475201 corporate_data
```

## BigQueryテーブル一覧

### 1. sales_target_and_achievements (売上目標と実績)
- **目的**: 営業所・部署・担当者別の売上・粗利の目標と実績を管理
- **パーティション**: `sales_accounting_period` (売上計上月度)
- **クラスタリング**: `branch_code` (営業所コード)
- **主要カラム**:
  - 営業所/部署/担当者情報（コード・名称）
  - 売上目標・実績、粗利目標・実績
  - 前年同期のデータ（売上・粗利の目標と実績）

### 2. billing_balance (請求残高)
- **目的**: 営業所別の売掛金・手形残高を管理
- **パーティション**: `sales_month` (売上月度)
- **クラスタリング**: `branch_code` (営業所CD)
- **主要カラム**:
  - 当月売掛残高
  - 未落手形残高
  - 当月売上残高

### 3. ledger_income (収益元帳)
- **目的**: 収益側の会計伝票データを記録
- **パーティション**: `slip_date` (伝票日付、DATETIME型)
- **クラスタリング**: `classification_type` (整理区分)
- **主要カラム**:
  - 会計月、伝票日付、伝票No
  - 相手・自の勘定科目/補助科目/部門/取引先情報
  - 消費税情報（区分・税率・控除割合）
  - 借方・貸方・金額・残高

### 4. ledger_loss (費用元帳)
- **目的**: 費用側の会計伝票データを記録
- **パーティション**: `slip_date` (伝票日付、DATETIME型)
- **クラスタリング**: `classification_type` (整理区分)
- **主要カラム**: ledger_incomeと同じ構造

### 5. department_summary (部門サマリー)
- **目的**: 科目別・部門別の集計データ
- **パーティション**: `sales_accounting_period`
- **クラスタリング**: `code` (コード)
- **主要カラム**:
  - 科目名、合計
  - 本店、各営業課（工事営業1課、改修課、業務課など）
  - 各施工部門（第一施工、第二施工）
  - 各営業部門（硝子建材営業部、業務部、施工部など）

### 6. internal_interest (社内利息)
- **目的**: 支店間の社内利息計算データ
- **パーティション**: `year_month` (年月)
- **クラスタリング**: `branch` (支店)
- **主要カラム**:
  - 支店、分類、内訳
  - 金額、利率、利息

### 7. profit_plan_term (利益計画期間)
- **目的**: 期間別・担当者別の利益計画データ
- **パーティション**: `period` (期間)
- **クラスタリング**: `item` (項目)
- **主要カラム**:
  - 東京支店計、工事営業部計
  - 各担当者別（佐々木、浅井、小笠原、高石、山本など）
  - 各事業区分別（硝子工事、硝子販売、サッシ販売など）

### 8. stocks (在庫)
- **目的**: 支店・部署別の在庫データを管理
- **パーティション**: `year_month` (年月、DATE型)
- **クラスタリング**: `branch` (支店)
- **主要カラム**:
  - 支店、部署、種別（期末未成工事、当月在庫など）
  - 金額、年月

### 9. ms_allocation_ratio (案分比率マスタ)
- **目的**: 支店・部署別の案分比率を管理（マスタテーブル）
- **パーティション**: なし（マスタテーブルのためパーティション化不要）
- **クラスタリング**: `branch` (支店)
- **主要カラム**:
  - 支店、部署、種別（業務部門案分など）
  - 比率、年月（DATE型）

### 10. ms_department_category (部門カテゴリマスタ)
- **目的**: 部門カテゴリコードと名称の対照表（マスタテーブル）
- **パーティション**: なし（マスタテーブルのためパーティション化不要）
- **クラスタリング**: `department_category_code` (部門カテゴリコード、INTEGER型)
- **主要カラム**:
  - 部門カテゴリコード（INTEGER型: 0, 10, 11, 13, 18, 20...）
  - 部門カテゴリコード名（本店、工事営業１課、改修課、硝子建材営業課など）

### データ設計方針
- **パーティション化ルール**:
  - **トランザクションテーブル**: 日付カラムでパーティション化（月次パーティション）
  - **マスタテーブル**: パーティション化不要
    - `ms_` プレフィックスがついているテーブルはマスタテーブルとして扱う
    - 例: `ms_allocation_ratio`, `ms_department_category`
- カラムマッピング定義は `columns/` ディレクトリ配下のCSVファイルで管理
- 日本語カラム名から英語カラム名への変換は自動化

## セットアップ

詳細は [setup_instructions.md](setup_instructions.md) を参照してください。

## 必要な権限

- Google Drive API の読み取り権限
- GCS バケットへの読み書き権限
- 共有フォルダへのアクセス権限

## 依存ライブラリ

```bash
pip install google-cloud-storage pandas openpyxl google-api-python-client google-cloud-bigquery
```

## 実装状況 (2025-10-28)

### 完了済み
- ✅ Google Drive → GCS (raw/) 連携: 10ファイル同期成功
- ✅ Excel → CSV変換 (raw/ → proceed/): 10ファイル変換成功
- ✅ BigQuery連携: 10テーブルロード成功
  - sales_target_and_achievements (395行)
  - billing_balance (478,880行)
  - ledger_income (91,035行)
  - department_summary (226行)
  - internal_interest (82行)
  - profit_plan_term (248,886行)
  - ledger_loss (8,670行)
  - **stocks (4行)** ← 新規追加
  - **ms_allocation_ratio (132行)** ← 新規追加
  - **ms_department_category (27行)** ← 新規追加

### 設定ファイル管理
- ✅ `mapping/` ディレクトリをGit管理対象として確立
  - `mapping/mapping_files.csv`: ファイル名マッピング（日本語→英語）
  - `mapping/columns/*.csv`: カラム定義ファイル（日本語→英語、型定義、説明）
  - `mapping/monetary_scale_conversion.csv`: 金額単位変換設定
- ✅ GCSとの同期: `mapping/` → `gs://data-platform-landing-prod/config/`