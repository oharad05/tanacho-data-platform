# Tanacho Data Platform

Google Drive → GCS → BigQuery データパイプライン

## 概要

月次でGoogle Drive共有フォルダからExcelファイルを取得し、GCS経由でBigQueryへ連携するデータパイプラインです。

## アーキテクチャ

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
├── columns/              # カラムマッピング定義（日本語→英語）
├── mapping/              # ファイル名マッピング
├── run_service/          # Cloud Run用サービス
├── function/             # Cloud Functions用（旧版）
├── sync_drive_to_gcs.py  # Drive → GCS連携
├── transform_raw_to_proceed.py # Excel → CSV変換
├── load_to_bigquery.py   # CSV → BigQuery連携
└── setup_instructions.md # セットアップ手順書
```

## 実行方法

### 1. Google Drive → GCS (raw/)
```bash
python sync_drive_to_gcs.py 202509
```

### 2. Excel → CSV変換 (raw/ → proceed/)
```bash
python transform_raw_to_proceed.py 202509
```

### 3. CSV → BigQuery連携
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

### データ設計方針
- 全テーブルは月次でパーティショニングされており、効率的なクエリ実行が可能
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

## 実装状況 (2025-10-24)

### 完了済み
- ✅ Google Drive → GCS (raw/) 連携: 7ファイル同期成功
- ✅ Excel → CSV変換 (raw/ → proceed/): 7ファイル変換成功
- ✅ BigQuery連携: 5/7テーブルロード成功
  - sales_target_and_achievements (790行)
  - billing_balance (1,020行)
  - ledger_income (63行)
  - department_summary (339行)
  - ledger_loss (6行)

### 対応中の課題
- ⚠️ internal_interest: 日付フォーマット変換エラー
- ⚠️ profit_plan_term: 日付フォーマット変換エラー
  - 原因: datetime64型の値が正しく文字列に変換されていない
  - 対応予定: transform_raw_to_proceed.pyの日付変換処理改善