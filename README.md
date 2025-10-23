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
    ↓ [3. BigQuery連携 (未実装)]
BigQuery
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

### 3. 結果確認
```bash
# raw/ の確認
gsutil ls -l gs://data-platform-landing-prod/raw/202509/

# proceed/ の確認
gsutil ls -l gs://data-platform-landing-prod/proceed/202509/
```

## 対応テーブル

1. **sales_target_and_achievements** - 売上目標/実績
2. **billing_balance** - 請求残高
3. **ledger_income** - 元帳_雑収入
4. **department_summary** - 部門集計表
5. **internal_interest** - 社内金利
6. **profit_plan_term** - 損益5期目標
7. **ledger_loss** - 元帳_雑損失

## セットアップ

詳細は [setup_instructions.md](setup_instructions.md) を参照してください。

## 必要な権限

- Google Drive API の読み取り権限
- GCS バケットへの読み書き権限
- 共有フォルダへのアクセス権限

## 依存ライブラリ

```bash
pip install google-cloud-storage pandas openpyxl google-api-python-client
```