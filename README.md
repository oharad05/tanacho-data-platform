# タナチョウパイプライン

Google Drive → GCS → BigQuery → Looker Studio のデータパイプライン

## 概要

Google Drive上の月次データをGCSに取り込み、BigQueryに連携し、Looker Studioでダッシュボード表示するためのパイプラインです。

## 用語定義

**重要**: 以下の用語は全てのSQL実装で統一して使用してください。

- **実行日**: レポート作成対象の翌月
  - 例: 9/1のレポートの場合、実行日 = 2025年10月
- **作成月**: 実行日の前月（=全ての数字が集まっている最新の月）
  - 例: 9/1のレポートの場合、作成月 = 2025年9月
- **期首**: 会計年度の開始日 = **9月1日**
  - 9月以降の月の期首: その年の9月1日
  - 1-8月の期首: 前年の9月1日
  - 例: 2024年9月の期首 = 2024年9月1日
  - 例: 2025年2月の期首 = 2024年9月1日

### 営業外費用（社内利息）の計算における適用例

9/1の経営資料の場合:
- **値A（売掛残高）**: 実行日（10月）の2か月前 = **2025年8月**の売掛残高
- **値B（利率）**: 作成月（9月）の前月 = **2025年8月**の利率
- 計算: 値A × 値B = 8月の売掛残高 × 8月の利率

## アーキテクチャ

```
Google Drive (月次Excelファイル)
    ↓
GCS (data-platform-landing-prod)
    ├── raw/{YYYYMM}/*.xlsx
    └── proceed/{YYYYMM}/*.csv
    ↓
BigQuery
    ├── corporate_data (元データ)
    ├── corporate_data_dwh (DWHテーブル)
    └── corporate_data_dm (データマート)
    ↓
Looker Studio (ダッシュボード)
```

## 実行手順

### 処理フロー概要

```
1. Google Drive配置（手動・先方作業）
   ↓
2. Cloud Run: Drive → GCS (raw/)
   サービス: drive-to-gcs (run_service/main.py)
   トリガー: Pub/Sub topic 'drive-monthly'
   ↓
3. Cloud Run: raw/ → proceed/ + BigQuery連携
   サービス: gcs-to-bq (gcs_to_bq_service/main.py)
   トリガー: Pub/Sub topic 'transform-trigger'
   処理内容:
     - Excel → CSV変換 (raw/ → proceed/)
     - CSV → BigQuery読み込み (proceed/ → corporate_data)
   ↓
4. DWHテーブル更新（手動）
   スクリプト: sql/scripts/update_dwh.sh
   ↓
5. データマート更新（手動）
   スクリプト: sql/scripts/update_datamart.sh
   ↓
6. Looker Studio可視化
```

### 自動化範囲

| ステップ | 処理内容 | 自動化 | トリガー方法 |
|---------|---------|--------|-------------|
| 1 | Drive配置 | ✗ 手動 | 先方作業 |
| 2 | Drive→GCS(raw/) | ✓ 自動 | Pub/Sub: drive-monthly |
| 3 | raw/→proceed/ + BQ連携 | ✓ 自動 | Pub/Sub: transform-trigger |
| 4 | DWH作成 | ✗ 手動 | update_dwh.sh |
| 5 | DataMart作成 | ✗ 手動 | update_datamart.sh |
| 6 | Looker Studio | - | 手動参照 |

### インフラ構成

**Pub/Sub**:
- Topic: `drive-monthly`
  - Subscription: `drive-monthly-sub` → Cloud Run `drive-to-gcs` (push)
- Topic: `transform-trigger`
  - Subscription: `transform-trigger-sub` → Cloud Run `gcs-to-bq` (push)

**Cloud Run Services**:
- `drive-to-gcs` (asia-northeast1) - run_service/main.py
- `gcs-to-bq` (asia-northeast1) - gcs_to_bq_service/main.py

**Cloud Scheduler**: なし（DWH・DataMart更新は手動実行）

## 月次データ更新手順

### 1. Drive → GCS 同期（自動）

先方がGoogle Driveにファイルを配置すると、Pub/Sub経由で自動的に処理されます。

手動実行する場合:
```bash
python sync_drive_to_gcs.py {YYYYMM}
```

例: `python sync_drive_to_gcs.py 202509`

### 2. raw → proceed 変換 + BigQuery連携（自動）

ステップ1の完了後、Pub/Sub経由で自動的に処理されます。

手動実行する場合:
```bash
python transform_raw_to_proceed.py {YYYYMM}
python load_to_bigquery.py {YYYYMM} --replace
```

### 3. マスターデータ更新（初回のみ必要）

```bash
bq query --use_legacy_sql=false < sql/update_ms_department_category_group_name.sql
```

### 4. DWHテーブル更新（手動実行必須）

```bash
bash sql/scripts/update_dwh.sh
```

または個別実行:
```bash
cd sql/split_dwh_dm

# 各DWHテーブルを更新
for file in dwh_*.sql; do
  table_name=$(echo $file | sed 's/dwh_//' | sed 's/.sql//')
  echo "Processing: $table_name"

  # TRUNCATE & INSERT方式で更新
  {
    grep "^DECLARE" $file 2>/dev/null || echo ""
    echo ""
    echo "TRUNCATE TABLE \`data-platform-prod-475201.corporate_data_dwh.${table_name}\`;"
    echo ""
    echo "INSERT INTO \`data-platform-prod-475201.corporate_data_dwh.${table_name}\`"
    grep -v "^DECLARE" $file
  } | bq query --use_legacy_sql=false
done
```

**更新対象テーブル（11個）**:
- dwh_sales_actual, dwh_sales_actual_prev_year, dwh_sales_target
- operating_expenses, non_operating_income, non_operating_expenses
- miscellaneous_loss, head_office_expenses, dwh_recurring_profit_target
- operating_expenses_target, operating_income_target

### 5. データマート更新（手動実行必須）

```bash
bash sql/scripts/update_datamart.sh
```

または個別実行:
```bash
cd sql/split_dwh_dm

# データマートテーブルを更新
{
  grep "^DECLARE" datamart_management_report_vertical.sql 2>/dev/null || echo ""
  echo ""
  echo "TRUNCATE TABLE \`data-platform-prod-475201.corporate_data_dm.management_documents_all_period\`;"
  echo ""
  echo "INSERT INTO \`data-platform-prod-475201.corporate_data_dm.management_documents_all_period\`"
  grep -v "^DECLARE" datamart_management_report_vertical.sql
} | bq query --use_legacy_sql=false
```

**更新対象テーブル**:
- management_documents_all_period

## 重要な注意事項

### テーブル作成について

- **テーブルは既に作成済みです**
- 各SQLファイルはSELECT文のみを含んでいます
- 実行時は`TRUNCATE TABLE` + `INSERT INTO`でデータを置き換えます
- `CREATE TABLE`や`CREATE OR REPLACE TABLE`は使用しないでください

### 対象月の指定

- 各SQLファイル内の`DECLARE target_month`で対象月を自動計算しています（前月）
- 手動で特定の月を処理する場合は、各SQLファイル内のDATE値を修正してください

### パーティション仕様

- 既存テーブルにはパーティション設定があります
- `CREATE OR REPLACE TABLE`を使用するとパーティション仕様が変わるためエラーになります
- 必ず`TRUNCATE` + `INSERT`方式を使用してください

### detail_category 命名規則

**基本ルール**: 合計値を取っているdetail_categoryは「計」をつけ、そうでなければつけない

この命名規則は、売上データと経費データを正しくJOINするために重要です。

**「計」をつける例（集計レベル）**:
- `東京支店計` - 東京支店全体の合計
- `工事営業部計` - 工事営業部全体の合計
- `硝子建材営業部計` - 硝子建材営業部全体の合計
- `ガラス工事計` - ガラス工事グループの合計

**「計」をつけない例（個別レベル）**:
- `佐々木（大成・鹿島他）` - 個人担当者
- `浅井（清水他）` - 個人担当者
- `山本（改装）` - 個人担当者
- `硝子工事` - 個別部門
- `ビルサッシ` - 個別部門
- `硝子販売` - 個別部門

**JOIN時の注意点**:
- 売上データ（DWH）と経費データを結合する際は、`detail_category`をキーとして使用
- 経費データは集計レベル（「計」付き）でのみ存在する場合がある
- `parent_organization`も併用することで、正確な結合を保証

## トラブルシューティング

### エラー: "Cannot replace a table with a different partitioning spec"

テーブル作成時に`CREATE OR REPLACE TABLE`を使用した場合に発生します。
`TRUNCATE` + `INSERT`方式に変更してください。

### エラー: "Table not found"

テーブル名が間違っている可能性があります。

- DWHテーブル: `corporate_data_dwh.{table_name}`
- DMテーブル: `corporate_data_dm.management_documents_current_month_tbl`

### データが表示されない

1. BigQueryでテーブルのレコード数を確認
2. パーティション列の値を確認（対象月と一致しているか）
3. Looker Studioのフィルタ設定を確認

## 支店別組織構造

### 東京支店

```
東京支店計
├─ 工事営業部計
│   ├─ ガラス工事計 (工事営業１課 + 業務課)
│   │   ├─ 佐々木（大成・鹿島他）
│   │   ├─ 浅井（清水他）
│   │   ├─ 小笠原（三井住友他）
│   │   └─ 高石（内装・リニューアル）
│   └─ 山本（改装） (改修課)
└─ 硝子建材営業部
    ├─ 硝子工事
    ├─ ビルサッシ
    ├─ 硝子販売
    ├─ サッシ販売
    ├─ サッシ完成品
    └─ その他
```

### 長崎支店

```
長崎支店計
├─ 工事営業部計
└─ 硝子建材営業部計
```

**データソース**: #6 部門集計表 (department_summary)
- 工事営業部計 = 61「工事部」+ 63「業務部」× 案分比率
- 硝子建材営業部計 = 62「建材部」+ 63「業務部」× 案分比率
- 案分比率: #10_案分比率マスタ (ms_allocation_ratio) category='業務部門案分'

### 福岡支店

```
福岡支店計
├─ 工事部計
│   ├─ 硝子工事
│   ├─ ビルサッシ
│   └─ 内装工事
├─ 硝子樹脂計 (=硝子建材部門)
│   ├─ 硝子
│   ├─ 建材
│   └─ 樹脂
├─ GSセンター
└─ 福北センター
```

**データソース**: 長崎支店と同様の構造
- 工事部計 = 工事部 + 業務部 × 案分比率
- 硝子樹脂計 = 硝子樹脂部 + 業務部 × 案分比率
- 案分比率: #10_案分比率マスタ (ms_allocation_ratio) category='業務部門案分'

**注意事項**:
- DataMartでは「硝子樹脂計」として表示されるが、部門名は「硝子建材部門」
- GSセンター・福北センターは独立した集計単位

## 会計年度と累積計算

### 期首（会計年度開始日）

**期首日: 9月1日**

全ての累積計算（経常利益の累積本年実績・累積本年目標）は、期首から当月までの合算値として計算されます。

### 期首計算ロジック

```sql
-- 期首を月ごとに計算（期首は9/1）
fiscal_year_starts AS (
  SELECT DISTINCT
    year_month,
    CASE
      WHEN EXTRACT(MONTH FROM year_month) >= 9
      THEN DATE(EXTRACT(YEAR FROM year_month), 9, 1)
      ELSE DATE(EXTRACT(YEAR FROM year_month) - 1, 9, 1)
    END AS fiscal_start_date
  FROM org_categories_months
)
```

### 累積計算の対象

- **累積本年実績（経常利益）**: 期首（9/1）から当月までの経常利益実績の合計
- **累積本年目標（経常利益）**: 期首（9/1）から当月までの経常利益目標の合計

### 実装箇所

累積計算は以下のDataMart SQLファイルに実装されています:
- `sql/split_dwh_dm/datamart_management_report_tokyo.sql`
- `sql/split_dwh_dm/datamart_management_report_fukuoka.sql`
- `sql/split_dwh_dm/datamart_management_report_nagasaki.sql`

各ファイルの`cumulative_recurring_profit` CTEで、期首から当月までの累積計算を実施しています。

### 累積計算の例

| 対象月 | 期首 | 累積範囲 |
|--------|------|----------|
| 2024年9月 | 2024年9月1日 | 9月のみ |
| 2024年12月 | 2024年9月1日 | 9月〜12月 |
| 2025年2月 | 2024年9月1日 | 2024年9月〜2025年2月 |
| 2025年8月 | 2024年9月1日 | 2024年9月〜2025年8月 |
| 2025年9月 | 2025年9月1日 | 2025年9月のみ |

## スプレッドシート連携

### 概要

Google スプレッドシートから直接BigQueryにデータを連携する機能です。
既存のDrive連携（Excelファイル）とは**完全に独立**した処理として実装されています。

### アーキテクチャ

```
[スプレッドシート]
      │
      │ Sheets API で取得 → CSV変換
      ▼
[GCS: gs://data-platform-landing-prod/spreadsheet/]
      │
      ├── raw/{table_name}.csv              # 取得データ（CSV）
      └── config/
          ├── columns/                      # カラム変換用
          │   ├── {table_name}.csv
          │   └── ...
          └── mapping/                      # テーブル名定義用
              └── mapping_files.csv
      │
      │ bq load
      ▼
[BigQuery: corporate_data.ss_{table_name}]
```

### Drive連携との分離

| 項目 | Drive連携（既存） | スプレッドシート連携（新規） |
|------|------------------|---------------------------|
| **Cloud Runサービス** | `drive-to-gcs` | `spreadsheet-to-bq` |
| **ソースコード** | `run_service/main.py` | `spreadsheet_service/main.py` |
| **データソース** | Drive Folder ID | Sheets API（sheet_id指定） |
| **GCSパス** | `/raw/{yyyymm}/`, `/proceed/{yyyymm}/` | `/spreadsheet/raw/` |
| **設定ファイル** | `/config/mapping/mapping_files.csv` | `/spreadsheet/config/mapping/mapping_files.csv` |
| **BQテーブル** | `corporate_data.*` | `corporate_data.ss_*` |
| **トリガー** | Pub/Sub: `drive-monthly` | Cloud Scheduler: `spreadsheet-monthly` |

### 設定ファイル

#### マッピングファイル（シートID → テーブル名）

`gs://data-platform-landing-prod/spreadsheet/config/mapping/mapping_files.csv`

```csv
sheet_id,sheet_name,en_name
1ABC...XYZ,売上データ,sales_data
2DEF...UVW,費用データ,cost_data
```

| カラム | 説明 |
|--------|------|
| `sheet_id` | スプレッドシートのID |
| `sheet_name` | 取得対象のシート名（複数シートある場合） |
| `en_name` | BigQueryテーブル名（`ss_` プレフィックスは処理時に付与） |

#### カラムマッピング（日本語 → 英語）

`gs://data-platform-landing-prod/spreadsheet/config/columns/{table_name}.csv`

```csv
jp_name,en_name,data_type
年月,year_month,DATE
部門,department,STRING
売上金額,sales_amount,INTEGER
```

### 実行方法

#### 自動実行（月次）

Cloud Schedulerにより毎月自動実行されます。

#### 手動実行

```bash
# GCPコンソールから
# Cloud Run > spreadsheet-to-bq > 「実行」ボタン

# またはcurlで
curl -X POST "https://spreadsheet-to-bq-xxx.asia-northeast1.run.app/sync" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"
```

### デグレ防止設計

スプレッドシート連携は以下の点でDrive連携と完全に分離されており、相互に影響しません：

1. **サービス分離**: 別のCloud Run Service（`spreadsheet-to-bq`）
2. **コード分離**: 別ディレクトリ（`spreadsheet_service/`）
3. **GCSパス分離**: `/spreadsheet/` プレフィックスで完全分離
4. **テーブル分離**: `ss_` プレフィックスで既存テーブルと区別
5. **トリガー分離**: 別のCloud Scheduler Job

### ディレクトリ構成

```
tanacho-pipeline/
├── run_service/                    # 既存（Drive連携）
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── spreadsheet_service/            # 新規（スプシ連携）
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
└── gcs_to_bq_service/              # 既存（GCS→BQ）
    └── ...
```

## 開発環境

- Python 3.9+
- Google Cloud SDK
- BigQuery CLI (bq)
