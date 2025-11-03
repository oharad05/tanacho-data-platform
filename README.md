# タナチョウパイプライン

Google Drive → GCS → BigQuery → Looker Studio のデータパイプライン

## 概要

Google Drive上の月次データをGCSに取り込み、BigQueryに連携し、Looker Studioでダッシュボード表示するためのパイプラインです。

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

## 月次データ更新手順

### 1. Drive → GCS 同期

```bash
python sync_drive_to_gcs.py {YYYYMM}
```

例: `python sync_drive_to_gcs.py 202509`

### 2. raw → proceed 変換（Excel → CSV）

```bash
python transform_raw_to_proceed.py {YYYYMM}
```

### 3. proceed → BigQuery ロード

```bash
python load_to_bigquery.py {YYYYMM} --replace
```

### 4. マスターデータ更新（初回のみ必要）

```bash
bq query --use_legacy_sql=false < sql/update_ms_department_category_group_name.sql
```

### 5. DWHテーブル更新

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

### 6. データマート更新

```bash
cd sql/split_dwh_dm

# データマートテーブルを更新
{
  grep "^DECLARE" datamart_management_report_vertical.sql 2>/dev/null || echo ""
  echo ""
  echo "TRUNCATE TABLE \`data-platform-prod-475201.corporate_data_dm.management_documents_current_month_tbl\`;"
  echo ""
  echo "INSERT INTO \`data-platform-prod-475201.corporate_data_dm.management_documents_current_month_tbl\`"
  grep -v "^DECLARE" datamart_management_report_vertical.sql
} | bq query --use_legacy_sql=false
```

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

## 開発環境

- Python 3.9+
- Google Cloud SDK
- BigQuery CLI (bq)
