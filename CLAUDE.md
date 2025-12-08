# 命令に対する基本姿勢
実装の方向性に迷ったり､仕様が不明な場合は､実装を止めて質問して下さい｡

# tanacho-pipeline プロジェクト設定

## データロード方針
Bigqueryでは必ずpartition列を設定する｡

### 1. 全データ洗い替え（デフォルト動作）
```bash
python scripts/manual/load_to_bigquery.py
```
- 2024年9月（期首）以降のデータを全て削除
- GCSの`proceed/`フォルダにある全年月のCSVを再ロード
- **冪等性保証**: 何度実行しても同じ結果になる

### 2. 月を指定してInsert
```bash
python scripts/manual/load_to_bigquery.py YYYYMM
```
- 指定月のデータのみ削除して再ロード（追加モード）
- パーティション列を使用して対象データを特定

---

## BigQueryテーブル・パーティション列一覧

### corporate_data（生データ層）

| テーブル名 | パーティション列 | 型 | クラスタリング列 | 状態 |
|-----------|-----------------|------|-----------------|------|
| sales_target_and_achievements | sales_accounting_period | DATE | branch_code | 設定済 |
| billing_balance | sales_month | DATE | branch_code | 設定済 |
| ledger_income | slip_date | DATETIME | classification_type | 設定済 |
| ledger_loss | accounting_month | DATETIME | classification_type | 設定済 |
| department_summary | sales_accounting_period | DATE | code | 設定済 |
| internal_interest | year_month | DATE | branch | 設定済 |
| profit_plan_term | period | DATE | item | 設定済 |
| profit_plan_term_nagasaki | period | DATE | item | 設定済 |
| profit_plan_term_fukuoka | period | DATE | item | 設定済 |
| stocks | year_month | DATE | branch | 設定済 |
| ms_allocation_ratio | year_month | DATE | branch | 設定済 |
| customer_sales_target_and_achievements | sales_accounting_period | DATE | branch_code, customer_code | 設定済 |
| construction_progress_days_amount | property_period | DATE | branch_code | 設定済 |
| construction_progress_days_final_date | final_billing_sales_date | DATE | - | 設定済 |

#### スプレッドシート連携テーブル（パーティションなし）
| テーブル名 | 説明 |
|-----------|------|
| ss_gs_sales_profit | GS売上利益 |
| ss_inventory_advance_tokyo | 東京在庫前払 |
| ss_inventory_advance_nagasaki | 長崎在庫前払 |
| ss_inventory_advance_fukuoka | 福岡在庫前払 |
| management_materials_current_month | 経営資料（当月） |

### corporate_data_dwh（DWH層）

| テーブル名 | パーティション列候補 | 備考 |
|-----------|-------------------|------|
| dwh_sales_actual | year_month | SQLで生成（CREATE OR REPLACE） |
| dwh_sales_actual_prev_year | year_month | SQLで生成（CREATE OR REPLACE） |
| dwh_sales_target | year_month | SQLで生成（CREATE OR REPLACE） |
| dwh_recurring_profit_target | year_month | SQLで生成（CREATE OR REPLACE） |
| operating_expenses | year_month | SQLで生成（CREATE OR REPLACE） |
| operating_expenses_target | year_month | SQLで生成（CREATE OR REPLACE） |
| operating_income_target | year_month | SQLで生成（CREATE OR REPLACE） |
| non_operating_income | year_month | SQLで生成（CREATE OR REPLACE） |
| non_operating_expenses | year_month | SQLで生成（CREATE OR REPLACE） |
| non_operating_expenses_nagasaki | year_month | SQLで生成（CREATE OR REPLACE） |
| non_operating_expenses_fukuoka | year_month | SQLで生成（CREATE OR REPLACE） |
| miscellaneous_loss | year_month | SQLで生成（CREATE OR REPLACE） |
| head_office_expenses | year_month | SQLで生成（CREATE OR REPLACE） |
| aggregated_metrics_all_branches | - | 集計テーブル |

### corporate_data_dm（DataMart層）

| テーブル名 | パーティション列候補 | 備考 |
|-----------|-------------------|------|
| management_documents_all_period_tokyo | date | SQLで生成（全洗い替え） |
| management_documents_all_period_nagasaki | date | SQLで生成（全洗い替え） |
| management_documents_all_period_fukuoka | date | SQLで生成（全洗い替え） |
| management_documents_all_period_all | date | SQLで生成（全洗い替え） |
| management_documents_all_period_all_for_display | date | SQLで生成（全洗い替え） |
| cumulative_management_documents_all_period_all | date | SQLで生成（全洗い替え） |
| cumulative_management_documents_all_period_all_for_display | date | SQLで生成（全洗い替え） |

---

## load_to_bigquery.py TABLE_CONFIG

```python
TABLE_CONFIG = {
    "sales_target_and_achievements": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["branch_code"]
    },
    "billing_balance": {
        "partition_field": "sales_month",
        "clustering_fields": ["branch_code"]
    },
    "ledger_income": {
        "partition_field": "slip_date",
        "clustering_fields": ["classification_type"]
    },
    "department_summary": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["code"]
    },
    "internal_interest": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "profit_plan_term": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "profit_plan_term_nagasaki": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "profit_plan_term_fukuoka": {
        "partition_field": "period",
        "clustering_fields": ["item"]
    },
    "ledger_loss": {
        "partition_field": "accounting_month",
        "clustering_fields": ["classification_type"]
    },
    "stocks": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "ms_allocation_ratio": {
        "partition_field": "year_month",
        "clustering_fields": ["branch"]
    },
    "customer_sales_target_and_achievements": {
        "partition_field": "sales_accounting_period",
        "clustering_fields": ["branch_code"]
    },
    "construction_progress_days_amount": {
        "partition_field": "property_period",
        "clustering_fields": ["branch_code"]
    },
    "construction_progress_days_final_date": {
        "partition_field": "final_billing_sales_date",
        "clustering_fields": []
    },
}
```

---

## データフロー

```
GCS (proceed/YYYYMM/*.csv)
    ↓ load_to_bigquery.py
BigQuery corporate_data（生データ）
    ↓ sql/split_dwh_dm/*.sql
BigQuery corporate_data_dwh（DWH）
    ↓ sql/split_dwh_dm/*.sql
BigQuery corporate_data_dm（DataMart）
```

---

## 実行スクリプト

### 全データ洗い替え
```bash
# 1. GCSからBigQueryへロード（corporate_data）
python scripts/manual/load_to_bigquery.py

# 2. DWH・DataMart再構築
bash sql/scripts/update_dwh.sh
bash sql/scripts/update_datamart.sh
```

### 月指定ロード
```bash
# 特定月のみロード
python scripts/manual/load_to_bigquery.py 202509

# DWH・DataMart再構築
bash sql/scripts/update_dwh.sh
bash sql/scripts/update_datamart.sh
```

---

## パーティション設定履歴

### 2024-12-09 パーティション設定完了
以下のテーブルにBigQueryネイティブパーティションを設定：

| テーブル | パーティション列 | クラスタリング列 |
|---------|-----------------|-----------------|
| sales_target_and_achievements | sales_accounting_period | branch_code |
| ledger_loss | DATE(accounting_month) | classification_type |
| department_summary | sales_accounting_period | code |
| stocks | year_month | branch |
| ms_allocation_ratio | year_month | branch |
| construction_progress_days_amount | property_period | branch_code |
| construction_progress_days_final_date | final_billing_sales_date | - |

**効果:**
- クエリコスト削減（パーティションプルーニング）
- パフォーマンス向上
- 月指定Insertの高速化
