# 命令に対する基本姿勢
実装の方向性に迷ったり､仕様が不明な場合は､実装を止めて質問して下さい｡

# データの整合性の確認方法
sql/tmp_file/に正解データのpdfがあります｡
正解データのpdfと計算結果が一致しているか検証をします｡
検証する項目の粒度は､「main_category×secondary_category×secondary_department」です｡pdfに値が載っているもののみ検証してください｡

## 1.単月のデータ比較
### 手順1-1.pdfとcsvの比較
date(2025/09)などを指定します｡2025/09の場合､下記の{yymm}は2509と読み解いてください｡
1-1.main_department='東京支店'
pdf名: {yymm}損益東京.pdf の全ページ
計算結果: sql/tmp_file/SSでの可視化 - PL(月単位).csv の東京支店の範囲
1-2.main_department='長崎支店'
pdf名: {yymm}損益長崎.pdf の1ページ目
計算結果: sql/tmp_file/SSでの可視化 - PL(月単位).csv の長崎支店の範囲
main_category×secondary_category×secondary_department
1-3.main_department='福岡支店'
pdf名: {yymm}損益福岡.pdf の1ページ目
計算結果: sql/tmp_file/SSでの可視化 - PL(月単位).csv の福岡支店の範囲

### 手順1-2.比較結果の出力
確認結果を､csv出力できる形式で､docs/test/{支店名}/{yyyymm}/test_output.csvという名前で出力してください｡項目は下記です｡
main_category
secondary_category
secondary_department
pdf_val(pdfでの値)
csv_val(csvでの値)
diff_pdf_csv(pdf_val-csv_val)
is_equal(diff_pdf_csv=0なら1､そうでないならゼロ)
is_large_diff(diff_pdf_csvの絶対値が6以上なら1､そうでないなら0)
invest_result(一旦nullで定義)

### 手順1-3.調査結果・提案の入力
invest_resultに､調査した結果をテキストで入力してください｡

### 手順1-4.比較結果の報告
手順1-2の出力のうち､is_equal=0の項目を表示してください｡

# データ整合性確認後の修正プロセス
is_large_diff = 1の項目を対象に､ズレが起きている原因の調査及び解決策の提案を行ってください｡
調査・提案は order by main_department_sort_order,secondary_department_sort_order,main_category_sort_order,secondary_category_sort_orderの順に行ってください｡
調査に迷った場合は､sql/tmp_file/データソース一覧・加工内容 - #1_1経営資料（当月）_latest.csvが仕様書なので､そこを参考にしてください｡
必ず1項目毎に調査を止めてください｡対応方針を表示してください｡また､invest_resultに入力する内容を■invest_resultへの入力内容という項目で表示してください｡もし何らかの条件でpdfとcsvが完全一致もしくは誤差が少ない(±2程度)する場合は「◯◯の条件の場合､pdfとcsvの差分は△△となる」という記述を必ず入れてください｡
「SSでの可視化 - PL(月単位)_before.csv」のファイルは､データを再取り込みする前の結果です｡この結果がpdfと一致している可能性があります｡一致している場合は報告をし､その場合はデータ自体の問題の可能性が高いので､その方針で調査してください｡
before時に連携して使ったデータはcorporate_date_bkに存在するので､必要に応じて参照してください｡
原因はmain_category×secondary_category毎に同一の可能性があるので､調査する際はそれを参考にしてください｡
「OKです｡invest_resultを入力して次に進んでください」と書いたら､■invest_resultへの入力内容の内容を入力し次に進んで問題ないです｡

# tanacho-pipeline プロジェクト設定

## データロード方針
Bigqueryでは必ずpartition列を設定する｡

### 1. データ洗いがえ
#### 1-1.全データ洗いがえ(何も指定しない場合はこちらで実行｡replaceを行うイメージ)
driveとスプレッドシートから､Bigqueryのcorporate_dataのデータソースをreplaceする手順を記載します｡
- **冪等性保証**: 何度実行しても同じ結果になる が担保されているかは常に意識してください｡
1.driveとスプレッドシートから､gcsのraw/にデータをロード
2｡gcsのraw/からproceed/にデータ変換しつつロード
3.corporate_dataのテーブルを､corporate_data_bkにコピーし､バックアップを作成
4.corporate_dataのレコードをすべて削除
5.gcsからBigqueryにデータをロード（※テーブルタイプ別処理あり、下記参照）
6.重複が発生していないことを調査
7.ロードしたcorporate_data配下のテーブルとcorporate_data_bkのテーブルを比較し､差分を調査
8.実行完了後､下記を表示
①drive・スプレッドシート→gcsのロード成功数・失敗数・失敗したファイル名(orシート名)
②gcsのraw/→proceed/のロード成功数・失敗数・失敗したファイル名(orシート名)
③Bigqueryのロード成功数・失敗数・失敗したテーブル名
④6と7の調査結果を表示

##### 5. テーブルタイプ別ロード処理

テーブルは「単月型」と「累積型」の2種類があり、ロード処理が異なる。

**単月型テーブル**: 各CSVがその月のデータのみ含む → 全CSVをそのままロード

**累積型テーブル**: 各CSVが全期間のデータを含む → 全CSVを結合してキー毎に最新フォルダ（max(source_folder)）を優先

| ソースファイル名 | テーブル名 | タイプ | ユニークキー |
|-----------------|-----------|--------|-------------|
| 3_請求残高一覧表（月間）.xlsx | billing_balance | 累積型 | sales_month, branch_code, branch_name |
| 12_損益5期目標.xlsx（東京支店目標103期） | profit_plan_term | 累積型 | period, item |
| 12_損益5期目標.xlsx（長崎支店目標103期） | profit_plan_term_nagasaki | 累積型 | period, item |
| 12_損益5期目標.xlsx（福岡支店目標103期） | profit_plan_term_fukuoka | 累積型 | period, item |
| 10_案分比率マスタ.xlsx | ms_allocation_ratio | 累積型 | year_month, branch, department, category |
| 1_全支店[1.売上管理] 担当者売上目標／実績データ.xlsx | sales_target_and_achievements | 単月型 | - |
| 4_元帳_雑収入.xlsx | ledger_income | 単月型 | - |
| 6_部門集計表_YYYYMM.xlsx | department_summary | 単月型 | - |
| 7_社内金利計算表.xlsx | internal_interest | 単月型 | - |
| 9_在庫.xlsx | stocks | 単月型 | - |
| 16_元帳_雑損失.xlsx | ledger_loss | 単月型 | - |
| 工事進捗日数金額.xlsx | construction_progress_days_amount | 累積型 | property_period, branch_code, staff_code, property_number, customer_code, contract_date |
| 工事進捗日数最終日.xlsx | construction_progress_days_final_date | 累積型（※未対応） | final_billing_sales_date, property_number, property_data_classification |

累積型テーブルには `source_folder` カラム（INTEGER型、例: 202511）が追加され、どのフォルダからロードされたかを追跡可能

**※注意: 重複データを含むテーブル**
以下のテーブルは累積型だが、現在の`load_to_bigquery.py`では重複排除処理が未実装のため、重複データを含む状態でロードされる：
- `construction_progress_days_final_date`: 約2.5倍の重複（51,424行 / ユニークキー20,732件）

このテーブルを使用する際は、クエリ側でDISTINCTまたはユニークキーによる重複排除が必要。
#### 1-2.指定年月のみ洗いがえ
パラメータとしてyyyymm(ex.202509)を渡し､driveは該当のyyyymmのみ更新する｡スプレッドシートは1-1と同様に実装する

参考コマンド
```bash
python scripts/manual/load_to_bigquery.py
```

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
