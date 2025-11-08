# 長崎支店 DataMart実装状況

## ✅ 完了した実装

### 1. DWHテーブル (corporate_data_dwh)
- ✅ `dwh_sales_actual_nagasaki` - 本年実績(売上・粗利)
- ✅ `dwh_sales_actual_prev_year_nagasaki` - 前年実績(売上・粗利)
- ✅ `dwh_sales_target_nagasaki` - 本年目標(売上・粗利)
- ✅ `operating_expenses_nagasaki` - 営業経費(業務部案分込み)
- ✅ `non_operating_income_nagasaki` - 営業外収入(業務部案分込み)
- ✅ `miscellaneous_loss_nagasaki` - 雑損失(業務部案分込み)

### 2. DataMartテーブル (corporate_data_dm)
- ✅ `management_documents_all_period_nagasaki` - 長崎支店DataMart
- ✅ `management_documents_all_period_all` - 東京+長崎統合DataMart

### 3. データ検証
- ✅ 長崎支店データのみ含まれる(東京データ混在なし)
- ✅ 部門別カテゴリ正しく表示
- ✅ 組織階層: 長崎支店計 → 工事営業部計/硝子建材営業部計 → 部門別

---

## ❌ 未実装の項目

### 1. 本店管理費 (head_office_expenses_nagasaki)
**必要なファイル**: #5 部門集計表_202509.xlsx (長崎支店版)
**ステータス**: ファイルが存在しないため未実装
**対応**: ファイル提供を依頼してください

### 2. 社内利息 (non_operating_expenses_nagasaki)
**データソース**: #7 社内金利計算表.xlsx + #9 在庫.xlsx
**ステータス**: ロジックが複雑なため未実装
**対応**: 必要に応じて後日実装

### 3. 目標系テーブル3つ (#12 損益5期目標) ✅ 完了
**データソース**: #12 損益5期目標.xlsx の「長崎支店目標103期」シート
**ステータス**: テーブル化完了、DataMart統合完了

**作成したテーブル**:
- ✅ `profit_plan_term_nagasaki` - 損益5期目標(長崎)元テーブル (corporate_data)
- ✅ `dwh_recurring_profit_target_nagasaki` - 経常利益目標 (corporate_data_dwh)
- ✅ `operating_expenses_target_nagasaki` - 営業経費目標 (corporate_data_dwh)
- ✅ `operating_income_target_nagasaki` - 営業利益目標 (corporate_data_dwh)

**実装内容**:
1. ✅ `config/columns/profit_plan_term_nagasaki.csv` 作成
2. ✅ `config/mapping/mapping_files.csv` に追加
3. ✅ ExcelからCSVへ変換、BigQueryにロード
4. ✅ DWH SQLファイル作成:
   - `sql/split_dwh_dm/dwh_recurring_profit_target_nagasaki.sql`
   - `sql/split_dwh_dm/operating_expenses_target_nagasaki.sql`
   - `sql/split_dwh_dm/operating_income_target_nagasaki.sql`
5. ✅ DataMart SQLに統合 (datamart_management_report_nagasaki.sql)

**検証結果**: 2025-09-01のデータで営業経費、営業利益、経常利益の目標値が正しく表示されることを確認

---

## 📝 実装済みテーブルの詳細

### データソースとロジック

| テーブル名 | データソース | 主なロジック |
|---|---|---|
| dwh_sales_actual_nagasaki | #1 sales_target_and_achievements | branch_code IN (061, 065, 066) |
| dwh_sales_actual_prev_year_nagasaki | #1 sales_target_and_achievements | 前年実績カラム使用 |
| dwh_sales_target_nagasaki | #1 sales_target_and_achievements | 目標カラム使用 |
| operating_expenses_nagasaki | #6 department_summary + #10 ms_allocation_ratio | 業務部(63)を案分比率で配分 |
| non_operating_income_nagasaki | #4 ledger_income + #10 ms_allocation_ratio | 業務部(63)を案分比率で配分 |
| miscellaneous_loss_nagasaki | #16 ledger_loss + #10 ms_allocation_ratio | 業務部(63)を案分比率で配分 |

### 営業所コードと部門マッピング

**営業所コード**:
- 061 = 工事営業部
- 065, 066 = 硝子建材営業部

**部門コード** (department_summaryテーブル):
- 61 (construction_department) = 工事営業部
- 62 (glass_building_material_sales_department) = 硝子建材営業部
- 63 (operations_department) = 業務部(案分対象)

**division_code マッピング**:
- 工事営業部(061):
  - 11 → ガラス工事
  - 21 → ビルサッシ
- 硝子建材営業部(065/066):
  - 11 → 硝子工事
  - 10 → 硝子販売
  - 20 → サッシ工事/サッシ販売
  - 22他 → 完成品(その他)
