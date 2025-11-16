/*
============================================================
中間テーブル: aggregated_metrics (東京支店) + 前年営業経費・営業利益
============================================================
目的: DataMartの複雑さを軽減するため、aggregated_metricsに
     営業経費と営業利益の前年データを事前に結合
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.aggregated_metrics_tokyo_with_prev` AS
WITH base_metrics AS (
  -- 既存のdatamart_management_report_tokyo.sqlのaggregated_metricsと同じロジック
  -- (省略: 実際には既存のCTEをここにコピー)
  SELECT 
    year_month,
    organization,
    detail_category,
    sales_actual,
    sales_target,
    sales_prev_year,
    gross_profit_actual,
    gross_profit_target,
    gross_profit_prev_year,
    operating_expense_actual,
    operating_expense_target,
    operating_income_actual,
    operating_income_target
  FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period`
  WHERE main_department = '東京支店'
    AND main_category = '売上高'  -- aggregated_metricsの代わりに既存テーブルから取得
)

SELECT
  bm.*,
  oe_prev.operating_expense_amount AS operating_expense_prev_year,
  bm.gross_profit_prev_year - COALESCE(oe_prev.operating_expense_amount, 0) AS operating_income_prev_year
FROM base_metrics bm
LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe_prev
  ON DATE_ADD(oe_prev.year_month, INTERVAL 1 YEAR) = bm.year_month
  AND oe_prev.detail_category = bm.detail_category
  AND oe_prev.branch = '東京支店';
