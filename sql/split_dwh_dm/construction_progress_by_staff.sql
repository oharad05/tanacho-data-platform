/*
============================================================
DataMart: 担当者別_5.工事物件進捗管理表
============================================================
目的: 担当者別の工事物件進捗状況を表示

データソース:
  - construction_progress_days_amount
  - construction_progress_days_final_date

表示項目:
  - 物件No、物件名、得意先CD、得意先名
  - 契約日、契約金額
  - 実行予算、予定粗利、予定粗利率（画面）
  - 仕入実績、実績粗利、実績粗利率
  - 予算差異、予算差異率（実績-予算）
  - 原価進捗率
  - 仕掛金売上金額、出来高比率（売上+約）
  - 売上原価、粗利額、粗利率
  - 売工日（売上/仕入完了）、完了日、最終仕入日、最終経過日数

集計単位:
  - 支店 × 担当者 × 物件
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.construction_progress_by_staff`
PARTITION BY property_period
CLUSTER BY main_department, staff_code
AS

WITH base_data AS (
  SELECT
    a.property_period,
    -- 支店判定（営業所単位）
    -- 東京支店：025 硝子建材営業部
    -- 福岡支店：031 福岡硝子建材営業
    -- 長崎支店：065 長崎硝子建材営１ + 066 長崎硝子建材営２
    CASE
      WHEN CAST(a.branch_code AS INT64) = 25 THEN '東京支店'
      WHEN CAST(a.branch_code AS INT64) IN (65, 66) THEN '長崎支店'
      WHEN CAST(a.branch_code AS INT64) = 31 THEN '福岡支店'
      ELSE 'その他'
    END AS main_department,
    CAST(a.branch_code AS INT64) AS branch_code,
    a.branch_name,
    CAST(a.staff_code AS INT64) AS staff_code,
    a.staff_name,

    -- 物件情報
    a.property_number,
    a.property_name_1,
    a.property_name_2,
    a.customer_code,
    a.customer_name,
    a.contract_date,

    -- 契約・予算情報（千円単位）
    ROUND(CAST(a.contract_amount AS FLOAT64) / 1000, 0) AS contract_amount,
    ROUND(CAST(a.execution_budget AS FLOAT64) / 1000, 0) AS execution_budget,
    ROUND(CAST(a.planned_gross_profit_amount AS FLOAT64) / 1000, 0) AS planned_gross_profit,
    CAST(a.planned_gross_profit_rate_display AS FLOAT64) AS planned_gross_profit_rate,

    -- 仕入・実績情報（千円単位）
    ROUND(CAST(a.purchase_actual AS FLOAT64) / 1000, 0) AS purchase_actual,
    ROUND(CAST(a.actual_gross_profit_amount AS FLOAT64) / 1000, 0) AS actual_gross_profit,
    CAST(a.actual_gross_profit_rate AS FLOAT64) AS actual_gross_profit_rate,

    -- 予算差異（千円単位）
    ROUND(CAST(a.budget_variance AS FLOAT64) / 1000, 0) AS budget_variance,
    -- 予算差異率（実績-予算）
    CASE
      WHEN a.execution_budget > 0
      THEN ROUND((CAST(a.budget_variance AS FLOAT64) / CAST(a.execution_budget AS FLOAT64)) * 100, 1)
      ELSE NULL
    END AS budget_variance_rate,

    -- 原価進捗率
    CAST(a.cost_progress_rate AS FLOAT64) AS cost_progress_rate,

    -- 仕掛金・売上情報（千円単位）
    ROUND(CAST(a.work_in_progress_amount AS FLOAT64) / 1000, 0) AS work_in_progress_amount,
    ROUND(CAST(a.sales_amount AS FLOAT64) / 1000, 0) AS sales_amount,
    CAST(a.completion_ratio_sales AS FLOAT64) AS completion_ratio_sales,

    -- 売上原価・粗利（千円単位）
    ROUND(CAST(a.cost_of_sales AS FLOAT64) / 1000, 0) AS cost_of_sales,
    ROUND(CAST(a.gross_profit_amount AS FLOAT64) / 1000, 0) AS gross_profit_amount,
    CAST(a.gross_profit_rate AS FLOAT64) AS gross_profit_rate,

    -- 売工日（売上/仕入完了）= completion_date
    a.completion_date AS sales_completion_date,

    -- 完了日
    a.planned_completion_date,

    -- 物件完了フラグ
    a.property_completion_flag,

    -- 最終請求売上日（final_dateテーブルから取得）
    f.final_billing_sales_date,

    -- 最終経過日数（最終請求売上日から当月末までの日数）
    CASE
      WHEN f.final_billing_sales_date IS NOT NULL
      THEN DATE_DIFF(
        DATE_TRUNC(DATE_ADD(a.property_period, INTERVAL 1 MONTH), MONTH),
        f.final_billing_sales_date,
        DAY
      )
      ELSE NULL
    END AS days_since_final_billing

  FROM `data-platform-prod-475201.corporate_data.construction_progress_days_amount` a
  LEFT JOIN (
    -- 物件ごとに最新の最終請求売上日を取得
    SELECT
      property_number,
      MAX(final_billing_sales_date) AS final_billing_sales_date
    FROM `data-platform-prod-475201.corporate_data.construction_progress_days_final_date`
    GROUP BY property_number
  ) f
    ON a.property_number = f.property_number
)

SELECT
  property_period,
  main_department,
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  branch_code,
  branch_name,
  staff_code,
  staff_name,

  -- 物件情報
  property_number,
  property_name_1,
  property_name_2,
  customer_code,
  customer_name,
  contract_date,

  -- 契約・予算情報
  contract_amount,
  execution_budget,
  planned_gross_profit,
  planned_gross_profit_rate,

  -- 仕入・実績情報
  purchase_actual,
  actual_gross_profit,
  actual_gross_profit_rate,

  -- 予算差異
  budget_variance,
  budget_variance_rate,

  -- 原価進捗率
  cost_progress_rate,

  -- 仕掛金・売上情報
  work_in_progress_amount,
  sales_amount,
  completion_ratio_sales,

  -- 売上原価・粗利
  cost_of_sales,
  gross_profit_amount,
  gross_profit_rate,

  -- 日付情報
  sales_completion_date,
  planned_completion_date,
  final_billing_sales_date,
  days_since_final_billing,

  -- 完了フラグ
  property_completion_flag

FROM base_data
WHERE main_department != 'その他';
