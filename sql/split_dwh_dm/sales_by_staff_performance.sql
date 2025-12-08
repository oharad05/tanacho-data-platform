/*
============================================================
DataMart: 担当者別_1.業績
============================================================
目的: 担当者別の業績サマリー

データソース:
  - sales_target_and_achievements

表示項目:
  - 売上高（計画/実績/計画比・差）
  - 粗利額（計画/実績/計画比・差）
  - 粗利率（計画/実績/計画差）

集計単位:
  - 支店 × 部門 × 担当者 × 年月

累計計算:
  - 期首月（9月）からの累計を計算
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.sales_by_staff_performance`
PARTITION BY sales_accounting_period
CLUSTER BY main_department, staff_code
AS

WITH base_data AS (
  SELECT
    sales_accounting_period,
    -- 支店判定（営業所単位）
    -- 東京支店：025 硝子建材営業部
    -- 福岡支店：031 福岡硝子建材営業
    -- 長崎支店：065 長崎硝子建材営１ + 066 長崎硝子建材営２
    CASE
      WHEN CAST(department_code AS INT64) = 25 THEN '東京支店'
      WHEN CAST(department_code AS INT64) IN (65, 66) THEN '長崎支店'
      WHEN CAST(department_code AS INT64) = 31 THEN '福岡支店'
      ELSE 'その他'
    END AS main_department,
    department_code,
    department_name,
    staff_code,
    staff_name,
    sales_target,
    sales_actual,
    gross_profit_target,
    gross_profit_actual,
    prev_year_sales_actual,
    prev_year_gross_profit_actual
  FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
),

-- 担当者別に集計
staff_summary AS (
  SELECT
    sales_accounting_period,
    main_department,
    department_code,
    department_name,
    staff_code,
    staff_name,
    SUM(sales_target) AS monthly_sales_target,
    SUM(sales_actual) AS monthly_sales_actual,
    SUM(gross_profit_target) AS monthly_gross_profit_target,
    SUM(gross_profit_actual) AS monthly_gross_profit_actual,
    SUM(prev_year_sales_actual) AS monthly_prev_year_sales,
    SUM(prev_year_gross_profit_actual) AS monthly_prev_year_gross_profit
  FROM base_data
  WHERE main_department != 'その他'
  GROUP BY sales_accounting_period, main_department, department_code, department_name, staff_code, staff_name
),

-- 期首月を計算して累計用の情報を付与
fiscal_year_info AS (
  SELECT
    *,
    -- 期首月計算（9月が期首）
    DATE_SUB(
      DATE_TRUNC(
        DATE_ADD(sales_accounting_period, INTERVAL 4 MONTH),
        YEAR
      ),
      INTERVAL 4 MONTH
    ) AS fiscal_start_month
  FROM staff_summary
),

-- 累計計算
cumulative_data AS (
  SELECT
    f.sales_accounting_period,
    f.main_department,
    f.department_code,
    f.department_name,
    f.staff_code,
    f.staff_name,
    f.monthly_sales_target,
    f.monthly_sales_actual,
    f.monthly_gross_profit_target,
    f.monthly_gross_profit_actual,
    f.monthly_prev_year_sales,
    f.monthly_prev_year_gross_profit,
    f.fiscal_start_month,
    -- 累計値
    SUM(s.monthly_sales_target) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_target,
    SUM(s.monthly_sales_actual) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_actual,
    SUM(s.monthly_gross_profit_target) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gross_profit_target,
    SUM(s.monthly_gross_profit_actual) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gross_profit_actual,
    SUM(s.monthly_prev_year_sales) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_sales,
    SUM(s.monthly_prev_year_gross_profit) OVER (
      PARTITION BY f.main_department, f.staff_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_gross_profit
  FROM fiscal_year_info f
  JOIN staff_summary s
    ON f.sales_accounting_period = s.sales_accounting_period
    AND f.main_department = s.main_department
    AND f.staff_code = s.staff_code
)

SELECT
  sales_accounting_period,
  main_department,
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  CAST(department_code AS INT64) AS department_code,
  department_name,
  CAST(staff_code AS INT64) AS staff_code,
  staff_name,

  -- 当月データ（千円単位）
  ROUND(monthly_sales_target / 1000, 0) AS monthly_sales_target,
  ROUND(monthly_sales_actual / 1000, 0) AS monthly_sales_actual,
  ROUND(monthly_gross_profit_target / 1000, 0) AS monthly_gross_profit_target,
  ROUND(monthly_gross_profit_actual / 1000, 0) AS monthly_gross_profit_actual,
  ROUND(monthly_prev_year_sales / 1000, 0) AS monthly_prev_year_sales,
  ROUND(monthly_prev_year_gross_profit / 1000, 0) AS monthly_prev_year_gross_profit,

  -- 当月売上比率
  CASE WHEN monthly_sales_target > 0
    THEN ROUND(monthly_sales_actual / monthly_sales_target * 100, 0)
    ELSE NULL
  END AS monthly_sales_target_ratio,
  -- 当月売上差
  ROUND((monthly_sales_actual - monthly_sales_target) / 1000, 0) AS monthly_sales_target_diff,
  -- 当月売上前年比
  CASE WHEN monthly_prev_year_sales > 0
    THEN ROUND(monthly_sales_actual / monthly_prev_year_sales * 100, 0)
    ELSE NULL
  END AS monthly_sales_prev_year_ratio,

  -- 当月粗利比率
  CASE WHEN monthly_gross_profit_target > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_gross_profit_target * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_target_ratio,
  -- 当月粗利差
  ROUND((monthly_gross_profit_actual - monthly_gross_profit_target) / 1000, 0) AS monthly_gross_profit_target_diff,
  -- 当月粗利前年比
  CASE WHEN monthly_prev_year_gross_profit > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_prev_year_ratio,

  -- 当月粗利率（計画）
  CASE WHEN monthly_sales_target > 0
    THEN ROUND(monthly_gross_profit_target / monthly_sales_target * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate_target,
  -- 当月粗利率（実績）
  CASE WHEN monthly_sales_actual > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate_actual,
  -- 当月粗利率差（実績 - 計画）
  CASE WHEN monthly_sales_actual > 0 AND monthly_sales_target > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 0)
       - ROUND(monthly_gross_profit_target / monthly_sales_target * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate_diff,

  -- 累計データ（千円単位）
  ROUND(cumulative_sales_target / 1000, 0) AS cumulative_sales_target,
  ROUND(cumulative_sales_actual / 1000, 0) AS cumulative_sales_actual,
  ROUND(cumulative_gross_profit_target / 1000, 0) AS cumulative_gross_profit_target,
  ROUND(cumulative_gross_profit_actual / 1000, 0) AS cumulative_gross_profit_actual,
  ROUND(cumulative_prev_year_sales / 1000, 0) AS cumulative_prev_year_sales,
  ROUND(cumulative_prev_year_gross_profit / 1000, 0) AS cumulative_prev_year_gross_profit,

  -- 累計売上比率
  CASE WHEN cumulative_sales_target > 0
    THEN ROUND(cumulative_sales_actual / cumulative_sales_target * 100, 0)
    ELSE NULL
  END AS cumulative_sales_target_ratio,
  -- 累計売上差
  ROUND((cumulative_sales_actual - cumulative_sales_target) / 1000, 0) AS cumulative_sales_target_diff,
  -- 累計売上前年比
  CASE WHEN cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_sales_actual / cumulative_prev_year_sales * 100, 0)
    ELSE NULL
  END AS cumulative_sales_prev_year_ratio,

  -- 累計粗利比率
  CASE WHEN cumulative_gross_profit_target > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_gross_profit_target * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_target_ratio,
  -- 累計粗利差
  ROUND((cumulative_gross_profit_actual - cumulative_gross_profit_target) / 1000, 0) AS cumulative_gross_profit_target_diff,
  -- 累計粗利前年比
  CASE WHEN cumulative_prev_year_gross_profit > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_prev_year_ratio,

  -- 累計粗利率（計画）
  CASE WHEN cumulative_sales_target > 0
    THEN ROUND(cumulative_gross_profit_target / cumulative_sales_target * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate_target,
  -- 累計粗利率（実績）
  CASE WHEN cumulative_sales_actual > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate_actual,
  -- 累計粗利率差（実績 - 計画）
  CASE WHEN cumulative_sales_actual > 0 AND cumulative_sales_target > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 0)
       - ROUND(cumulative_gross_profit_target / cumulative_sales_target * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate_diff

FROM cumulative_data;
