/*
============================================================
DataMart: 総括表_4.主力取引先別実績
============================================================
目的: 粗利金額上位20社の取引先別実績

データソース:
  - customer_sales_target_and_achievements

表示項目:
  - 得意先コード・得意先名
  - 売上高（実績/計画比/前年比）
  - 粗利額（実績/計画比/前年比）
  - 粗利率（実績/計画差/前年差）

ランキング:
  - 支店×年月ごとに粗利金額上位20社を抽出
  - 20社計も算出
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.sales_summary_top_customers`
PARTITION BY sales_accounting_period
CLUSTER BY main_department
AS

WITH base_data AS (
  SELECT
    sales_accounting_period,
    -- 支店判定（営業所単位）
    -- 東京支店：025 硝子建材営業部
    -- 福岡支店：031 福岡硝子建材営業
    -- 長崎支店：065 長崎硝子建材営１ + 066 長崎硝子建材営２
    CASE
      WHEN CAST(branch_code AS INT64) = 25 THEN '東京支店'
      WHEN CAST(branch_code AS INT64) IN (65, 66) THEN '長崎支店'
      WHEN CAST(branch_code AS INT64) = 31 THEN '福岡支店'
      ELSE 'その他'
    END AS main_department,
    customer_code,
    customer_name,
    sales_actual,
    sales_target,
    gross_profit_actual,
    gross_profit_target,
    prev_year_sales_actual,
    prev_year_gross_profit_actual
  FROM `data-platform-prod-475201.corporate_data.customer_sales_target_and_achievements`
),

-- 顧客別に集計（同一顧客が複数部門にまたがる場合を考慮）
customer_summary AS (
  SELECT
    sales_accounting_period,
    main_department,
    customer_code,
    customer_name,
    SUM(sales_actual) AS monthly_sales_actual,
    SUM(sales_target) AS monthly_sales_target,
    SUM(gross_profit_actual) AS monthly_gross_profit_actual,
    SUM(gross_profit_target) AS monthly_gross_profit_target,
    SUM(prev_year_sales_actual) AS monthly_prev_year_sales,
    SUM(prev_year_gross_profit_actual) AS monthly_prev_year_gross_profit
  FROM base_data
  WHERE main_department != 'その他'
  GROUP BY sales_accounting_period, main_department, customer_code, customer_name
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
  FROM customer_summary
),

-- 累計計算
cumulative_data AS (
  SELECT
    f.sales_accounting_period,
    f.main_department,
    f.customer_code,
    f.customer_name,
    f.monthly_sales_actual,
    f.monthly_sales_target,
    f.monthly_gross_profit_actual,
    f.monthly_gross_profit_target,
    f.monthly_prev_year_sales,
    f.monthly_prev_year_gross_profit,
    f.fiscal_start_month,
    -- 累計値
    SUM(c.monthly_sales_actual) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_actual,
    SUM(c.monthly_sales_target) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_target,
    SUM(c.monthly_gross_profit_actual) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gross_profit_actual,
    SUM(c.monthly_gross_profit_target) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gross_profit_target,
    SUM(c.monthly_prev_year_sales) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_sales,
    SUM(c.monthly_prev_year_gross_profit) OVER (
      PARTITION BY f.main_department, f.customer_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_gross_profit
  FROM fiscal_year_info f
  JOIN customer_summary c
    ON f.sales_accounting_period = c.sales_accounting_period
    AND f.main_department = c.main_department
    AND f.customer_code = c.customer_code
),

-- 粗利上位20社を抽出（累計粗利でランキング）
ranked_customers AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY sales_accounting_period, main_department
      ORDER BY cumulative_gross_profit_actual DESC
    ) AS ranking
  FROM cumulative_data
),

-- 上位20社のみ抽出
top20_customers AS (
  SELECT * FROM ranked_customers WHERE ranking <= 20
)

-- 個別顧客データ
SELECT
  sales_accounting_period,
  main_department,
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  ranking,
  customer_code,
  customer_name,
  CONCAT(customer_code, ' ', customer_name) AS customer_display_name,
  -- 事業形態（将来的にattribute_code_1からマッピング予定）
  CAST(NULL AS STRING) AS business_type,

  -- 当月データ（千円単位）
  ROUND(monthly_sales_actual / 1000, 0) AS monthly_sales_actual,
  ROUND(monthly_sales_target / 1000, 0) AS monthly_sales_target,
  ROUND(monthly_gross_profit_actual / 1000, 0) AS monthly_gross_profit_actual,
  ROUND(monthly_gross_profit_target / 1000, 0) AS monthly_gross_profit_target,
  ROUND(monthly_prev_year_sales / 1000, 0) AS monthly_prev_year_sales,
  ROUND(monthly_prev_year_gross_profit / 1000, 0) AS monthly_prev_year_gross_profit,

  -- 当月比率
  CASE WHEN monthly_sales_target > 0
    THEN ROUND(monthly_sales_actual / monthly_sales_target * 100, 0)
    ELSE NULL
  END AS monthly_sales_target_ratio,
  CASE WHEN monthly_prev_year_sales > 0
    THEN ROUND(monthly_sales_actual / monthly_prev_year_sales * 100, 0)
    ELSE NULL
  END AS monthly_sales_prev_year_ratio,
  CASE WHEN monthly_gross_profit_target > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_gross_profit_target * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_target_ratio,
  CASE WHEN monthly_prev_year_gross_profit > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_prev_year_ratio,

  -- 当月粗利率
  CASE WHEN monthly_sales_actual > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate,
  -- 当月粗利率（計画）
  CASE WHEN monthly_sales_target > 0
    THEN ROUND(monthly_gross_profit_target / monthly_sales_target * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_target,
  -- 当月粗利率（前年）
  CASE WHEN monthly_prev_year_sales > 0
    THEN ROUND(monthly_prev_year_gross_profit / monthly_prev_year_sales * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_prev_year,
  -- 当月粗利率差（計画差）
  CASE WHEN monthly_sales_actual > 0 AND monthly_sales_target > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 1)
       - ROUND(monthly_gross_profit_target / monthly_sales_target * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_target_diff,
  -- 当月粗利率差（前年差）
  CASE WHEN monthly_sales_actual > 0 AND monthly_prev_year_sales > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 1)
       - ROUND(monthly_prev_year_gross_profit / monthly_prev_year_sales * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_prev_year_diff,

  -- 累計データ（千円単位）
  ROUND(cumulative_sales_actual / 1000, 0) AS cumulative_sales_actual,
  ROUND(cumulative_sales_target / 1000, 0) AS cumulative_sales_target,
  ROUND(cumulative_gross_profit_actual / 1000, 0) AS cumulative_gross_profit_actual,
  ROUND(cumulative_gross_profit_target / 1000, 0) AS cumulative_gross_profit_target,
  ROUND(cumulative_prev_year_sales / 1000, 0) AS cumulative_prev_year_sales,
  ROUND(cumulative_prev_year_gross_profit / 1000, 0) AS cumulative_prev_year_gross_profit,

  -- 累計比率
  CASE WHEN cumulative_sales_target > 0
    THEN ROUND(cumulative_sales_actual / cumulative_sales_target * 100, 0)
    ELSE NULL
  END AS cumulative_sales_target_ratio,
  CASE WHEN cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_sales_actual / cumulative_prev_year_sales * 100, 0)
    ELSE NULL
  END AS cumulative_sales_prev_year_ratio,
  CASE WHEN cumulative_gross_profit_target > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_gross_profit_target * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_target_ratio,
  CASE WHEN cumulative_prev_year_gross_profit > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_prev_year_ratio,

  -- 累計粗利率
  CASE WHEN cumulative_sales_actual > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate,
  -- 累計粗利率差（計画差）
  CASE WHEN cumulative_sales_actual > 0 AND cumulative_sales_target > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 1)
       - ROUND(cumulative_gross_profit_target / cumulative_sales_target * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate_target_diff,
  -- 累計粗利率差（前年差）
  CASE WHEN cumulative_sales_actual > 0 AND cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 1)
       - ROUND(cumulative_prev_year_gross_profit / cumulative_prev_year_sales * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate_prev_year_diff

FROM top20_customers

UNION ALL

-- 20社計
SELECT
  sales_accounting_period,
  main_department,
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  99 AS ranking,  -- 合計行用
  '99999' AS customer_code,
  '20社計' AS customer_name,
  '20社　計' AS customer_display_name,
  CAST(NULL AS STRING) AS business_type,

  -- 当月データ
  ROUND(SUM(monthly_sales_actual) / 1000, 0) AS monthly_sales_actual,
  ROUND(SUM(monthly_sales_target) / 1000, 0) AS monthly_sales_target,
  ROUND(SUM(monthly_gross_profit_actual) / 1000, 0) AS monthly_gross_profit_actual,
  ROUND(SUM(monthly_gross_profit_target) / 1000, 0) AS monthly_gross_profit_target,
  ROUND(SUM(monthly_prev_year_sales) / 1000, 0) AS monthly_prev_year_sales,
  ROUND(SUM(monthly_prev_year_gross_profit) / 1000, 0) AS monthly_prev_year_gross_profit,

  -- 当月比率
  CASE WHEN SUM(monthly_sales_target) > 0
    THEN ROUND(SUM(monthly_sales_actual) / SUM(monthly_sales_target) * 100, 0)
    ELSE NULL
  END AS monthly_sales_target_ratio,
  CASE WHEN SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_sales_actual) / SUM(monthly_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS monthly_sales_prev_year_ratio,
  CASE WHEN SUM(monthly_gross_profit_target) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_gross_profit_target) * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_target_ratio,
  CASE WHEN SUM(monthly_prev_year_gross_profit) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_prev_year_gross_profit) * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_prev_year_ratio,

  -- 当月粗利率
  CASE WHEN SUM(monthly_sales_actual) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_sales_actual) * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate,
  CASE WHEN SUM(monthly_sales_target) > 0
    THEN ROUND(SUM(monthly_gross_profit_target) / SUM(monthly_sales_target) * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_target,
  CASE WHEN SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_prev_year_gross_profit) / SUM(monthly_prev_year_sales) * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_prev_year,
  CASE WHEN SUM(monthly_sales_actual) > 0 AND SUM(monthly_sales_target) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_sales_actual) * 100, 1)
       - ROUND(SUM(monthly_gross_profit_target) / SUM(monthly_sales_target) * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_target_diff,
  CASE WHEN SUM(monthly_sales_actual) > 0 AND SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_sales_actual) * 100, 1)
       - ROUND(SUM(monthly_prev_year_gross_profit) / SUM(monthly_prev_year_sales) * 100, 1)
    ELSE NULL
  END AS monthly_gross_profit_rate_prev_year_diff,

  -- 累計データ
  ROUND(SUM(cumulative_sales_actual) / 1000, 0) AS cumulative_sales_actual,
  ROUND(SUM(cumulative_sales_target) / 1000, 0) AS cumulative_sales_target,
  ROUND(SUM(cumulative_gross_profit_actual) / 1000, 0) AS cumulative_gross_profit_actual,
  ROUND(SUM(cumulative_gross_profit_target) / 1000, 0) AS cumulative_gross_profit_target,
  ROUND(SUM(cumulative_prev_year_sales) / 1000, 0) AS cumulative_prev_year_sales,
  ROUND(SUM(cumulative_prev_year_gross_profit) / 1000, 0) AS cumulative_prev_year_gross_profit,

  -- 累計比率
  CASE WHEN SUM(cumulative_sales_target) > 0
    THEN ROUND(SUM(cumulative_sales_actual) / SUM(cumulative_sales_target) * 100, 0)
    ELSE NULL
  END AS cumulative_sales_target_ratio,
  CASE WHEN SUM(cumulative_prev_year_sales) > 0
    THEN ROUND(SUM(cumulative_sales_actual) / SUM(cumulative_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS cumulative_sales_prev_year_ratio,
  CASE WHEN SUM(cumulative_gross_profit_target) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_gross_profit_target) * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_target_ratio,
  CASE WHEN SUM(cumulative_prev_year_gross_profit) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_prev_year_gross_profit) * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_prev_year_ratio,

  -- 累計粗利率
  CASE WHEN SUM(cumulative_sales_actual) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_sales_actual) * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate,
  CASE WHEN SUM(cumulative_sales_actual) > 0 AND SUM(cumulative_sales_target) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_sales_actual) * 100, 1)
       - ROUND(SUM(cumulative_gross_profit_target) / SUM(cumulative_sales_target) * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate_target_diff,
  CASE WHEN SUM(cumulative_sales_actual) > 0 AND SUM(cumulative_prev_year_sales) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_sales_actual) * 100, 1)
       - ROUND(SUM(cumulative_prev_year_gross_profit) / SUM(cumulative_prev_year_sales) * 100, 1)
    ELSE NULL
  END AS cumulative_gross_profit_rate_prev_year_diff

FROM top20_customers
GROUP BY sales_accounting_period, main_department;
