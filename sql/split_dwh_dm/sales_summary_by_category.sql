/*
============================================================
DataMart: 総括表_2.売上粗利形態別
============================================================
目的: 部門（division）別の売上・粗利サマリー

データソース:
  - sales_target_and_achievements

表示項目:
  - 部門コード・部門名（division_code, division_name）
  - 売上高（実績/前年比）
  - 粗利額（実績/前年比）
  - 粗利率（実績/前年差）

集計単位:
  - 支店（東京/長崎/福岡）× 部門（division）× 年月

累計計算:
  - 期首月（9月）からの累計を計算
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.sales_summary_by_category`
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
      WHEN department_code = 25 THEN '東京支店'
      WHEN department_code IN (65, 66) THEN '長崎支店'
      WHEN department_code = 31 THEN '福岡支店'
      ELSE 'その他'
    END AS main_department,
    division_code,
    division_name,
    sales_actual,
    gross_profit_actual,
    prev_year_sales_actual,
    prev_year_gross_profit_actual
  FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
),

-- 当月データを部門別に集計
monthly_summary AS (
  SELECT
    sales_accounting_period,
    main_department,
    division_code,
    division_name,
    SUM(sales_actual) AS monthly_sales_actual,
    SUM(gross_profit_actual) AS monthly_gross_profit_actual,
    SUM(prev_year_sales_actual) AS monthly_prev_year_sales,
    SUM(prev_year_gross_profit_actual) AS monthly_prev_year_gross_profit
  FROM base_data
  WHERE main_department != 'その他'
  GROUP BY sales_accounting_period, main_department, division_code, division_name
),

-- 期首月を計算（9月始まり）
fiscal_year_start AS (
  SELECT
    sales_accounting_period,
    main_department,
    division_code,
    division_name,
    monthly_sales_actual,
    monthly_gross_profit_actual,
    monthly_prev_year_sales,
    monthly_prev_year_gross_profit,
    -- 期首月計算（9月が期首）
    DATE_TRUNC(
      DATE_ADD(sales_accounting_period, INTERVAL 4 MONTH),
      YEAR
    ) AS fiscal_year,
    -- 会計年度の開始月
    DATE_SUB(
      DATE_TRUNC(
        DATE_ADD(sales_accounting_period, INTERVAL 4 MONTH),
        YEAR
      ),
      INTERVAL 4 MONTH
    ) AS fiscal_start_month
  FROM monthly_summary
),

-- 累計計算
cumulative_data AS (
  SELECT
    f.sales_accounting_period,
    f.main_department,
    f.division_code,
    f.division_name,
    f.monthly_sales_actual,
    f.monthly_gross_profit_actual,
    f.monthly_prev_year_sales,
    f.monthly_prev_year_gross_profit,
    f.fiscal_start_month,
    -- 累計値（期首月から当月まで）
    SUM(m.monthly_sales_actual) OVER (
      PARTITION BY f.main_department, f.division_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_actual,
    SUM(m.monthly_gross_profit_actual) OVER (
      PARTITION BY f.main_department, f.division_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gross_profit_actual,
    SUM(m.monthly_prev_year_sales) OVER (
      PARTITION BY f.main_department, f.division_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_sales,
    SUM(m.monthly_prev_year_gross_profit) OVER (
      PARTITION BY f.main_department, f.division_code, f.fiscal_start_month
      ORDER BY f.sales_accounting_period
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_prev_year_gross_profit
  FROM fiscal_year_start f
  JOIN monthly_summary m
    ON f.sales_accounting_period = m.sales_accounting_period
    AND f.main_department = m.main_department
    AND f.division_code = m.division_code
)

SELECT
  sales_accounting_period,
  main_department,
  -- 支店ソート順
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  division_code,
  division_name,
  -- 部門表示名（コード + 名前）
  CONCAT(LPAD(CAST(division_code AS STRING), 3, '0'), ' ', division_name) AS division_display_name,
  -- 部門ソート順
  CAST(division_code AS INT64) AS division_sort_order,

  -- 当月データ（千円単位に変換）
  ROUND(monthly_sales_actual / 1000, 0) AS monthly_sales_actual,
  ROUND(monthly_gross_profit_actual / 1000, 0) AS monthly_gross_profit_actual,
  ROUND(monthly_prev_year_sales / 1000, 0) AS monthly_prev_year_sales,
  ROUND(monthly_prev_year_gross_profit / 1000, 0) AS monthly_prev_year_gross_profit,

  -- 当月前年比（%）
  CASE WHEN monthly_prev_year_sales > 0
    THEN ROUND(monthly_sales_actual / monthly_prev_year_sales * 100, 0)
    ELSE NULL
  END AS monthly_sales_prev_year_ratio,
  CASE WHEN monthly_prev_year_gross_profit > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_prev_year_ratio,

  -- 当月粗利率（%）
  CASE WHEN monthly_sales_actual > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate,
  -- 当月前年粗利率（%）
  CASE WHEN monthly_prev_year_sales > 0
    THEN ROUND(monthly_prev_year_gross_profit / monthly_prev_year_sales * 100, 0)
    ELSE NULL
  END AS monthly_prev_year_gross_profit_rate,
  -- 当月粗利率前年差（%ポイント）
  CASE WHEN monthly_sales_actual > 0 AND monthly_prev_year_sales > 0
    THEN ROUND(monthly_gross_profit_actual / monthly_sales_actual * 100, 0)
       - ROUND(monthly_prev_year_gross_profit / monthly_prev_year_sales * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate_diff,

  -- 累計データ（千円単位に変換）
  ROUND(cumulative_sales_actual / 1000, 0) AS cumulative_sales_actual,
  ROUND(cumulative_gross_profit_actual / 1000, 0) AS cumulative_gross_profit_actual,
  ROUND(cumulative_prev_year_sales / 1000, 0) AS cumulative_prev_year_sales,
  ROUND(cumulative_prev_year_gross_profit / 1000, 0) AS cumulative_prev_year_gross_profit,

  -- 累計前年比（%）
  CASE WHEN cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_sales_actual / cumulative_prev_year_sales * 100, 0)
    ELSE NULL
  END AS cumulative_sales_prev_year_ratio,
  CASE WHEN cumulative_prev_year_gross_profit > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_prev_year_gross_profit * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_prev_year_ratio,

  -- 累計粗利率（%）
  CASE WHEN cumulative_sales_actual > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate,
  -- 累計前年粗利率（%）
  CASE WHEN cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_prev_year_gross_profit / cumulative_prev_year_sales * 100, 0)
    ELSE NULL
  END AS cumulative_prev_year_gross_profit_rate,
  -- 累計粗利率前年差（%ポイント）
  CASE WHEN cumulative_sales_actual > 0 AND cumulative_prev_year_sales > 0
    THEN ROUND(cumulative_gross_profit_actual / cumulative_sales_actual * 100, 0)
       - ROUND(cumulative_prev_year_gross_profit / cumulative_prev_year_sales * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate_diff

FROM cumulative_data

UNION ALL

-- 合計行
SELECT
  sales_accounting_period,
  main_department,
  -- 支店ソート順
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  999 AS division_code,  -- 合計用のダミーコード
  '合計' AS division_name,
  '【合　計】' AS division_display_name,
  999 AS division_sort_order,

  -- 当月データ（千円単位に変換）
  ROUND(SUM(monthly_sales_actual) / 1000, 0) AS monthly_sales_actual,
  ROUND(SUM(monthly_gross_profit_actual) / 1000, 0) AS monthly_gross_profit_actual,
  ROUND(SUM(monthly_prev_year_sales) / 1000, 0) AS monthly_prev_year_sales,
  ROUND(SUM(monthly_prev_year_gross_profit) / 1000, 0) AS monthly_prev_year_gross_profit,

  -- 当月前年比（%）
  CASE WHEN SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_sales_actual) / SUM(monthly_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS monthly_sales_prev_year_ratio,
  CASE WHEN SUM(monthly_prev_year_gross_profit) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_prev_year_gross_profit) * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_prev_year_ratio,

  -- 当月粗利率（%）
  CASE WHEN SUM(monthly_sales_actual) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_sales_actual) * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate,
  -- 当月前年粗利率（%）
  CASE WHEN SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_prev_year_gross_profit) / SUM(monthly_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS monthly_prev_year_gross_profit_rate,
  -- 当月粗利率前年差
  CASE WHEN SUM(monthly_sales_actual) > 0 AND SUM(monthly_prev_year_sales) > 0
    THEN ROUND(SUM(monthly_gross_profit_actual) / SUM(monthly_sales_actual) * 100, 0)
       - ROUND(SUM(monthly_prev_year_gross_profit) / SUM(monthly_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS monthly_gross_profit_rate_diff,

  -- 累計データ（千円単位に変換）
  ROUND(SUM(cumulative_sales_actual) / 1000, 0) AS cumulative_sales_actual,
  ROUND(SUM(cumulative_gross_profit_actual) / 1000, 0) AS cumulative_gross_profit_actual,
  ROUND(SUM(cumulative_prev_year_sales) / 1000, 0) AS cumulative_prev_year_sales,
  ROUND(SUM(cumulative_prev_year_gross_profit) / 1000, 0) AS cumulative_prev_year_gross_profit,

  -- 累計前年比（%）
  CASE WHEN SUM(cumulative_prev_year_sales) > 0
    THEN ROUND(SUM(cumulative_sales_actual) / SUM(cumulative_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS cumulative_sales_prev_year_ratio,
  CASE WHEN SUM(cumulative_prev_year_gross_profit) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_prev_year_gross_profit) * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_prev_year_ratio,

  -- 累計粗利率（%）
  CASE WHEN SUM(cumulative_sales_actual) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_sales_actual) * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate,
  -- 累計前年粗利率（%）
  CASE WHEN SUM(cumulative_prev_year_sales) > 0
    THEN ROUND(SUM(cumulative_prev_year_gross_profit) / SUM(cumulative_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS cumulative_prev_year_gross_profit_rate,
  -- 累計粗利率前年差
  CASE WHEN SUM(cumulative_sales_actual) > 0 AND SUM(cumulative_prev_year_sales) > 0
    THEN ROUND(SUM(cumulative_gross_profit_actual) / SUM(cumulative_sales_actual) * 100, 0)
       - ROUND(SUM(cumulative_prev_year_gross_profit) / SUM(cumulative_prev_year_sales) * 100, 0)
    ELSE NULL
  END AS cumulative_gross_profit_rate_diff

FROM cumulative_data
GROUP BY sales_accounting_period, main_department;
