/*
============================================================
DataMart: 総括表_1.業績
============================================================
目的: 支店レベルの業績サマリーテーブル

データソース:
  - cumulative_management_documents_all_period_all
    （monthly_value: 当月、cumulative_value: 累計を使用）

表示項目:
  - 売上高（額・率）
  - 粗利額（額・率）= 売上総利益
  - 営業経費（額・率）
  - 営業利益（額・率）
  - 経常利益（額・率）

カテゴリ:
  - 計画（本年目標）
  - 実績（本年実績）
  - 前年（前年実績）
  - 計画比（目標比% × 100）
  - 計画差（実績 - 計画）
  - 前年比（前年比% × 100）
  - 前年差（実績 - 前年）

単位:
  - 額: 千円
  - 率: %（小数を100倍して表示）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.sales_summary_performance`
PARTITION BY date
CLUSTER BY main_department
AS

WITH base_data AS (
  -- cumulative_management_documents_all_period_allから支店計のみ抽出
  SELECT
    date,
    main_category,
    secondary_category,
    main_department,
    secondary_department,
    monthly_value,
    cumulative_value
  FROM `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all`
  WHERE (main_department = '東京支店' AND secondary_department = '東京支店計')
     OR (main_department = '長崎支店' AND secondary_department = '長崎支店計')
     OR (main_department = '福岡支店' AND secondary_department = '福岡支店計')
),

-- 当月データをピボット（金額項目）- monthly_valueを使用
monthly_amount_raw AS (
  SELECT
    date,
    main_department,
    main_category,
    MAX(CASE WHEN secondary_category = '本年目標(千円)' THEN monthly_value END) AS plan,
    MAX(CASE WHEN secondary_category = '本年実績(千円)' THEN monthly_value END) AS actual,
    MAX(CASE WHEN secondary_category = '前年実績(千円)' THEN monthly_value END) AS prev_year
  FROM base_data
  WHERE main_category IN ('売上高', '売上総利益', '営業経費', '営業利益', '経常利益')
  GROUP BY date, main_department, main_category
),

-- 当月比率を計算（元データにmonthly比率がないため）
monthly_amount AS (
  SELECT
    date,
    main_department,
    main_category,
    plan,
    actual,
    prev_year,
    -- 計画比（%）= 実績 / 計画 * 100
    CASE WHEN plan > 0 THEN ROUND(actual / plan * 100, 1) ELSE NULL END AS plan_ratio,
    -- 前年比（%）= 実績 / 前年 * 100
    CASE WHEN prev_year > 0 THEN ROUND(actual / prev_year * 100, 1) ELSE NULL END AS prev_year_ratio
  FROM monthly_amount_raw
),

-- 累計データをピボット（金額項目）- cumulative_valueを使用
cumulative_amount AS (
  SELECT
    date,
    main_department,
    main_category,
    MAX(CASE WHEN secondary_category = '本年目標(千円)' THEN cumulative_value END) AS plan,
    MAX(CASE WHEN secondary_category = '本年実績(千円)' THEN cumulative_value END) AS actual,
    MAX(CASE WHEN secondary_category = '前年実績(千円)' THEN cumulative_value END) AS prev_year,
    MAX(CASE WHEN secondary_category = '目標比(%)' THEN cumulative_value * 100 END) AS plan_ratio,
    MAX(CASE WHEN secondary_category = '前年比(%)' THEN cumulative_value * 100 END) AS prev_year_ratio
  FROM base_data
  WHERE main_category IN ('売上高', '売上総利益', '営業経費', '営業利益', '経常利益')
  GROUP BY date, main_department, main_category
),

-- 売上高を取得（率計算用）
monthly_sales AS (
  SELECT date, main_department, actual AS sales_actual, plan AS sales_plan, prev_year AS sales_prev_year
  FROM monthly_amount
  WHERE main_category = '売上高'
),

cumulative_sales AS (
  SELECT date, main_department, actual AS sales_actual, plan AS sales_plan, prev_year AS sales_prev_year
  FROM cumulative_amount
  WHERE main_category = '売上高'
),

-- 当月粗利率を売上高・粗利額から計算
monthly_gross_profit_rate AS (
  SELECT
    s.date,
    s.main_department,
    CASE WHEN s.plan > 0 THEN ROUND(g.plan / s.plan * 100, 1) ELSE NULL END AS plan_rate,
    CASE WHEN s.actual > 0 THEN ROUND(g.actual / s.actual * 100, 1) ELSE NULL END AS actual_rate,
    CASE WHEN s.prev_year > 0 THEN ROUND(g.prev_year / s.prev_year * 100, 1) ELSE NULL END AS prev_year_rate
  FROM monthly_amount s
  INNER JOIN monthly_amount g ON s.date = g.date AND s.main_department = g.main_department
  WHERE s.main_category = '売上高' AND g.main_category = '売上総利益'
),

-- 累計粗利率を売上高・粗利額から計算
cumulative_gross_profit_rate AS (
  SELECT
    s.date,
    s.main_department,
    CASE WHEN s.plan > 0 THEN ROUND(g.plan / s.plan * 100, 1) ELSE NULL END AS plan_rate,
    CASE WHEN s.actual > 0 THEN ROUND(g.actual / s.actual * 100, 1) ELSE NULL END AS actual_rate,
    CASE WHEN s.prev_year > 0 THEN ROUND(g.prev_year / s.prev_year * 100, 1) ELSE NULL END AS prev_year_rate
  FROM cumulative_amount s
  INNER JOIN cumulative_amount g ON s.date = g.date AND s.main_department = g.main_department
  WHERE s.main_category = '売上高' AND g.main_category = '売上総利益'
),

-- メインデータを結合
combined AS (
  SELECT
    m.date,
    m.main_department,
    m.main_category,
    -- カテゴリ表示名
    CASE m.main_category
      WHEN '売上高' THEN '売上高'
      WHEN '売上総利益' THEN '粗利額'
      WHEN '営業経費' THEN '営業経費'
      WHEN '営業利益' THEN '営業利益'
      WHEN '経常利益' THEN '経常利益'
    END AS display_category,
    -- カテゴリソート順
    CASE m.main_category
      WHEN '売上高' THEN 1
      WHEN '売上総利益' THEN 2
      WHEN '営業経費' THEN 3
      WHEN '営業利益' THEN 4
      WHEN '経常利益' THEN 5
    END AS category_sort_order,
    -- 当月額データ（千円）
    m.plan AS monthly_plan,
    m.actual AS monthly_actual,
    m.prev_year AS monthly_prev_year,
    m.plan_ratio AS monthly_plan_ratio,  -- %表示（既に100倍済み）
    SAFE_SUBTRACT(m.actual, m.plan) AS monthly_plan_diff,  -- 差（千円）
    m.prev_year_ratio AS monthly_prev_year_ratio,  -- %表示
    SAFE_SUBTRACT(m.actual, m.prev_year) AS monthly_prev_year_diff,  -- 差（千円）
    -- 累計額データ（千円）
    c.plan AS cumulative_plan,
    c.actual AS cumulative_actual,
    c.prev_year AS cumulative_prev_year,
    c.plan_ratio AS cumulative_plan_ratio,
    SAFE_SUBTRACT(c.actual, c.plan) AS cumulative_plan_diff,
    c.prev_year_ratio AS cumulative_prev_year_ratio,
    SAFE_SUBTRACT(c.actual, c.prev_year) AS cumulative_prev_year_diff,
    -- 当月率（売上高比%）- 粗利率は専用データ、他は計算
    CASE
      WHEN m.main_category = '売上総利益' THEN gpr.actual_rate
      WHEN m.main_category = '売上高' THEN NULL  -- 売上高自体は率不要
      WHEN ms.sales_actual > 0 THEN ROUND(m.actual / ms.sales_actual * 100, 1)
      ELSE NULL
    END AS monthly_actual_rate,
    CASE
      WHEN m.main_category = '売上総利益' THEN gpr.plan_rate
      WHEN m.main_category = '売上高' THEN NULL
      WHEN ms.sales_plan > 0 THEN ROUND(m.plan / ms.sales_plan * 100, 1)
      ELSE NULL
    END AS monthly_plan_rate,
    CASE
      WHEN m.main_category = '売上総利益' THEN gpr.prev_year_rate
      WHEN m.main_category = '売上高' THEN NULL
      WHEN ms.sales_prev_year > 0 THEN ROUND(m.prev_year / ms.sales_prev_year * 100, 1)
      ELSE NULL
    END AS monthly_prev_year_rate,
    -- 累計率（売上高比%）
    CASE
      WHEN m.main_category = '売上総利益' THEN cgpr.actual_rate
      WHEN m.main_category = '売上高' THEN NULL
      WHEN cs.sales_actual > 0 THEN ROUND(c.actual / cs.sales_actual * 100, 1)
      ELSE NULL
    END AS cumulative_actual_rate,
    CASE
      WHEN m.main_category = '売上総利益' THEN cgpr.plan_rate
      WHEN m.main_category = '売上高' THEN NULL
      WHEN cs.sales_plan > 0 THEN ROUND(c.plan / cs.sales_plan * 100, 1)
      ELSE NULL
    END AS cumulative_plan_rate,
    CASE
      WHEN m.main_category = '売上総利益' THEN cgpr.prev_year_rate
      WHEN m.main_category = '売上高' THEN NULL
      WHEN cs.sales_prev_year > 0 THEN ROUND(c.prev_year / cs.sales_prev_year * 100, 1)
      ELSE NULL
    END AS cumulative_prev_year_rate
  FROM monthly_amount m
  LEFT JOIN cumulative_amount c
    ON m.date = c.date AND m.main_department = c.main_department AND m.main_category = c.main_category
  LEFT JOIN monthly_sales ms
    ON m.date = ms.date AND m.main_department = ms.main_department
  LEFT JOIN cumulative_sales cs
    ON m.date = cs.date AND m.main_department = cs.main_department
  LEFT JOIN monthly_gross_profit_rate gpr
    ON m.date = gpr.date AND m.main_department = gpr.main_department
  LEFT JOIN cumulative_gross_profit_rate cgpr
    ON m.date = cgpr.date AND m.main_department = cgpr.main_department
)

SELECT
  date,
  main_department,
  -- 支店ソート順
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  main_category,
  display_category,
  category_sort_order,
  -- 当月額データ（千円）
  monthly_plan,
  monthly_actual,
  monthly_prev_year,
  monthly_plan_ratio,  -- %
  monthly_plan_diff,   -- 千円
  monthly_prev_year_ratio,  -- %
  monthly_prev_year_diff,   -- 千円
  -- 累計額データ（千円）
  cumulative_plan,
  cumulative_actual,
  cumulative_prev_year,
  cumulative_plan_ratio,  -- %
  cumulative_plan_diff,   -- 千円
  cumulative_prev_year_ratio,  -- %
  cumulative_prev_year_diff,   -- 千円
  -- 当月率データ（%）
  monthly_plan_rate,
  monthly_actual_rate,
  monthly_prev_year_rate,
  -- 当月率の差分（%ポイント）
  SAFE_SUBTRACT(monthly_actual_rate, monthly_plan_rate) AS monthly_rate_plan_diff,
  SAFE_SUBTRACT(monthly_actual_rate, monthly_prev_year_rate) AS monthly_rate_prev_year_diff,
  -- 累計率データ（%）
  cumulative_plan_rate,
  cumulative_actual_rate,
  cumulative_prev_year_rate,
  -- 累計率の差分（%ポイント）
  SAFE_SUBTRACT(cumulative_actual_rate, cumulative_plan_rate) AS cumulative_rate_plan_diff,
  SAFE_SUBTRACT(cumulative_actual_rate, cumulative_prev_year_rate) AS cumulative_rate_prev_year_diff
FROM combined;
