/*
============================================================
DM: 累計経営資料（全支店統合版）- Looker Studio用
============================================================
目的: 期首（9月）からの累計値を計算し、グラフ可視化用のデータソースを作成
データソース: management_documents_all_period_all
対象支店: 東京支店、長崎支店、福岡支店
対象部門: main_display_flag=1 のもの

出力スキーマ:
  - date: 対象年月（DATE型）
  - date_sort_key: 日付ソート用キー（YYYYMM形式の整数）
  - date_label: 日付表示用ラベル（例: "9月", "10月"）
  - fiscal_year: 会計年度（例: "2024年度"）
  - fiscal_month: 会計月（9月=1, 10月=2, ..., 8月=12）
  - main_department: 支店名
  - main_department_sort_order: 支店ソート順
  - secondary_department: 部門名
  - secondary_department_sort_order: 部門ソート順
  - metric: 指標名（売上高、売上総利益、営業利益率、営業利益、営業経費、経常利益）
  - metric_sort_order: 指標ソート順
  - category: カテゴリ（本年実績、前年実績、本年目標）
  - monthly_value: 当月値（千円）
  - cumulative_value: 期首からの累計値（千円）

累計計算ロジック:
  - 会計年度: 9月〜翌8月
  - 累計: 期首（9月）から当月までの合計
  - 営業利益率: 累計営業利益 ÷ 累計売上高 × 100
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all` AS

WITH
-- ============================================================
-- 会計年度・会計月の計算
-- ============================================================
base_data AS (
  SELECT
    date,
    -- 日付ソート用キー（YYYYMM形式の整数）
    EXTRACT(YEAR FROM date) * 100 + EXTRACT(MONTH FROM date) AS date_sort_key,
    -- 日付表示用ラベル（例: "9月"）
    CONCAT(CAST(EXTRACT(MONTH FROM date) AS STRING), '月') AS date_label,
    -- 会計年度: 9月〜翌8月（9月以降は当年、1-8月は前年）
    CASE
      WHEN EXTRACT(MONTH FROM date) >= 9 THEN CONCAT(CAST(EXTRACT(YEAR FROM date) AS STRING), '年度')
      ELSE CONCAT(CAST(EXTRACT(YEAR FROM date) - 1 AS STRING), '年度')
    END AS fiscal_year,
    -- 会計月: 9月=1, 10月=2, ..., 8月=12
    CASE
      WHEN EXTRACT(MONTH FROM date) >= 9 THEN EXTRACT(MONTH FROM date) - 8
      ELSE EXTRACT(MONTH FROM date) + 4
    END AS fiscal_month,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_category,
    secondary_category,
    display_value,
    main_display_flag
  FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all`
  WHERE main_display_flag = 1
),

-- ============================================================
-- 対象データの抽出（指標 × カテゴリ）
-- ============================================================
filtered_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_category AS metric,
    -- カテゴリ名を簡略化
    CASE secondary_category
      WHEN '本年実績(千円)' THEN '本年実績'
      WHEN '前年実績(千円)' THEN '前年実績'
      WHEN '本年目標(千円)' THEN '本年目標'
    END AS category,
    display_value AS monthly_value
  FROM base_data
  WHERE main_category IN ('売上高', '売上総利益', '営業利益', '営業経費', '経常利益')
    AND secondary_category IN ('本年実績(千円)', '前年実績(千円)', '本年目標(千円)')
),

-- ============================================================
-- 累計計算（Window関数）
-- ============================================================
cumulative_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    metric,
    -- 指標ソート順
    CASE metric
      WHEN '売上高' THEN 1
      WHEN '売上総利益' THEN 2
      WHEN '営業利益率' THEN 3
      WHEN '営業利益' THEN 4
      WHEN '営業経費' THEN 5
      WHEN '経常利益' THEN 6
    END AS metric_sort_order,
    category,
    monthly_value,
    -- 累計値の計算
    SUM(monthly_value) OVER (
      PARTITION BY fiscal_year, main_department, secondary_department, metric, category
      ORDER BY fiscal_month
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_value
  FROM filtered_data
),

-- ============================================================
-- 営業利益率の計算用データ（売上高と営業利益の累計を取得）
-- ============================================================
profit_margin_base AS (
  SELECT
    c1.date,
    c1.date_sort_key,
    c1.date_label,
    c1.fiscal_year,
    c1.fiscal_month,
    c1.main_department,
    c1.main_department_sort_order,
    c1.secondary_department,
    c1.secondary_department_sort_order,
    c1.category,
    c1.cumulative_value AS cumulative_sales,
    c2.cumulative_value AS cumulative_operating_income
  FROM cumulative_data c1
  INNER JOIN cumulative_data c2
    ON c1.date = c2.date
    AND c1.fiscal_year = c2.fiscal_year
    AND c1.main_department = c2.main_department
    AND c1.secondary_department = c2.secondary_department
    AND c1.category = c2.category
  WHERE c1.metric = '売上高'
    AND c2.metric = '営業利益'
),

-- ============================================================
-- 営業利益率データの生成
-- ============================================================
profit_margin_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    '営業利益率' AS metric,
    3 AS metric_sort_order,
    category,
    -- 当月の営業利益率は計算しない（累計のみ意味がある）
    NULL AS monthly_value,
    -- 累計営業利益率 = 累計営業利益 ÷ 累計売上高 × 100
    SAFE_DIVIDE(cumulative_operating_income, cumulative_sales) * 100 AS cumulative_value
  FROM profit_margin_base
)

-- ============================================================
-- 最終出力（通常指標 + 営業利益率）
-- ============================================================
SELECT
  date,
  date_sort_key,
  date_label,
  fiscal_year,
  fiscal_month,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_sort_order,
  metric,
  metric_sort_order,
  category,
  monthly_value,
  cumulative_value
FROM cumulative_data

UNION ALL

SELECT
  date,
  date_sort_key,
  date_label,
  fiscal_year,
  fiscal_month,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_sort_order,
  metric,
  metric_sort_order,
  category,
  monthly_value,
  cumulative_value
FROM profit_margin_data

ORDER BY
  date_sort_key,
  main_department_sort_order,
  secondary_department_sort_order,
  metric_sort_order,
  category;
