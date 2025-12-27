/*
============================================================
DM: 累計経営資料（全支店統合版）- Looker Studio用
============================================================
目的: 期首（9月）からの累計値を計算し、グラフ可視化用のデータソースを作成
データソース: management_documents_all_period_all
対象支店: 東京支店、長崎支店、福岡支店
対象部門: 全部門（main_display_flagに関わらず）

出力スキーマ:
  - date: 対象年月（DATE型）
  - date_sort_key: 日付ソート用キー（YYYYMM形式の整数）
  - date_label: 日付表示用ラベル（例: "9月", "10月"）
  - fiscal_year: 会計年度（例: "2024年度"）
  - fiscal_month: 会計月（9月=1, 10月=2, ..., 8月=12）
  - main_category: 指標名（売上高、売上総利益、売上総利益率、営業経費、営業利益、等）
  - main_category_sort_order: 指標ソート順
  - secondary_category: カテゴリ（本年実績、前年実績、本年目標、前年比、目標比、等）
  - secondary_category_sort_order: カテゴリソート順
  - main_department: 支店名
  - main_department_sort_order: 支店ソート順
  - secondary_department: 部門名
  - secondary_department_sort_order: 部門ソート順
  - main_display_flag: 表示フラグ
  - monthly_value: 当月値（千円）
  - cumulative_value: 期首からの累計値（千円）

累計計算ロジック:
  - 会計年度: 9月〜翌8月
  - 累計: 期首（9月）から当月までの合計
  - 売上総利益率: 累計売上総利益 ÷ 累計売上高 × 100
  - 前年比: 累計本年実績 ÷ 累計前年実績 × 100
  - 目標比: 累計本年実績 ÷ 累計本年目標 × 100

パフォーマンス最適化:
  - 中間結果をTEMPテーブルに格納し、CPU使用量を削減
============================================================
*/

-- ステップ1: 基本データと累計計算
CREATE TEMP TABLE cumulative_amount AS
WITH
base_data AS (
  SELECT
    date,
    EXTRACT(YEAR FROM date) * 100 + EXTRACT(MONTH FROM date) AS date_sort_key,
    CONCAT(CAST(EXTRACT(MONTH FROM date) AS STRING), '月') AS date_label,
    CASE
      WHEN EXTRACT(MONTH FROM date) >= 9 THEN CONCAT(CAST(EXTRACT(YEAR FROM date) AS STRING), '年度')
      ELSE CONCAT(CAST(EXTRACT(YEAR FROM date) - 1 AS STRING), '年度')
    END AS fiscal_year,
    CASE
      WHEN EXTRACT(MONTH FROM date) >= 9 THEN EXTRACT(MONTH FROM date) - 8
      ELSE EXTRACT(MONTH FROM date) + 4
    END AS fiscal_month,
    main_category,
    main_category_sort_order,
    secondary_category,
    secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    display_value,
    main_display_flag
  FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all`
),
amount_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    secondary_category,
    CASE
      WHEN main_category = '経常利益' THEN
        CASE secondary_category
          WHEN '本年目標(千円)' THEN 1
          WHEN '本年実績(千円)' THEN 2
          WHEN '累積本年目標(千円)' THEN 3
          WHEN '累積本年実績(千円)' THEN 4
          ELSE 99
        END
      ELSE
        CASE secondary_category
          WHEN '本年実績(千円)' THEN 1
          WHEN '前年実績(千円)' THEN 2
          WHEN '本年目標(千円)' THEN 3
          ELSE 99
        END
    END AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    display_value AS monthly_value
  FROM base_data
  WHERE main_category != '売上総利益率'
    AND (
      secondary_category IN ('本年実績(千円)', '前年実績(千円)', '本年目標(千円)')
      OR (main_category = '経常利益' AND secondary_category IN ('累積本年実績(千円)', '累積本年目標(千円)'))
    )
)
SELECT
  date,
  date_sort_key,
  date_label,
  fiscal_year,
  fiscal_month,
  main_category,
  main_category_sort_order,
  secondary_category,
  secondary_category_sort_order,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_sort_order,
  main_display_flag,
  monthly_value,
  SUM(monthly_value) OVER (
    PARTITION BY fiscal_year, main_department, secondary_department, main_category, secondary_category
    ORDER BY fiscal_month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_value
FROM amount_data;

-- ステップ2: 売上総利益率データ
CREATE TEMP TABLE gross_profit_margin_data AS
SELECT
  c1.date,
  c1.date_sort_key,
  c1.date_label,
  c1.fiscal_year,
  c1.fiscal_month,
  '売上総利益率' AS main_category,
  3 AS main_category_sort_order,
  CASE c1.secondary_category
    WHEN '本年実績(千円)' THEN '本年実績(%)'
    WHEN '前年実績(千円)' THEN '前年実績(%)'
    WHEN '本年目標(千円)' THEN '本年目標(%)'
  END AS secondary_category,
  c1.secondary_category_sort_order,
  c1.main_department,
  c1.main_department_sort_order,
  c1.secondary_department,
  c1.secondary_department_sort_order,
  c1.main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(c2.cumulative_value, c1.cumulative_value) AS cumulative_value
FROM cumulative_amount c1
INNER JOIN cumulative_amount c2
  ON c1.date = c2.date
  AND c1.fiscal_year = c2.fiscal_year
  AND c1.main_department = c2.main_department
  AND c1.secondary_department = c2.secondary_department
  AND c1.secondary_category = c2.secondary_category
WHERE c1.main_category = '売上高'
  AND c2.main_category = '売上総利益'
  AND c1.secondary_category IN ('本年実績(千円)', '前年実績(千円)', '本年目標(千円)');

-- ステップ3: 前年比・目標比データ
CREATE TEMP TABLE ratio_data AS
-- 前年比
SELECT
  c1.date,
  c1.date_sort_key,
  c1.date_label,
  c1.fiscal_year,
  c1.fiscal_month,
  c1.main_category,
  c1.main_category_sort_order,
  '前年比(%)' AS secondary_category,
  4 AS secondary_category_sort_order,
  c1.main_department,
  c1.main_department_sort_order,
  c1.secondary_department,
  c1.secondary_department_sort_order,
  c1.main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(c1.cumulative_value, c2.cumulative_value) AS cumulative_value
FROM cumulative_amount c1
INNER JOIN cumulative_amount c2
  ON c1.date = c2.date
  AND c1.fiscal_year = c2.fiscal_year
  AND c1.main_department = c2.main_department
  AND c1.secondary_department = c2.secondary_department
  AND c1.main_category = c2.main_category
WHERE c1.secondary_category = '本年実績(千円)'
  AND c2.secondary_category = '前年実績(千円)'
  AND c1.main_category NOT IN ('売上総利益率', '経常利益')
UNION ALL
-- 目標比
SELECT
  c1.date,
  c1.date_sort_key,
  c1.date_label,
  c1.fiscal_year,
  c1.fiscal_month,
  c1.main_category,
  c1.main_category_sort_order,
  '目標比(%)' AS secondary_category,
  5 AS secondary_category_sort_order,
  c1.main_department,
  c1.main_department_sort_order,
  c1.secondary_department,
  c1.secondary_department_sort_order,
  c1.main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(c1.cumulative_value, c2.cumulative_value) AS cumulative_value
FROM cumulative_amount c1
INNER JOIN cumulative_amount c2
  ON c1.date = c2.date
  AND c1.fiscal_year = c2.fiscal_year
  AND c1.main_department = c2.main_department
  AND c1.secondary_department = c2.secondary_department
  AND c1.main_category = c2.main_category
WHERE c1.secondary_category = '本年実績(千円)'
  AND c2.secondary_category = '本年目標(千円)'
  AND c1.main_category NOT IN ('売上総利益率', '経常利益');

-- ステップ4: 売上総利益率の前年比・目標比
CREATE TEMP TABLE gross_margin_ratio AS
SELECT
  g1.date,
  g1.date_sort_key,
  g1.date_label,
  g1.fiscal_year,
  g1.fiscal_month,
  '売上総利益率' AS main_category,
  3 AS main_category_sort_order,
  '前年比(%)' AS secondary_category,
  4 AS secondary_category_sort_order,
  g1.main_department,
  g1.main_department_sort_order,
  g1.secondary_department,
  g1.secondary_department_sort_order,
  g1.main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(g1.cumulative_value, g2.cumulative_value) AS cumulative_value
FROM gross_profit_margin_data g1
INNER JOIN gross_profit_margin_data g2
  ON g1.date = g2.date
  AND g1.main_department = g2.main_department
  AND g1.secondary_department = g2.secondary_department
WHERE g1.secondary_category = '本年実績(%)'
  AND g2.secondary_category = '前年実績(%)'
UNION ALL
SELECT
  g1.date,
  g1.date_sort_key,
  g1.date_label,
  g1.fiscal_year,
  g1.fiscal_month,
  '売上総利益率' AS main_category,
  3 AS main_category_sort_order,
  '目標比(%)' AS secondary_category,
  5 AS secondary_category_sort_order,
  g1.main_department,
  g1.main_department_sort_order,
  g1.secondary_department,
  g1.secondary_department_sort_order,
  g1.main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(g1.cumulative_value, g3.cumulative_value) AS cumulative_value
FROM gross_profit_margin_data g1
INNER JOIN gross_profit_margin_data g3
  ON g1.date = g3.date
  AND g1.main_department = g3.main_department
  AND g1.secondary_department = g3.secondary_department
WHERE g1.secondary_category = '本年実績(%)'
  AND g3.secondary_category = '本年目標(%)';

-- ステップ5: 経常利益の累積比率データ
CREATE TEMP TABLE recurring_profit_ratio AS
WITH base AS (
  SELECT
    c1.date,
    c1.date_sort_key,
    c1.date_label,
    c1.fiscal_year,
    c1.fiscal_month,
    c1.main_category,
    c1.main_category_sort_order,
    c1.main_department,
    c1.main_department_sort_order,
    c1.secondary_department,
    c1.secondary_department_sort_order,
    c1.main_display_flag,
    c1.cumulative_value AS cumulative_actual,
    c2.cumulative_value AS cumulative_target,
    c3.cumulative_value AS cumulative_prev_year
  FROM cumulative_amount c1
  LEFT JOIN cumulative_amount c2
    ON c1.date = c2.date
    AND c1.fiscal_year = c2.fiscal_year
    AND c1.main_department = c2.main_department
    AND c1.secondary_department = c2.secondary_department
    AND c1.main_category = c2.main_category
    AND c2.secondary_category = '累積本年目標(千円)'
  LEFT JOIN cumulative_amount c3
    ON c1.date = c3.date
    AND c1.fiscal_year = c3.fiscal_year
    AND c1.main_department = c3.main_department
    AND c1.secondary_department = c3.secondary_department
    AND c1.main_category = c3.main_category
    AND c3.secondary_category = '前年実績(千円)'
  WHERE c1.main_category = '経常利益'
    AND c1.secondary_category = '累積本年実績(千円)'
)
-- 累積目標比
SELECT
  date, date_sort_key, date_label, fiscal_year, fiscal_month,
  main_category, main_category_sort_order,
  '累積目標比(%)' AS secondary_category,
  5 AS secondary_category_sort_order,
  main_department, main_department_sort_order,
  secondary_department, secondary_department_sort_order,
  main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(cumulative_actual, cumulative_target) AS cumulative_value
FROM base
UNION ALL
-- 累積前年比
SELECT
  date, date_sort_key, date_label, fiscal_year, fiscal_month,
  main_category, main_category_sort_order,
  '累積前年比(%)' AS secondary_category,
  7 AS secondary_category_sort_order,
  main_department, main_department_sort_order,
  secondary_department, secondary_department_sort_order,
  main_display_flag,
  CAST(NULL AS FLOAT64) AS monthly_value,
  SAFE_DIVIDE(cumulative_actual, cumulative_prev_year) AS cumulative_value
FROM base;

-- ステップ6: 経常利益の累積前年実績
CREATE TEMP TABLE recurring_profit_prev_year AS
SELECT
  date, date_sort_key, date_label, fiscal_year, fiscal_month,
  main_category, main_category_sort_order,
  '累積前年実績(千円)' AS secondary_category,
  6 AS secondary_category_sort_order,
  main_department, main_department_sort_order,
  secondary_department, secondary_department_sort_order,
  main_display_flag, monthly_value, cumulative_value
FROM cumulative_amount
WHERE main_category = '経常利益'
  AND secondary_category = '前年実績(千円)';

-- ステップ7: 最終テーブル作成
CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all` AS
SELECT * FROM cumulative_amount
UNION ALL
SELECT * FROM gross_profit_margin_data
UNION ALL
SELECT * FROM ratio_data
UNION ALL
SELECT * FROM gross_margin_ratio
UNION ALL
SELECT * FROM recurring_profit_ratio
UNION ALL
SELECT * FROM recurring_profit_prev_year
ORDER BY
  date_sort_key,
  main_department_sort_order,
  secondary_department_sort_order,
  main_category_sort_order,
  secondary_category_sort_order;
