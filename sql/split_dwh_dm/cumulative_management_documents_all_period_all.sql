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

-- ============================================================
-- 対象データの抽出（金額系項目）
-- 売上総利益率を除く全main_category × 金額系secondary_category
-- ============================================================
amount_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    -- secondary_categoryの表示名（千円単位を明示）
    CASE secondary_category
      WHEN '本年実績(千円)' THEN '本年実績(千円)'
      WHEN '前年実績(千円)' THEN '前年実績(千円)'
      WHEN '本年目標(千円)' THEN '本年目標(千円)'
      WHEN '累積本年実績(千円)' THEN '累積本年実績(千円)'
      WHEN '累積本年目標(千円)' THEN '累積本年目標(千円)'
      ELSE secondary_category
    END AS secondary_category,
    -- secondary_category_sort_order
    -- 経常利益は特別な順序: 本年目標→本年実績→累積本年目標→累積本年実績
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
      -- 通常項目: 本年実績、前年実績、本年目標
      secondary_category IN ('本年実績(千円)', '前年実績(千円)', '本年目標(千円)')
      -- 経常利益のみ: 累積本年実績、累積本年目標
      OR (main_category = '経常利益' AND secondary_category IN ('累積本年実績(千円)', '累積本年目標(千円)'))
    )
),

-- ============================================================
-- 累計計算（金額系）
-- ============================================================
cumulative_amount AS (
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
    -- 累計値の計算
    SUM(monthly_value) OVER (
      PARTITION BY fiscal_year, main_department, secondary_department, main_category, secondary_category
      ORDER BY fiscal_month
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_value
  FROM amount_data
),

-- ============================================================
-- 売上総利益率の計算用データ（売上高と売上総利益の累計を取得）
-- ============================================================
gross_profit_margin_base AS (
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
    c1.main_display_flag,
    c1.secondary_category,
    c1.secondary_category_sort_order,
    c1.cumulative_value AS cumulative_sales,
    c2.cumulative_value AS cumulative_gross_profit
  FROM cumulative_amount c1
  INNER JOIN cumulative_amount c2
    ON c1.date = c2.date
    AND c1.fiscal_year = c2.fiscal_year
    AND c1.main_department = c2.main_department
    AND c1.secondary_department = c2.secondary_department
    AND c1.secondary_category = c2.secondary_category
  WHERE c1.main_category = '売上高'
    AND c2.main_category = '売上総利益'
    AND c1.secondary_category IN ('本年実績(千円)', '前年実績(千円)', '本年目標(千円)')
),

-- ============================================================
-- 売上総利益率データの生成
-- ============================================================
gross_profit_margin_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,  -- management_documents_all_period_allと同じ
    -- 売上総利益率のsecondary_categoryは(%)を付ける
    CASE secondary_category
      WHEN '本年実績(千円)' THEN '本年実績(%)'
      WHEN '前年実績(千円)' THEN '前年実績(%)'
      WHEN '本年目標(千円)' THEN '本年目標(%)'
    END AS secondary_category,
    secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    -- 当月の売上総利益率は計算しない（累計のみ意味がある）
    NULL AS monthly_value,
    -- 累計売上総利益率 = 累計売上総利益 ÷ 累計売上高
    SAFE_DIVIDE(cumulative_gross_profit, cumulative_sales) AS cumulative_value
  FROM gross_profit_margin_base
),

-- ============================================================
-- 前年比の計算用データ
-- ============================================================
yoy_ratio_base AS (
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
    c1.cumulative_value AS cumulative_current_year,
    c2.cumulative_value AS cumulative_prev_year
  FROM cumulative_amount c1
  INNER JOIN cumulative_amount c2
    ON c1.date = c2.date
    AND c1.fiscal_year = c2.fiscal_year
    AND c1.main_department = c2.main_department
    AND c1.secondary_department = c2.secondary_department
    AND c1.main_category = c2.main_category
  WHERE c1.secondary_category = '本年実績(千円)'
    AND c2.secondary_category = '前年実績(千円)'
    AND c1.main_category NOT IN ('売上総利益率')
),

-- ============================================================
-- 前年比データの生成
-- ============================================================
yoy_ratio_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    '前年比(%)' AS secondary_category,
    4 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 累計前年比 = 累計本年実績 ÷ 累計前年実績
    SAFE_DIVIDE(cumulative_current_year, cumulative_prev_year) AS cumulative_value
  FROM yoy_ratio_base
  WHERE main_category != '経常利益'
),

-- ============================================================
-- 目標比の計算用データ
-- ============================================================
target_ratio_base AS (
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
    c1.cumulative_value AS cumulative_current_year,
    c2.cumulative_value AS cumulative_target
  FROM cumulative_amount c1
  INNER JOIN cumulative_amount c2
    ON c1.date = c2.date
    AND c1.fiscal_year = c2.fiscal_year
    AND c1.main_department = c2.main_department
    AND c1.secondary_department = c2.secondary_department
    AND c1.main_category = c2.main_category
  WHERE c1.secondary_category = '本年実績(千円)'
    AND c2.secondary_category = '本年目標(千円)'
    AND c1.main_category NOT IN ('売上総利益率')
),

-- ============================================================
-- 目標比データの生成（経常利益は除外）
-- ============================================================
target_ratio_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    '目標比(%)' AS secondary_category,
    5 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 累計目標比 = 累計本年実績 ÷ 累計本年目標
    SAFE_DIVIDE(cumulative_current_year, cumulative_target) AS cumulative_value
  FROM target_ratio_base
  WHERE main_category != '経常利益'
),

-- ============================================================
-- 売上総利益率の前年比・目標比計算用データ
-- ============================================================
gross_margin_ratio_base AS (
  SELECT
    g1.date,
    g1.date_sort_key,
    g1.date_label,
    g1.fiscal_year,
    g1.fiscal_month,
    g1.main_department,
    g1.main_department_sort_order,
    g1.secondary_department,
    g1.secondary_department_sort_order,
    g1.main_display_flag,
    g1.cumulative_value AS current_year_margin,
    g2.cumulative_value AS prev_year_margin,
    g3.cumulative_value AS target_margin
  FROM gross_profit_margin_data g1
  LEFT JOIN gross_profit_margin_data g2
    ON g1.date = g2.date
    AND g1.main_department = g2.main_department
    AND g1.secondary_department = g2.secondary_department
    AND g2.secondary_category = '前年実績(%)'
  LEFT JOIN gross_profit_margin_data g3
    ON g1.date = g3.date
    AND g1.main_department = g3.main_department
    AND g1.secondary_department = g3.secondary_department
    AND g3.secondary_category = '本年目標(%)'
  WHERE g1.secondary_category = '本年実績(%)'
),

-- ============================================================
-- 売上総利益率の前年比データ
-- ============================================================
gross_margin_yoy_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,
    '前年比(%)' AS secondary_category,
    4 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 売上総利益率の前年比 = 本年売上総利益率 ÷ 前年売上総利益率
    SAFE_DIVIDE(current_year_margin, prev_year_margin) AS cumulative_value
  FROM gross_margin_ratio_base
  WHERE prev_year_margin IS NOT NULL
),

-- ============================================================
-- 売上総利益率の目標比データ
-- ============================================================
gross_margin_target_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,
    '目標比(%)' AS secondary_category,
    5 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 売上総利益率の目標比 = 本年売上総利益率 ÷ 目標売上総利益率
    SAFE_DIVIDE(current_year_margin, target_margin) AS cumulative_value
  FROM gross_margin_ratio_base
  WHERE target_margin IS NOT NULL
),

-- ============================================================
-- 経常利益の累積目標比・前年比計算用データ
-- 経常利益は累積本年実績(千円)と累積本年目標(千円)がソースにあるため
-- それらの累積値から比率を計算する
-- ============================================================
recurring_profit_ratio_base AS (
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
),

-- ============================================================
-- 経常利益の累積目標比データ
-- ============================================================
recurring_profit_target_ratio_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    '累積目標比(%)' AS secondary_category,
    5 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 累積目標比 = 累積本年実績 ÷ 累積本年目標
    SAFE_DIVIDE(cumulative_actual, cumulative_target) AS cumulative_value
  FROM recurring_profit_ratio_base
),

-- ============================================================
-- 経常利益の累積前年比データ
-- ============================================================
recurring_profit_yoy_ratio_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    '累積前年比(%)' AS secondary_category,
    7 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    NULL AS monthly_value,
    -- 累積前年比 = 累積本年実績 ÷ 累積前年実績
    SAFE_DIVIDE(cumulative_actual, cumulative_prev_year) AS cumulative_value
  FROM recurring_profit_ratio_base
),

-- ============================================================
-- 経常利益の累積前年実績データ
-- 前年実績(千円)の累積値を「累積前年実績(千円)」として出力
-- ============================================================
recurring_profit_prev_year_data AS (
  SELECT
    date,
    date_sort_key,
    date_label,
    fiscal_year,
    fiscal_month,
    main_category,
    main_category_sort_order,
    '累積前年実績(千円)' AS secondary_category,
    6 AS secondary_category_sort_order,
    main_department,
    main_department_sort_order,
    secondary_department,
    secondary_department_sort_order,
    main_display_flag,
    monthly_value,
    cumulative_value
  FROM cumulative_amount
  WHERE main_category = '経常利益'
    AND secondary_category = '前年実績(千円)'
)

-- ============================================================
-- 最終出力（全データを統合）
-- ============================================================
SELECT * FROM cumulative_amount
UNION ALL
SELECT * FROM gross_profit_margin_data
UNION ALL
SELECT * FROM yoy_ratio_data
UNION ALL
SELECT * FROM target_ratio_data
UNION ALL
SELECT * FROM gross_margin_yoy_data
UNION ALL
SELECT * FROM gross_margin_target_data
UNION ALL
SELECT * FROM recurring_profit_target_ratio_data
UNION ALL
SELECT * FROM recurring_profit_yoy_ratio_data
UNION ALL
SELECT * FROM recurring_profit_prev_year_data

ORDER BY
  date_sort_key,
  main_department_sort_order,
  secondary_department_sort_order,
  main_category_sort_order,
  secondary_category_sort_order;
