/*
============================================================
DataMart: 経営資料（当月）ダッシュボード用SQL（縦持ち形式） - 福岡支店版
============================================================
目的: 月次損益計算書を組織階層別に可視化（Looker Studio用縦持ち出力）
対象データ: 前月実績データ（CURRENT_DATEから自動計算）
組織階層: 福岡支店計 > 工事営業部計/硝子建材営業部計 > 部門別

出力スキーマ:
  - date: 対象月（DATE型）
  - main_category: 大項目（売上高、売上総利益など）
  - secondary_category: 小項目（前年実績、本年目標、本年実績、またはNULL）
  - main_department: 最上位部門（福岡支店）
  - secondary_department: 詳細部門（福岡支店計、工事営業部計、ガラス工事など）
  - value: 集計値

データソース:
  - DWHテーブル（福岡支店用: dwh_sales_actual_prev_year` WHERE branch = '福岡支店', operating_expenses` WHERE branch = '福岡支店' など）

注意事項:
  - 金額は円単位でDBに格納、Looker Studioで千円表示
  - 売上総利益率は小数（0.3 = 30%）で格納
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_fukuoka` AS
WITH
-- ============================================================
-- 中間テーブルから集計済みメトリクスを読み込み
-- ============================================================
aggregated_metrics AS (
  SELECT *
  FROM `data-platform-prod-475201.corporate_data_dwh.aggregated_metrics_all_branches`
  WHERE branch = '福岡支店'
),
-- ============================================================
-- 経常利益の累積計算（期首4/1から当月まで）
-- ============================================================
cumulative_recurring_profit AS (
  WITH
  -- 全組織×detail_category×月の組み合わせを取得
  org_categories_months AS (
    SELECT DISTINCT year_month, detail_category
    FROM aggregated_metrics
    WHERE recurring_profit_actual IS NOT NULL
  ),

  -- 期首を月ごとに計算（期首は4/1）
  fiscal_year_starts AS (
    SELECT DISTINCT
      year_month,
      CASE
        WHEN EXTRACT(MONTH FROM year_month) >= 4
        THEN DATE(EXTRACT(YEAR FROM year_month), 4, 1)
        ELSE DATE(EXTRACT(YEAR FROM year_month) - 1, 4, 1)
      END AS fiscal_start_date
    FROM org_categories_months
  )

  -- 累積計算（各月ごとに期首から当月までの累積）
  SELECT
    am_target.year_month,
    am_target.detail_category,
    SUM(am_source.recurring_profit_actual) AS cumulative_actual,
    -- 目標も累積
    (SELECT SUM(recurring_profit_target)
     FROM aggregated_metrics am_inner
     CROSS JOIN fiscal_year_starts fys_inner
     WHERE am_inner.detail_category = am_target.detail_category
     AND am_inner.year_month >= fys_inner.fiscal_start_date
     AND am_inner.year_month <= am_target.year_month
     AND fys_inner.year_month = am_target.year_month) AS cumulative_target
  FROM aggregated_metrics am_target
  CROSS JOIN fiscal_year_starts fys
  LEFT JOIN aggregated_metrics am_source
    ON am_target.detail_category = am_source.detail_category
    AND am_source.year_month >= fys.fiscal_start_date
    AND am_source.year_month <= am_target.year_month
  WHERE fys.year_month = am_target.year_month
    AND am_target.recurring_profit_actual IS NOT NULL
  GROUP BY am_target.year_month, am_target.detail_category
),
-- ============================================================
-- 11. 縦持ち形式への変換（UNION ALL）
-- ============================================================
vertical_format AS (
  -- 売上高: 前年実績
  SELECT
    year_month AS date,
    '売上高' AS main_category,
    1 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    1 AS secondary_category_sort_order,
    '福岡支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END AS secondary_department_sort_order,
    sales_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 本年目標
  SELECT
    year_month,
    '売上高',
    1,
    '本年目標',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    sales_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 本年実績
  SELECT
    year_month,
    '売上高',
    1,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    sales_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 前年比
  SELECT
    year_month,
    '売上高',
    1,
    '前年比(%)',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(sales_prev_year, 0) IS NULL THEN NULL
      ELSE sales_actual / sales_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 目標比
  SELECT
    year_month,
    '売上高',
    1,
    '目標比(%)',
    5,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(sales_target, 0) IS NULL THEN NULL
      ELSE sales_actual / sales_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益: 前年実績
  SELECT
    year_month,
    '売上総利益',
    2,
    '前年実績',
    1,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_prev_year
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 本年目標
  SELECT
    year_month,
    '売上総利益',
    2,
    '本年目標',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 本年実績
  SELECT
    year_month,
    '売上総利益',
    2,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 前年比
  SELECT
    year_month,
    '売上総利益',
    2,
    '前年比(%)',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(gross_profit_prev_year, 0) IS NULL THEN NULL
      ELSE gross_profit_actual / gross_profit_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 目標比
  SELECT
    year_month,
    '売上総利益',
    2,
    '目標比(%)',
    5,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(gross_profit_target, 0) IS NULL THEN NULL
      ELSE gross_profit_actual / gross_profit_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益率: 前年実績
  SELECT
    year_month,
    '売上総利益率',
    3,
    '前年実績',
    1,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_margin_prev_year
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 本年目標
  SELECT
    year_month,
    '売上総利益率',
    3,
    '本年目標',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_margin_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 本年実績
  SELECT
    year_month,
    '売上総利益率',
    3,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    gross_profit_margin_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 前年比
  SELECT
    year_month,
    '売上総利益率',
    3,
    '前年比(%)',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(gross_profit_margin_prev_year, 0) IS NULL THEN NULL
      ELSE gross_profit_margin_actual / gross_profit_margin_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 目標比
  SELECT
    year_month,
    '売上総利益率',
    3,
    '目標比(%)',
    5,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(gross_profit_margin_target, 0) IS NULL THEN NULL
      ELSE gross_profit_margin_actual / gross_profit_margin_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業経費: 本年目標
  SELECT
    year_month,
    '営業経費',
    4,
    '本年目標',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_expense_target
  FROM aggregated_metrics
  WHERE operating_expense_target IS NOT NULL
  UNION ALL
  -- 営業経費: 前年実績
  SELECT
    year_month,
    '営業経費',
    4,
    '前年実績',
    1,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_expense_prev_year
  FROM aggregated_metrics

  UNION ALL

  -- 営業経費: 本年実績
  SELECT
    year_month,
    '営業経費',
    4,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_expense_actual
  FROM aggregated_metrics
  UNION ALL
  -- 営業経費: 目標比
  SELECT
    year_month,
    '営業経費',
    4,
    '目標比(%)',
    5,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(operating_expense_target, 0) IS NULL THEN NULL
      ELSE operating_expense_actual / operating_expense_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業経費: 前年比
  SELECT
    year_month,
    '営業経費',
    4,
    '前年比(%)',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(operating_expense_prev_year, 0) IS NULL THEN NULL
      ELSE operating_expense_actual / operating_expense_prev_year
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益: 前年実績
  SELECT
    year_month,
    '営業利益',
    5,
    '前年実績',
    1,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_income_prev_year
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益: 本年目標
  SELECT
    year_month,
    '営業利益',
    5,
    '本年目標',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_income_target
  FROM aggregated_metrics
  WHERE operating_income_target IS NOT NULL
  UNION ALL
  -- 営業利益: 本年実績
  SELECT
    year_month,
    '営業利益',
    5,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    operating_income_actual
  FROM aggregated_metrics
  UNION ALL
  -- 営業利益: 目標比
  SELECT
    year_month,
    '営業利益',
    5,
    '目標比(%)',
    5,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(operating_income_target, 0) IS NULL THEN NULL
      ELSE operating_income_actual / operating_income_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益: 前年比
  SELECT
    year_month,
    '営業利益',
    5,
    '前年比(%)',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    CASE
      WHEN NULLIF(operating_income_prev_year, 0) IS NULL THEN NULL
      ELSE operating_income_actual / operating_income_prev_year
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（リベート）: 本年実績のみ
  SELECT
    year_month,
    '営業外収入（リベート）',
    6,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    rebate_income
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（その他）: 本年実績のみ
  SELECT
    year_month,
    '営業外収入（その他）',
    7,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    other_non_operating_income
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（社内利息A・B）: 本年実績のみ
  SELECT
    year_month,
    '営業外費用（社内利息A・B）',
    8,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    non_operating_expenses
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（雑損失）: 本年実績のみ
  SELECT
    year_month,
    '営業外費用（雑損失）',
    9,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    miscellaneous_loss
  FROM aggregated_metrics

  UNION ALL

  -- 本店管理費: 本年実績のみ
  SELECT
    year_month,
    '本店管理費',
    10,
    '本年実績',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    head_office_expense
  FROM aggregated_metrics

  UNION ALL

  -- 経常利益: 本年目標
  SELECT
    year_month,
    '経常利益',
    11,
    '本年目標',
    1,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    recurring_profit_target
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 本年実績
  SELECT
    year_month,
    '経常利益',
    11,
    '本年実績',
    2,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    recurring_profit_actual
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 累積本年目標（期首4/1からの累積）
  SELECT
    year_month,
    '経常利益',
    11,
    '累積本年目標',
    3,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    COALESCE(cumulative_target, 0)
  FROM cumulative_recurring_profit
  WHERE cumulative_target IS NOT NULL
  UNION ALL
  -- 経常利益: 累積本年実績（期首4/1からの累積）
  SELECT
    year_month,
    '経常利益',
    11,
    '累積本年実績',
    4,
    '福岡支店',
    detail_category,
    CASE detail_category
      WHEN '福岡支店計' THEN 100
      WHEN '工事部計' THEN 101
      WHEN '硝子工事' THEN 102
      WHEN 'ビルサッシ' THEN 103
      WHEN '内装工事' THEN 104
      WHEN '硝子樹脂部計' THEN 105
      WHEN '硝子' THEN 106
      WHEN '建材' THEN 107
      WHEN '樹脂' THEN 108
      WHEN 'GSセンター' THEN 109
      WHEN '福北センター' THEN 110
      ELSE 199
    END,
    COALESCE(cumulative_actual, 0)
  FROM cumulative_recurring_profit
)

SELECT
  date,
  main_category,
  main_category_sort_order,
  -- secondary_categoryに(千円)または(%)を付加
  CASE
    -- 金額項目に(千円)を付加
    WHEN NOT REGEXP_CONTAINS(secondary_category, r'\(%\)')
      THEN CONCAT(secondary_category, '(千円)')
    ELSE secondary_category
  END AS secondary_category,
  -- secondary_category_graphnameから(千円)と(%)を除外
  REGEXP_REPLACE(
    CASE
      -- 金額項目に(千円)を付加
      WHEN NOT REGEXP_CONTAINS(secondary_category, r'\(%\)')
        THEN CONCAT(secondary_category, '(千円)')
      ELSE secondary_category
    END,
    r'\(千円\)|\(%\)',
    ''
  ) AS secondary_category_graphname,
  secondary_category_sort_order,
  main_department,
  3 AS main_department_sort_order,  -- 福岡支店=3
  secondary_department,
  -- secondary_department_newlineに改行コードを挿入
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    secondary_department,
    '佐々木（大成・鹿島他）', '佐々木\n（大成・鹿島他）'),
    '浅井（清水他）', '浅井\n（清水他）'),
    '小笠原（三井住友他）', '小笠原\n（三井住友他）'),
    '高石（内装・リニューアル）', '高石\n（内装・リニューアル）'),
    '岡本（清水他）', '岡本\n（清水他）'),
    '山本（改装）', '山本\n（改装）')
  AS secondary_department_newline,
  secondary_department_sort_order,
  value,
  -- display_valueの計算
  CASE
    -- 利益率（売上総利益率など）は小数で格納されているので100倍してパーセント表示
    WHEN REGEXP_CONTAINS(main_category, r'(利益率|粗利率|営業利益率)')
      AND NOT REGEXP_CONTAINS(secondary_category, r'(目標比|前年比)\(%\)')
      THEN value * 100
    -- 目標比と前年比は比率（倍率）で格納されているので100倍してパーセント表示
    -- （例: value=1.5 → 150%と表示）
    WHEN REGEXP_CONTAINS(secondary_category, r'(目標比|前年比)\(%\)') THEN value * 100
    -- 千円表記の項目（1/1000倍して四捨五入）
    WHEN main_category != '売上総利益率'
      AND NOT REGEXP_CONTAINS(secondary_category, r'\(%\)')
      THEN ROUND(value / 1000, 0)
    ELSE value
  END AS display_value
FROM vertical_format;
