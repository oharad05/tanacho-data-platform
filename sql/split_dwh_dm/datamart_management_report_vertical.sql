/*
============================================================
DataMart: 経営資料（当月）ダッシュボード用SQL（縦持ち形式）
============================================================
目的: 月次損益計算書を組織階層別に可視化（Looker Studio用縦持ち出力）
対象データ: 前月実績データ（CURRENT_DATEから自動計算）
組織階層: 東京支店計 > 工事営業部計/硝子建材営業部 > 担当者別/部門別

出力スキーマ:
  - date: 対象月（DATE型）
  - main_category: 大項目（売上高、売上総利益など）
  - secondary_category: 小項目（前年実績、本年目標、本年実績、またはNULL）
  - main_department: 最上位部門（東京支店）
  - secondary_department: 詳細部門（東京支店計、工事営業部計、佐々木（大成・鹿島他）など）
  - value: 集計値

データソース:
  - DWHテーブル（dwh_sales_actual, dwh_sales_target, dwh_operating_expenses など）

注意事項:
  - 金額は円単位でDBに格納、Looker Studioで千円表示
  - 売上総利益率は小数（0.3 = 30%）で格納
============================================================
*/

DECLARE target_month DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH);

WITH
-- ============================================================
-- DWHテーブルからデータを読み込み
-- ============================================================

-- 1. 売上高・粗利実績（本年実績）
sales_actual AS (
  SELECT
    year_month,
    organization,
    detail_category,
    sales_amount,
    gross_profit_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.sales_actual`
),

-- 1-2. 売上高・粗利実績（前年実績）
sales_actual_prev_year AS (
  SELECT
    year_month,
    organization,
    detail_category,
    sales_amount,
    gross_profit_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.sales_actual_prev_year`
),

-- 2. 売上高・粗利目標
sales_target AS (
  SELECT
    year_month,
    metric_type,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.sales_target`
),

-- 3. 営業経費
operating_expenses AS (
  SELECT
    year_month,
    detail_category,
    operating_expense_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
),

-- 4. 営業外収入（リベート・その他）
non_operating_income AS (
  SELECT
    year_month,
    detail_category,
    rebate_income,
    other_non_operating_income
  FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
),

-- 5. 営業外費用（社内利息）
non_operating_expenses AS (
  SELECT
    year_month,
    detail_category,
    interest_expense
  FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses`
),

-- 6. 営業外費用（雑損失）
miscellaneous_loss AS (
  SELECT
    year_month,
    detail_category,
    miscellaneous_loss_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss`
),

-- 7. 本店管理費
head_office_expenses AS (
  SELECT
    year_month,
    detail_category,
    head_office_expense
  FROM `data-platform-prod-475201.corporate_data_dwh.head_office_expenses`
),

-- 8. 経常利益目標
recurring_profit_target AS (
  SELECT
    year_month,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.recurring_profit_target`
),

-- ============================================================
-- 経費データの統合
-- ============================================================
expense_data AS (
  SELECT
    oe.detail_category,
    oe.operating_expense_amount AS operating_expense,
    noi.rebate_income,
    noi.other_non_operating_income AS other_income,
    noe.interest_expense,
    ml.miscellaneous_loss_amount AS misc_loss,
    hoe.head_office_expense AS hq_expense
  FROM operating_expenses oe
  LEFT JOIN non_operating_income noi ON oe.detail_category = noi.detail_category
  LEFT JOIN non_operating_expenses noe ON oe.detail_category = noe.detail_category
  LEFT JOIN miscellaneous_loss ml ON oe.detail_category = ml.detail_category
  LEFT JOIN head_office_expenses hoe ON oe.detail_category = hoe.detail_category
),


-- ============================================================
-- 9. 全指標の統合
-- ============================================================
consolidated_metrics AS (
  SELECT
    sa.organization,
    sa.detail_category,

    -- ========== 売上高 ==========
    sa.sales_amount AS sales_actual,  -- 本年実績
    st_sales.target_amount AS sales_target,  -- 本年目標
    COALESCE(sa_prev.sales_amount, 0) AS sales_prev_year,  -- 前年実績

    -- ========== 売上総利益 ==========
    sa.gross_profit_amount AS gross_profit_actual,  -- 本年実績
    st_gp.target_amount AS gross_profit_target,  -- 本年目標
    COALESCE(sa_prev.gross_profit_amount, 0) AS gross_profit_prev_year,  -- 前年実績

    -- ========== 売上総利益率 ==========
    SAFE_DIVIDE(sa.gross_profit_amount, sa.sales_amount) AS gross_profit_margin_actual,
    SAFE_DIVIDE(st_gp.target_amount, st_sales.target_amount) AS gross_profit_margin_target,
    SAFE_DIVIDE(sa_prev.gross_profit_amount, sa_prev.sales_amount) AS gross_profit_margin_prev_year,

    -- ========== 営業経費 ==========
    CAST(NULL AS FLOAT64) AS operating_expense_actual,  -- 個人レベルには営業経費データなし
    CAST(NULL AS FLOAT64) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,

    -- ========== 営業利益 ==========
    CAST(NULL AS FLOAT64) AS operating_income_actual,  -- 後で集計レベルで計算
    CAST(NULL AS FLOAT64) AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,

    -- ========== 営業外収入 ==========
    CAST(NULL AS FLOAT64) AS rebate_income,
    CAST(NULL AS FLOAT64) AS other_non_operating_income,

    -- ========== 営業外費用 ==========
    CAST(NULL AS FLOAT64) AS non_operating_expenses,
    CAST(NULL AS FLOAT64) AS miscellaneous_loss,

    -- ========== 本店管理費 ==========
    CAST(NULL AS FLOAT64) AS head_office_expense,

    -- ========== 経常利益 ==========
    CAST(NULL AS FLOAT64) AS recurring_profit_actual,
    rpt.target_amount AS recurring_profit_target

  FROM sales_actual sa
  LEFT JOIN sales_actual_prev_year sa_prev
    ON sa.organization = sa_prev.organization
    AND sa.detail_category = sa_prev.detail_category
  LEFT JOIN sales_target st_sales
    ON sa.organization = st_sales.organization
    AND sa.detail_category = st_sales.detail_category
    AND st_sales.metric_type = 'sales'
  LEFT JOIN sales_target st_gp
    ON sa.organization = st_gp.organization
    AND sa.detail_category = st_gp.detail_category
    AND st_gp.metric_type = 'gross_profit'
  LEFT JOIN recurring_profit_target rpt
    ON sa.organization = rpt.organization
    AND sa.detail_category = rpt.detail_category
),


-- ============================================================
-- 10. 組織階層の集計（工事営業部計、東京支店計）
-- ============================================================
aggregated_metrics AS (
  -- 詳細レベル（担当者別・部門別）
  SELECT *
  FROM consolidated_metrics

  UNION ALL

  -- 中間レベル（ガラス工事計 = 佐々木+岡本+小笠原+高石）
  SELECT
    cm.organization,
    'ガラス工事計' AS detail_category,
    -- ========== 売上・粗利は個人データを集計 ==========
    SUM(cm.sales_actual) AS sales_actual,
    SUM(cm.sales_target) AS sales_target,
    SUM(cm.sales_prev_year) AS sales_prev_year,
    SUM(cm.gross_profit_actual) AS gross_profit_actual,
    SUM(cm.gross_profit_target) AS gross_profit_target,
    SUM(cm.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)) AS gross_profit_margin_prev_year,
    -- ========== 経費はexpense_dataから直接取得 ==========
    MAX(ed.operating_expense) AS operating_expense_actual,
    CAST(NULL AS FLOAT64) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    -- 営業利益の再計算
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    CAST(NULL AS FLOAT64) AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,
    -- 営業外収入
    MAX(ed.rebate_income) AS rebate_income,
    MAX(ed.other_income) AS other_non_operating_income,
    -- 営業外費用
    MAX(ed.interest_expense) AS non_operating_expenses,
    MAX(ed.misc_loss) AS miscellaneous_loss,
    -- 本店管理費
    MAX(ed.hq_expense) AS head_office_expense,
    -- 経常利益の再計算
    (
      SUM(cm.gross_profit_actual)
      - COALESCE(MAX(ed.operating_expense), 0)
      + COALESCE(MAX(ed.rebate_income), 0)
      + COALESCE(MAX(ed.other_income), 0)
      - COALESCE(MAX(ed.interest_expense), 0)
      - COALESCE(MAX(ed.misc_loss), 0)
      - COALESCE(MAX(ed.hq_expense), 0)
    ) AS recurring_profit_actual,
    MAX(cm.recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics cm
  CROSS JOIN (SELECT * FROM expense_data WHERE detail_category = 'ガラス工事計') ed
  WHERE cm.organization = '工事営業部'
    AND cm.detail_category IN ('佐々木（大成・鹿島他）', '岡本（清水他）', '小笠原（三井住友他）', '高石（内装・リニューアル）')
  GROUP BY cm.organization

  UNION ALL

  -- 組織計レベル（工事営業部計）
  SELECT
    cm.organization,
    CONCAT(cm.organization, '計') AS detail_category,
    -- ========== 売上・粗利は個人/部門データを集計 ==========
    SUM(cm.sales_actual) AS sales_actual,
    SUM(cm.sales_target) AS sales_target,
    SUM(cm.sales_prev_year) AS sales_prev_year,
    SUM(cm.gross_profit_actual) AS gross_profit_actual,
    SUM(cm.gross_profit_target) AS gross_profit_target,
    SUM(cm.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)) AS gross_profit_margin_prev_year,
    -- ========== 経費はexpense_dataから集計（ガラス工事計 + 山本（改装）） ==========
    SUM(COALESCE(ed.operating_expense, 0)) AS operating_expense_actual,
    CAST(NULL AS FLOAT64) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - SUM(COALESCE(ed.operating_expense, 0)) AS operating_income_actual,
    CAST(NULL AS FLOAT64) AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,
    SUM(COALESCE(ed.rebate_income, 0)) AS rebate_income,
    SUM(COALESCE(ed.other_income, 0)) AS other_non_operating_income,
    SUM(COALESCE(ed.interest_expense, 0)) AS non_operating_expenses,
    SUM(COALESCE(ed.misc_loss, 0)) AS miscellaneous_loss,
    SUM(COALESCE(ed.hq_expense, 0)) AS head_office_expense,
    (
      SUM(cm.gross_profit_actual)
      - SUM(COALESCE(ed.operating_expense, 0))
      + SUM(COALESCE(ed.rebate_income, 0))
      + SUM(COALESCE(ed.other_income, 0))
      - SUM(COALESCE(ed.interest_expense, 0))
      - SUM(COALESCE(ed.misc_loss, 0))
      - SUM(COALESCE(ed.hq_expense, 0))
    ) AS recurring_profit_actual,
    MAX(cm.recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics cm
  LEFT JOIN expense_data ed
    ON ed.detail_category IN ('ガラス工事計', '山本（改装）')
  WHERE cm.organization = '工事営業部'
  GROUP BY cm.organization

  UNION ALL

  -- 組織計レベル（硝子建材営業部計）
  SELECT
    cm.organization,
    CONCAT(cm.organization, '計') AS detail_category,
    -- ========== 売上・粗利は個人/部門データを集計 ==========
    SUM(cm.sales_actual) AS sales_actual,
    SUM(cm.sales_target) AS sales_target,
    SUM(cm.sales_prev_year) AS sales_prev_year,
    SUM(cm.gross_profit_actual) AS gross_profit_actual,
    SUM(cm.gross_profit_target) AS gross_profit_target,
    SUM(cm.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)) AS gross_profit_margin_prev_year,
    -- ========== 経費はexpense_dataから取得（硝子建材営業部のみ） ==========
    MAX(ed.operating_expense) AS operating_expense_actual,
    CAST(NULL AS FLOAT64) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    CAST(NULL AS FLOAT64) AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,
    MAX(ed.rebate_income) AS rebate_income,
    MAX(ed.other_income) AS other_non_operating_income,
    MAX(ed.interest_expense) AS non_operating_expenses,
    MAX(ed.misc_loss) AS miscellaneous_loss,
    MAX(ed.hq_expense) AS head_office_expense,
    (
      SUM(cm.gross_profit_actual)
      - COALESCE(MAX(ed.operating_expense), 0)
      + COALESCE(MAX(ed.rebate_income), 0)
      + COALESCE(MAX(ed.other_income), 0)
      - COALESCE(MAX(ed.interest_expense), 0)
      - COALESCE(MAX(ed.misc_loss), 0)
      - COALESCE(MAX(ed.hq_expense), 0)
    ) AS recurring_profit_actual,
    MAX(cm.recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics cm
  CROSS JOIN (SELECT * FROM expense_data WHERE detail_category = '硝子建材営業部') ed
  WHERE cm.organization = '硝子建材営業部'
  GROUP BY cm.organization

  UNION ALL

  -- 最上位レベル（東京支店計）
  SELECT
    '東京支店' AS organization,
    '東京支店計' AS detail_category,
    -- ========== 売上・粗利は個人/部門データを集計 ==========
    SUM(cm.sales_actual) AS sales_actual,
    SUM(cm.sales_target) AS sales_target,
    SUM(cm.sales_prev_year) AS sales_prev_year,
    SUM(cm.gross_profit_actual) AS gross_profit_actual,
    SUM(cm.gross_profit_target) AS gross_profit_target,
    SUM(cm.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)) AS gross_profit_margin_prev_year,
    -- ========== 経費はexpense_dataから集計（全組織の合計） ==========
    SUM(COALESCE(ed.operating_expense, 0)) AS operating_expense_actual,
    CAST(NULL AS FLOAT64) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - SUM(COALESCE(ed.operating_expense, 0)) AS operating_income_actual,
    CAST(NULL AS FLOAT64) AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,
    SUM(COALESCE(ed.rebate_income, 0)) AS rebate_income,
    SUM(COALESCE(ed.other_income, 0)) AS other_non_operating_income,
    SUM(COALESCE(ed.interest_expense, 0)) AS non_operating_expenses,
    SUM(COALESCE(ed.misc_loss, 0)) AS miscellaneous_loss,
    SUM(COALESCE(ed.hq_expense, 0)) AS head_office_expense,
    (
      SUM(cm.gross_profit_actual)
      - SUM(COALESCE(ed.operating_expense, 0))
      + SUM(COALESCE(ed.rebate_income, 0))
      + SUM(COALESCE(ed.other_income, 0))
      - SUM(COALESCE(ed.interest_expense, 0))
      - SUM(COALESCE(ed.misc_loss, 0))
      - SUM(COALESCE(ed.hq_expense, 0))
    ) AS recurring_profit_actual,
    MAX(cm.recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics cm
  CROSS JOIN expense_data ed
),

-- ============================================================
-- 11. 縦持ち形式への変換（UNION ALL）
-- ============================================================
vertical_format AS (
  -- 売上高: 前年実績
  SELECT
    target_month AS date,
    '売上高' AS main_category,
    1 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    1 AS secondary_category_sort_order,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END AS secondary_department_sort_order,
    sales_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 本年目標
  SELECT
    target_month,
    '売上高',
    1,
    '本年目標',
    2,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    sales_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 本年実績
  SELECT
    target_month,
    '売上高',
    1,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    sales_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 前年比
  SELECT
    target_month,
    '売上高',
    1,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(sales_prev_year, 0) IS NULL THEN NULL
      ELSE sales_actual / sales_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上高: 目標比
  SELECT
    target_month,
    '売上高',
    1,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(sales_target, 0) IS NULL THEN NULL
      ELSE sales_actual / sales_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益: 前年実績
  SELECT
    target_month,
    '売上総利益',
    2,
    '前年実績',
    1,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_prev_year
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 本年目標
  SELECT
    target_month,
    '売上総利益',
    2,
    '本年目標',
    2,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 本年実績
  SELECT
    target_month,
    '売上総利益',
    2,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 前年比
  SELECT
    target_month,
    '売上総利益',
    2,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(gross_profit_prev_year, 0) IS NULL THEN NULL
      ELSE gross_profit_actual / gross_profit_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益: 目標比
  SELECT
    target_month,
    '売上総利益',
    2,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(gross_profit_target, 0) IS NULL THEN NULL
      ELSE gross_profit_actual / gross_profit_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益率: 前年実績
  SELECT
    target_month,
    '売上総利益率(%)',
    3,
    '前年実績',
    1,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_margin_prev_year
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 本年目標
  SELECT
    target_month,
    '売上総利益率(%)',
    3,
    '本年目標',
    2,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_margin_target
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 本年実績
  SELECT
    target_month,
    '売上総利益率(%)',
    3,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    gross_profit_margin_actual
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 前年比
  SELECT
    target_month,
    '売上総利益率(%)',
    3,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(gross_profit_margin_prev_year, 0) IS NULL THEN NULL
      ELSE gross_profit_margin_actual / gross_profit_margin_prev_year
    END
  FROM aggregated_metrics
  UNION ALL
  -- 売上総利益率: 目標比
  SELECT
    target_month,
    '売上総利益率(%)',
    3,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(gross_profit_margin_target, 0) IS NULL THEN NULL
      ELSE gross_profit_margin_actual / gross_profit_margin_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業経費: 本年実績のみ
  SELECT
    target_month,
    '営業経費',
    4,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    operating_expense_actual
  FROM aggregated_metrics
  UNION ALL
  -- 営業経費: 前年比
  SELECT
    target_month,
    '営業経費',
    4,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    NULL  -- 前年実績がないためNULL
  FROM aggregated_metrics
  UNION ALL
  -- 営業経費: 目標比
  SELECT
    target_month,
    '営業経費',
    4,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    NULL  -- 本年目標がないためNULL
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益: 本年実績のみ
  SELECT
    target_month,
    '営業利益',
    5,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    operating_income_actual
  FROM aggregated_metrics
  UNION ALL
  -- 営業利益: 前年比
  SELECT
    target_month,
    '営業利益',
    5,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    NULL  -- 前年実績がないためNULL
  FROM aggregated_metrics
  UNION ALL
  -- 営業利益: 目標比
  SELECT
    target_month,
    '営業利益',
    5,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    NULL  -- 本年目標がないためNULL
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（リベート）: 本年実績のみ
  SELECT
    target_month,
    '営業外収入（リベート）',
    6,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    rebate_income
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（その他）: 本年実績のみ
  SELECT
    target_month,
    '営業外収入（その他）',
    7,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    other_non_operating_income
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（社内利息）: 本年実績のみ
  SELECT
    target_month,
    '営業外費用（社内利息）',
    8,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    non_operating_expenses
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（雑損失）: 本年実績のみ
  SELECT
    target_month,
    '営業外費用（雑損失）',
    9,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    miscellaneous_loss
  FROM aggregated_metrics

  UNION ALL

  -- 本店管理費: 本年実績のみ
  SELECT
    target_month,
    '本店管理費',
    10,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    head_office_expense
  FROM aggregated_metrics

  UNION ALL

  -- 経常利益: 本年目標
  SELECT
    target_month,
    '経常利益',
    11,
    '本年目標',
    2,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    recurring_profit_target
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 本年実績
  SELECT
    target_month,
    '経常利益',
    11,
    '本年実績',
    3,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    recurring_profit_actual
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 前年比
  SELECT
    target_month,
    '経常利益',
    11,
    '前年比',
    4,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    NULL  -- 前年実績がないためNULL
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 目標比
  SELECT
    target_month,
    '経常利益',
    11,
    '目標比',
    5,
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN 'ガラス工事計' THEN 7
      WHEN '山本（改装）' THEN 8
      WHEN '硝子建材営業部計' THEN 9
      WHEN '硝子工事' THEN 10
      WHEN 'ビルサッシ' THEN 11
      WHEN '硝子販売' THEN 12
      WHEN 'サッシ販売' THEN 13
      WHEN 'サッシ完成品' THEN 14
      WHEN 'その他' THEN 15
      ELSE 99
    END,
    CASE
      WHEN NULLIF(recurring_profit_target, 0) IS NULL THEN NULL
      ELSE recurring_profit_actual / recurring_profit_target
    END
  FROM aggregated_metrics
)

SELECT
  *,
  CASE
    WHEN REGEXP_CONTAINS(main_category, r'(利益率|粗利率|営業利益率)') THEN value * 100
    WHEN secondary_category IN ('前年比', '目標比') THEN value * 100
    ELSE value
  END AS display_value
FROM vertical_format;
