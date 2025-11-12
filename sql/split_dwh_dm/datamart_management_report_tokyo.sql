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

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.management_documents_all_period` AS
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
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual`
  WHERE branch = '東京支店'
),

-- 1-2. 売上高・粗利実績（前年実績）
sales_actual_prev_year AS (
  SELECT
    year_month,
    organization,
    detail_category,
    sales_amount,
    gross_profit_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year`
  WHERE branch = '東京支店'
),

-- 2. 売上高・粗利目標
sales_target AS (
  SELECT
    year_month,
    metric_type,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target`
  WHERE branch = '東京支店'
),

-- 3. 営業経費
operating_expenses AS (
  SELECT
    year_month,
    detail_category,
    operating_expense_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
  WHERE branch = '東京支店'
),

-- 4. 営業外収入（リベート・その他）
non_operating_income AS (
  SELECT
    year_month,
    detail_category,
    rebate_income,
    other_non_operating_income
  FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
  WHERE branch = '東京支店'
),

-- 5. 営業外費用（社内利息A・B）
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
  WHERE branch = '東京支店'
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
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target`
  WHERE branch = '東京支店'
),

-- 9. 営業経費目標
operating_expenses_target AS (
  SELECT
    year_month,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target`
  WHERE branch = '東京支店'
),

-- 10. 営業利益目標
operating_income_target AS (
  SELECT
    year_month,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_income_target`
  WHERE branch = '東京支店'
),

-- ============================================================
-- 全組織×カテゴリ×月の組み合わせを生成
-- 売上関連テーブルのみから組み合わせを生成
-- 集計レベル（XX計）は除外して、個人/部門レベルのみを対象とする
-- ============================================================
all_combinations AS (
  SELECT DISTINCT year_month, organization, detail_category
  FROM (
    SELECT year_month, organization, detail_category FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` WHERE branch = '東京支店'
    UNION DISTINCT
    SELECT year_month, organization, detail_category FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` WHERE branch = '東京支店'
    UNION DISTINCT
    SELECT year_month, organization, detail_category FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` WHERE branch = '東京支店'
  )
  WHERE detail_category NOT LIKE '%計'  -- 集計レベルを除外
),

-- ============================================================
-- 11. 経常利益の累積計算（期首から当月まで）
-- ============================================================
cumulative_recurring_profit AS (
  WITH
  -- 全組織×detail_category×月の組み合わせを取得
  org_categories_months AS (
    SELECT DISTINCT year_month, organization, detail_category
    FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual`
    WHERE branch = '東京支店'
  ),

  -- 各月の経常利益実績を計算
  monthly_profit AS (
    SELECT
      sa.year_month,
      sa.organization,
      sa.detail_category,
      -- 売上総利益
      sa.gross_profit_amount
      -- 営業経費
      - COALESCE(oe.operating_expense_amount, 0)
      -- リベート収入
      + COALESCE(ni.rebate_income, 0)
      -- その他営業外収入
      + COALESCE(ni.other_non_operating_income, 0)
      -- 社内利息
      - COALESCE(ne.interest_expense, 0)
      -- 雑損失
      - COALESCE(ml.miscellaneous_loss_amount, 0)
      -- 本店管理費
      - COALESCE(he.head_office_expense, 0)
      AS monthly_recurring_profit
    FROM (SELECT * FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` WHERE branch = '東京支店') sa
    LEFT JOIN (SELECT * FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses` WHERE branch = '東京支店') oe
      ON sa.detail_category = oe.detail_category
      AND sa.year_month = oe.year_month
    LEFT JOIN (SELECT * FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income` WHERE branch = '東京支店') ni
      ON sa.detail_category = ni.detail_category
      AND sa.year_month = ni.year_month
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses` ne
      ON sa.detail_category = ne.detail_category
      AND sa.year_month = ne.year_month
    LEFT JOIN (SELECT * FROM `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` WHERE branch = '東京支店') ml
      ON sa.detail_category = ml.detail_category
      AND sa.year_month = ml.year_month
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` he
      ON sa.detail_category = he.detail_category
      AND sa.year_month = he.year_month
  ),

  -- 期首を月ごとに計算
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
    mp_target.year_month,
    mp_target.organization,
    mp_target.detail_category,
    SUM(mp_source.monthly_recurring_profit) AS cumulative_actual,
    -- 目標も累積
    (SELECT SUM(target_amount)
     FROM recurring_profit_target rpt
     CROSS JOIN fiscal_year_starts fys
     WHERE rpt.organization = mp_target.organization
     AND rpt.detail_category = mp_target.detail_category
     AND rpt.year_month >= fys.fiscal_start_date
     AND rpt.year_month <= mp_target.year_month
     AND fys.year_month = mp_target.year_month) AS cumulative_target
  FROM monthly_profit mp_target
  CROSS JOIN fiscal_year_starts fys
  LEFT JOIN monthly_profit mp_source
    ON mp_target.organization = mp_source.organization
    AND mp_target.detail_category = mp_source.detail_category
    AND mp_source.year_month >= fys.fiscal_start_date
    AND mp_source.year_month <= mp_target.year_month
  WHERE fys.year_month = mp_target.year_month
  GROUP BY mp_target.year_month, mp_target.organization, mp_target.detail_category
),

-- ============================================================
-- 経費データの統合
-- ============================================================
expense_data AS (
  SELECT
    oe.year_month,
    -- parent_organizationを追加（detail_categoryから導出）
    CASE
      WHEN oe.detail_category = 'ガラス工事計' THEN '工事営業部'
      WHEN oe.detail_category = '山本（改装）' THEN '工事営業部'
      WHEN oe.detail_category = '硝子建材営業部' THEN '硝子建材営業部'
      ELSE NULL
    END AS parent_organization,
    oe.detail_category,
    oe.operating_expense_amount AS operating_expense,
    noi.rebate_income,
    noi.other_non_operating_income AS other_income,
    noe.interest_expense,
    ml.miscellaneous_loss_amount AS misc_loss,
    hoe.head_office_expense AS hq_expense
  FROM operating_expenses oe
  LEFT JOIN non_operating_income noi
    ON oe.year_month = noi.year_month
    AND oe.detail_category = noi.detail_category
  LEFT JOIN non_operating_expenses noe
    ON oe.year_month = noe.year_month
    AND oe.detail_category = noe.detail_category
  LEFT JOIN miscellaneous_loss ml
    ON oe.year_month = ml.year_month
    AND oe.detail_category = ml.detail_category
  LEFT JOIN head_office_expenses hoe
    ON oe.year_month = hoe.year_month
    AND oe.detail_category = hoe.detail_category
),


-- ============================================================
-- 9. 全指標の統合
-- ============================================================
consolidated_metrics AS (
  SELECT
    ac.year_month,
    ac.organization,
    ac.detail_category,

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
    oet.target_amount AS operating_expense_target,  -- 本年目標
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,

    -- ========== 営業利益 ==========
    CAST(NULL AS FLOAT64) AS operating_income_actual,  -- 後で集計レベルで計算
    oit.target_amount AS operating_income_target,  -- 本年目標
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

  FROM all_combinations ac
  LEFT JOIN sales_actual sa
    ON ac.year_month = sa.year_month
    AND ac.organization = sa.organization
    AND ac.detail_category = sa.detail_category
  LEFT JOIN sales_actual_prev_year sa_prev
    ON ac.year_month = sa_prev.year_month
    AND ac.organization = sa_prev.organization
    AND ac.detail_category = sa_prev.detail_category
  LEFT JOIN sales_target st_sales
    ON ac.year_month = st_sales.year_month
    AND ac.organization = st_sales.organization
    AND ac.detail_category = st_sales.detail_category
    AND st_sales.metric_type = 'sales'
  LEFT JOIN sales_target st_gp
    ON ac.year_month = st_gp.year_month
    AND ac.organization = st_gp.organization
    AND ac.detail_category = st_gp.detail_category
    AND st_gp.metric_type = 'gross_profit'
  LEFT JOIN recurring_profit_target rpt
    ON ac.year_month = rpt.year_month
    AND ac.organization = rpt.organization
    AND ac.detail_category = rpt.detail_category
  LEFT JOIN operating_expenses_target oet
    ON ac.year_month = oet.year_month
    AND ac.organization = oet.organization
    AND ac.detail_category = oet.detail_category
  LEFT JOIN operating_income_target oit
    ON ac.year_month = oit.year_month
    AND ac.organization = oit.organization
    AND ac.detail_category = oit.detail_category
),


-- ============================================================
-- 10. 組織階層の集計（工事営業部計、東京支店計）
-- ============================================================
aggregated_metrics AS (
  -- 詳細レベル（担当者別・部門別）
  SELECT *
  FROM consolidated_metrics

  UNION ALL

  -- 中間レベル（ガラス工事計 = 佐々木+岡本+小笠原+高石+浅井）
  SELECT
    cm.year_month,
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
    MAX(oet_glass.target_amount) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    -- 営業利益の再計算
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    MAX(oit_glass.target_amount) AS operating_income_target,
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
    MAX(rpt_glass.target_amount) AS recurring_profit_target
  FROM consolidated_metrics cm
  LEFT JOIN expense_data ed
    ON cm.year_month = ed.year_month
    AND cm.organization = ed.parent_organization
    AND ed.detail_category = 'ガラス工事計'
  LEFT JOIN operating_expenses_target oet_glass
    ON cm.year_month = oet_glass.year_month
    AND oet_glass.organization = '工事営業部'
    AND oet_glass.detail_category = 'ガラス工事計'
  LEFT JOIN operating_income_target oit_glass
    ON cm.year_month = oit_glass.year_month
    AND oit_glass.organization = '工事営業部'
    AND oit_glass.detail_category = 'ガラス工事計'
  LEFT JOIN recurring_profit_target rpt_glass
    ON cm.year_month = rpt_glass.year_month
    AND rpt_glass.organization = '工事営業部'
    AND rpt_glass.detail_category = 'ガラス工事計'
  WHERE cm.organization = '工事営業部'
    AND cm.detail_category IN ('佐々木（大成・鹿島他）', '岡本（清水他）', '小笠原（三井住友他）', '高石（内装・リニューアル）', '浅井（清水他）')
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 組織計レベル（工事営業部計）
  SELECT
    cm.year_month,
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
    MAX(ed.operating_expense) AS operating_expense_actual,
    MAX(oet_eng.target_amount) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    MAX(oit_eng.target_amount) AS operating_income_target,
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
    MAX(rpt_eng.target_amount) AS recurring_profit_target
  FROM consolidated_metrics cm
  LEFT JOIN (
    SELECT
      year_month,
      parent_organization,
      SUM(COALESCE(operating_expense, 0)) AS operating_expense,
      SUM(COALESCE(rebate_income, 0)) AS rebate_income,
      SUM(COALESCE(other_income, 0)) AS other_income,
      SUM(COALESCE(interest_expense, 0)) AS interest_expense,
      SUM(COALESCE(misc_loss, 0)) AS misc_loss,
      SUM(COALESCE(hq_expense, 0)) AS hq_expense
    FROM expense_data
    WHERE detail_category IN ('ガラス工事計', '山本（改装）')
    GROUP BY year_month, parent_organization
  ) ed
    ON cm.year_month = ed.year_month
    AND cm.organization = ed.parent_organization
  LEFT JOIN operating_expenses_target oet_eng
    ON cm.year_month = oet_eng.year_month
    AND oet_eng.organization = '工事営業部'
    AND oet_eng.detail_category = '工事営業部計'
  LEFT JOIN operating_income_target oit_eng
    ON cm.year_month = oit_eng.year_month
    AND oit_eng.organization = '工事営業部'
    AND oit_eng.detail_category = '工事営業部計'
  LEFT JOIN recurring_profit_target rpt_eng
    ON cm.year_month = rpt_eng.year_month
    AND rpt_eng.organization = '工事営業部'
    AND rpt_eng.detail_category = '工事営業部計'
  WHERE cm.organization = '工事営業部'
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 組織計レベル（硝子建材営業部計）
  SELECT
    cm.year_month,
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
    MAX(oet_build.target_amount) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    MAX(oit_build.target_amount) AS operating_income_target,
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
    MAX(rpt_build.target_amount) AS recurring_profit_target
  FROM consolidated_metrics cm
  LEFT JOIN expense_data ed
    ON cm.year_month = ed.year_month
    AND cm.organization = ed.parent_organization
    AND ed.detail_category = '硝子建材営業部'
  LEFT JOIN operating_expenses_target oet_build
    ON cm.year_month = oet_build.year_month
    AND oet_build.organization = '硝子建材営業部'
    AND oet_build.detail_category = '硝子建材営業部計'
  LEFT JOIN operating_income_target oit_build
    ON cm.year_month = oit_build.year_month
    AND oit_build.organization = '硝子建材営業部'
    AND oit_build.detail_category = '硝子建材営業部計'
  LEFT JOIN recurring_profit_target rpt_build
    ON cm.year_month = rpt_build.year_month
    AND rpt_build.organization = '硝子建材営業部'
    AND rpt_build.detail_category = '硝子建材営業部計'
  WHERE cm.organization = '硝子建材営業部'
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 最上位レベル（東京支店計）
  SELECT
    cm.year_month,
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
    MAX(ed.operating_expense) AS operating_expense_actual,
    MAX(oet_tokyo.target_amount) AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    MAX(oit_tokyo.target_amount) AS operating_income_target,
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
    MAX(rpt_tokyo.target_amount) AS recurring_profit_target
  FROM consolidated_metrics cm
  LEFT JOIN (
    SELECT
      year_month,
      SUM(COALESCE(operating_expense, 0)) AS operating_expense,
      SUM(COALESCE(rebate_income, 0)) AS rebate_income,
      SUM(COALESCE(other_income, 0)) AS other_income,
      SUM(COALESCE(interest_expense, 0)) AS interest_expense,
      SUM(COALESCE(misc_loss, 0)) AS misc_loss,
      SUM(COALESCE(hq_expense, 0)) AS hq_expense
    FROM expense_data
    GROUP BY year_month
  ) ed ON cm.year_month = ed.year_month
  LEFT JOIN operating_expenses_target oet_tokyo
    ON cm.year_month = oet_tokyo.year_month
    AND oet_tokyo.organization = '東京支店'
    AND oet_tokyo.detail_category = '東京支店計'
  LEFT JOIN operating_income_target oit_tokyo
    ON cm.year_month = oit_tokyo.year_month
    AND oit_tokyo.organization = '東京支店'
    AND oit_tokyo.detail_category = '東京支店計'
  LEFT JOIN recurring_profit_target rpt_tokyo
    ON cm.year_month = rpt_tokyo.year_month
    AND rpt_tokyo.organization = '東京支店'
    AND rpt_tokyo.detail_category = '東京支店計'
  GROUP BY cm.year_month
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
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
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
    year_month,
    '売上高',
    1,
    '目標比(%)',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
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
    year_month,
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
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
    year_month,
    '売上総利益',
    2,
    '目標比(%)',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
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
    year_month,
    '売上総利益率',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
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
    year_month,
    '売上総利益率',
    3,
    '目標比(%)',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    operating_expense_target
  FROM aggregated_metrics
  WHERE operating_expense_target IS NOT NULL
  UNION ALL
  -- 営業経費: 本年実績
  SELECT
    year_month,
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    CASE
      WHEN NULLIF(operating_expense_target, 0) IS NULL THEN NULL
      ELSE operating_expense_actual / operating_expense_target
    END
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益: 本年目標
  SELECT
    year_month,
    '営業利益',
    5,
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    CASE
      WHEN NULLIF(operating_income_target, 0) IS NULL THEN NULL
      ELSE operating_income_actual / operating_income_target
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
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
    '東京支店',
    detail_category,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（清水他）' THEN 4
      WHEN '小笠原（三井住友他）' THEN 5
      WHEN '高石（内装・リニューアル）' THEN 6
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    recurring_profit_actual
  FROM aggregated_metrics
  UNION ALL
  -- 経常利益: 累積本年目標（現状は1ヶ月分のみなので当月目標と同じ）
  SELECT
    year_month,
    '経常利益',
    11,
    '累積本年目標',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    COALESCE(recurring_profit_target, 0)
  FROM aggregated_metrics
  WHERE recurring_profit_target IS NOT NULL
  UNION ALL
  -- 経常利益: 累積本年実績（現状は1ヶ月分のみなので当月実績と同じ）
  SELECT
    year_month,
    '経常利益',
    11,
    '累積本年実績',
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
      WHEN '岡本（清水他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子工事' THEN 11
      WHEN 'ビルサッシ' THEN 12
      WHEN '硝子販売' THEN 13
      WHEN 'サッシ販売' THEN 14
      WHEN 'サッシ完成品' THEN 15
      WHEN 'その他' THEN 16
      ELSE 99
    END,
    COALESCE(recurring_profit_actual, 0)
  FROM aggregated_metrics
  WHERE recurring_profit_actual IS NOT NULL
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
