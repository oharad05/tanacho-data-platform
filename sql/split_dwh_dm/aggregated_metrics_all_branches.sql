/*
============================================================
中間テーブル: aggregated_metrics_all_branches
============================================================
目的: DataMartの複雑さを軽減するため、aggregated_metricsを
     3支店統合で事前にテーブル化

役割: DWH層とDataMart層の中間に位置し、複雑な階層集計を実施
     各支店のDataMartはこのテーブルをbranchでフィルタして使用

対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名
  - organization: 組織
  - detail_category: 詳細分類
  - sales_actual, sales_target, sales_prev_year
  - gross_profit_actual, gross_profit_target, gross_profit_prev_year
  - gross_profit_margin_actual, gross_profit_margin_target, gross_profit_margin_prev_year
  - operating_expense_actual, operating_expense_target, operating_expense_prev_year
  - operating_income_actual, operating_income_target, operating_income_prev_year
  - rebate_income, other_non_operating_income, non_operating_expenses
  - miscellaneous_loss, head_office_expense
  - recurring_profit_actual, recurring_profit_target
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.aggregated_metrics_all_branches` AS

-- ============================================================
-- 東京支店
-- ============================================================
WITH tokyo_base AS (
  SELECT
    year_month,
    organization,
    detail_category,
    sales_amount AS sales_actual,
    gross_profit_amount AS gross_profit_actual
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual`
  WHERE branch = '東京支店'
),

tokyo_prev AS (
  SELECT
    year_month,
    organization,
    detail_category,
    sales_amount,
    gross_profit_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year`
  WHERE branch = '東京支店'
),

tokyo_target AS (
  SELECT
    year_month,
    metric_type,
    organization,
    detail_category,
    target_amount
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target`
  WHERE branch = '東京支店'
),

tokyo_expense AS (
  SELECT
    year_month,
    detail_category,
    operating_expense_amount,
    operating_expense_prev_year
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
  WHERE branch = '東京支店'
),

tokyo_expense_data AS (
  SELECT
    oe.year_month,
    CASE
      WHEN oe.detail_category = 'ガラス工事計' THEN '工事営業部'
      WHEN oe.detail_category = '山本（改装）' THEN '工事営業部'
      WHEN oe.detail_category = '硝子建材営業部' THEN '硝子建材営業部'
      ELSE NULL
    END AS parent_organization,
    oe.detail_category,
    oe.operating_expense_amount AS operating_expense,
    oe.operating_expense_prev_year,
    noi.rebate_income,
    noi.other_non_operating_income AS other_income,
    noe.interest_expense,
    ml.miscellaneous_loss_amount AS misc_loss,
    hoe.head_office_expense AS hq_expense
  FROM tokyo_expense oe
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON oe.year_month = noi.year_month AND oe.detail_category = noi.detail_category AND noi.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses` noe
    ON oe.year_month = noe.year_month AND oe.detail_category = noe.detail_category
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON oe.year_month = ml.year_month AND oe.detail_category = ml.detail_category AND ml.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON oe.year_month = hoe.year_month AND oe.detail_category = hoe.detail_category AND hoe.branch = '東京支店'
),

tokyo_consolidated AS (
  SELECT
    sa.year_month,
    sa.organization,
    sa.detail_category,
    sa.sales_actual,
    st_sales.target_amount AS sales_target,
    COALESCE(sa_prev.sales_amount, 0) AS sales_prev_year,
    sa.gross_profit_actual,
    st_gp.target_amount AS gross_profit_target,
    COALESCE(sa_prev.gross_profit_amount, 0) AS gross_profit_prev_year,
    SAFE_DIVIDE(sa.gross_profit_actual, sa.sales_actual) AS gross_profit_margin_actual,
    SAFE_DIVIDE(st_gp.target_amount, st_sales.target_amount) AS gross_profit_margin_target,
    SAFE_DIVIDE(sa_prev.gross_profit_amount, sa_prev.sales_amount) AS gross_profit_margin_prev_year,
    CAST(NULL AS FLOAT64) AS operating_expense_actual,
    oet.target_amount AS operating_expense_target,
    CAST(NULL AS FLOAT64) AS operating_expense_prev_year,
    CAST(NULL AS FLOAT64) AS operating_income_actual,
    oit.target_amount AS operating_income_target,
    CAST(NULL AS FLOAT64) AS operating_income_prev_year,
    CAST(NULL AS FLOAT64) AS rebate_income,
    CAST(NULL AS FLOAT64) AS other_non_operating_income,
    CAST(NULL AS FLOAT64) AS non_operating_expenses,
    CAST(NULL AS FLOAT64) AS miscellaneous_loss,
    CAST(NULL AS FLOAT64) AS head_office_expense,
    CAST(NULL AS FLOAT64) AS recurring_profit_actual,
    rpt.target_amount AS recurring_profit_target
  FROM tokyo_base sa
  LEFT JOIN tokyo_prev sa_prev
    ON sa.year_month = sa_prev.year_month AND sa.organization = sa_prev.organization AND sa.detail_category = sa_prev.detail_category
  LEFT JOIN tokyo_target st_sales
    ON sa.year_month = st_sales.year_month AND sa.organization = st_sales.organization AND sa.detail_category = st_sales.detail_category AND st_sales.metric_type = 'sales'
  LEFT JOIN tokyo_target st_gp
    ON sa.year_month = st_gp.year_month AND sa.organization = st_gp.organization AND sa.detail_category = st_gp.detail_category AND st_gp.metric_type = 'gross_profit'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND sa.organization = oet.organization AND sa.detail_category = oet.detail_category AND oet.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND sa.organization = oit.organization AND sa.detail_category = oit.detail_category AND oit.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND sa.organization = rpt.organization AND sa.detail_category = rpt.detail_category AND rpt.branch = '東京支店'
)

,tokyo_aggregated AS (
  -- 詳細レベル
  SELECT * FROM tokyo_consolidated

  UNION ALL

  -- ガラス工事計
  SELECT
    cm.year_month, cm.organization, 'ガラス工事計' AS detail_category,
    SUM(cm.sales_actual) AS sales_actual, SUM(cm.sales_target) AS sales_target, SUM(cm.sales_prev_year) AS sales_prev_year,
    SUM(cm.gross_profit_actual) AS gross_profit_actual, SUM(cm.gross_profit_target) AS gross_profit_target, SUM(cm.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)) AS gross_profit_margin_prev_year,
    MAX(ed.operating_expense) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(ed.operating_expense_prev_year) AS operating_expense_prev_year,
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    SUM(cm.gross_profit_prev_year) - COALESCE(MAX(ed.operating_expense_prev_year), 0) AS operating_income_prev_year,
    MAX(ed.rebate_income) AS rebate_income, MAX(ed.other_income) AS other_non_operating_income,
    MAX(ed.interest_expense) AS non_operating_expenses, MAX(ed.misc_loss) AS miscellaneous_loss, MAX(ed.hq_expense) AS head_office_expense,
    (SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) + COALESCE(MAX(ed.rebate_income), 0) + COALESCE(MAX(ed.other_income), 0)
     - COALESCE(MAX(ed.interest_expense), 0) - COALESCE(MAX(ed.misc_loss), 0) - COALESCE(MAX(ed.hq_expense), 0)) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM tokyo_consolidated cm
  LEFT JOIN tokyo_expense_data ed ON cm.year_month = ed.year_month AND cm.organization = ed.parent_organization AND ed.detail_category = 'ガラス工事計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON cm.year_month = oet.year_month AND oet.organization = '工事営業部' AND oet.detail_category = 'ガラス工事計' AND oet.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON cm.year_month = oit.year_month AND oit.organization = '工事営業部' AND oit.detail_category = 'ガラス工事計' AND oit.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON cm.year_month = rpt.year_month AND rpt.organization = '工事営業部' AND rpt.detail_category = 'ガラス工事計' AND rpt.branch = '東京支店'
  WHERE cm.organization = '工事営業部' AND cm.detail_category IN ('佐々木（大成・鹿島他）', '小笠原（三井住友他）', '高石（内装・リニューアル）', '浅井（清水他）')
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 工事営業部計
  SELECT
    cm.year_month, cm.organization, CONCAT(cm.organization, '計') AS detail_category,
    SUM(cm.sales_actual), SUM(cm.sales_target), SUM(cm.sales_prev_year),
    SUM(cm.gross_profit_actual), SUM(cm.gross_profit_target), SUM(cm.gross_profit_prev_year),
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)),
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)),
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)),
    MAX(ed.operating_expense), MAX(oet.target_amount), MAX(ed.operating_expense_prev_year),
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0),
    MAX(oit.target_amount),
    SUM(cm.gross_profit_prev_year) - COALESCE(MAX(ed.operating_expense_prev_year), 0),
    MAX(ed.rebate_income), MAX(ed.other_income), MAX(ed.interest_expense), MAX(ed.misc_loss), MAX(ed.hq_expense),
    (SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) + COALESCE(MAX(ed.rebate_income), 0) + COALESCE(MAX(ed.other_income), 0)
     - COALESCE(MAX(ed.interest_expense), 0) - COALESCE(MAX(ed.misc_loss), 0) - COALESCE(MAX(ed.hq_expense), 0)),
    MAX(rpt.target_amount)
  FROM tokyo_consolidated cm
  LEFT JOIN (
    SELECT year_month, parent_organization,
      SUM(COALESCE(operating_expense, 0)) AS operating_expense,
      SUM(COALESCE(operating_expense_prev_year, 0)) AS operating_expense_prev_year,
      SUM(COALESCE(rebate_income, 0)) AS rebate_income, SUM(COALESCE(other_income, 0)) AS other_income,
      SUM(COALESCE(interest_expense, 0)) AS interest_expense, SUM(COALESCE(misc_loss, 0)) AS misc_loss, SUM(COALESCE(hq_expense, 0)) AS hq_expense
    FROM tokyo_expense_data
    WHERE detail_category IN ('ガラス工事計', '山本（改装）')
    GROUP BY year_month, parent_organization
  ) ed ON cm.year_month = ed.year_month AND cm.organization = ed.parent_organization
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON cm.year_month = oet.year_month AND oet.organization = '工事営業部' AND oet.detail_category = '工事営業部計' AND oet.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON cm.year_month = oit.year_month AND oit.organization = '工事営業部' AND oit.detail_category = '工事営業部計' AND oit.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON cm.year_month = rpt.year_month AND rpt.organization = '工事営業部' AND rpt.detail_category = '工事営業部計' AND rpt.branch = '東京支店'
  WHERE cm.organization = '工事営業部'
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 硝子建材営業部計
  SELECT
    cm.year_month, cm.organization, CONCAT(cm.organization, '計') AS detail_category,
    SUM(cm.sales_actual), SUM(cm.sales_target), SUM(cm.sales_prev_year),
    SUM(cm.gross_profit_actual), SUM(cm.gross_profit_target), SUM(cm.gross_profit_prev_year),
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)),
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)),
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)),
    MAX(ed.operating_expense), MAX(oet.target_amount), MAX(ed.operating_expense_prev_year),
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0),
    MAX(oit.target_amount),
    SUM(cm.gross_profit_prev_year) - COALESCE(MAX(ed.operating_expense_prev_year), 0),
    MAX(ed.rebate_income), MAX(ed.other_income), MAX(ed.interest_expense), MAX(ed.misc_loss), MAX(ed.hq_expense),
    (SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) + COALESCE(MAX(ed.rebate_income), 0) + COALESCE(MAX(ed.other_income), 0)
     - COALESCE(MAX(ed.interest_expense), 0) - COALESCE(MAX(ed.misc_loss), 0) - COALESCE(MAX(ed.hq_expense), 0)),
    MAX(rpt.target_amount)
  FROM tokyo_consolidated cm
  LEFT JOIN tokyo_expense_data ed ON cm.year_month = ed.year_month AND cm.organization = ed.parent_organization AND ed.detail_category = '硝子建材営業部'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON cm.year_month = oet.year_month AND oet.organization = '硝子建材営業部' AND oet.detail_category = '硝子建材営業部計' AND oet.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON cm.year_month = oit.year_month AND oit.organization = '硝子建材営業部' AND oit.detail_category = '硝子建材営業部計' AND oit.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON cm.year_month = rpt.year_month AND rpt.organization = '硝子建材営業部' AND rpt.detail_category = '硝子建材営業部計' AND rpt.branch = '東京支店'
  WHERE cm.organization = '硝子建材営業部'
  GROUP BY cm.year_month, cm.organization

  UNION ALL

  -- 東京支店計
  SELECT
    cm.year_month, '東京支店' AS organization, '東京支店計' AS detail_category,
    SUM(cm.sales_actual), SUM(cm.sales_target), SUM(cm.sales_prev_year),
    SUM(cm.gross_profit_actual), SUM(cm.gross_profit_target), SUM(cm.gross_profit_prev_year),
    SAFE_DIVIDE(SUM(cm.gross_profit_actual), SUM(cm.sales_actual)),
    SAFE_DIVIDE(SUM(cm.gross_profit_target), SUM(cm.sales_target)),
    SAFE_DIVIDE(SUM(cm.gross_profit_prev_year), SUM(cm.sales_prev_year)),
    MAX(ed.operating_expense), MAX(oet.target_amount), MAX(ed.operating_expense_prev_year),
    SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0),
    MAX(oit.target_amount),
    SUM(cm.gross_profit_prev_year) - COALESCE(MAX(ed.operating_expense_prev_year), 0),
    MAX(ed.rebate_income), MAX(ed.other_income), MAX(ed.interest_expense), MAX(ed.misc_loss), MAX(ed.hq_expense),
    (SUM(cm.gross_profit_actual) - COALESCE(MAX(ed.operating_expense), 0) + COALESCE(MAX(ed.rebate_income), 0) + COALESCE(MAX(ed.other_income), 0)
     - COALESCE(MAX(ed.interest_expense), 0) - COALESCE(MAX(ed.misc_loss), 0) - COALESCE(MAX(ed.hq_expense), 0)),
    MAX(rpt.target_amount)
  FROM tokyo_consolidated cm
  LEFT JOIN (
    SELECT year_month,
      SUM(COALESCE(operating_expense, 0)) AS operating_expense,
      SUM(COALESCE(operating_expense_prev_year, 0)) AS operating_expense_prev_year,
      SUM(COALESCE(rebate_income, 0)) AS rebate_income, SUM(COALESCE(other_income, 0)) AS other_income,
      SUM(COALESCE(interest_expense, 0)) AS interest_expense, SUM(COALESCE(misc_loss, 0)) AS misc_loss, SUM(COALESCE(hq_expense, 0)) AS hq_expense
    FROM tokyo_expense_data
    GROUP BY year_month
  ) ed ON cm.year_month = ed.year_month
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON cm.year_month = oet.year_month AND oet.organization = '東京支店' AND oet.detail_category = '東京支店計' AND oet.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON cm.year_month = oit.year_month AND oit.organization = '東京支店' AND oit.detail_category = '東京支店計' AND oit.branch = '東京支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON cm.year_month = rpt.year_month AND rpt.organization = '東京支店' AND rpt.detail_category = '東京支店計' AND rpt.branch = '東京支店'
  GROUP BY cm.year_month
)

-- ============================================================
-- 長崎支店の集計処理
-- ============================================================
,nagasaki_aggregated AS (
  -- 詳細レベル（個人・部門レベル）
  SELECT
    sa.year_month,
    sa.organization,
    sa.detail_category,
    sa.sales_amount AS sales_actual,
    st_sales.target_amount AS sales_target,
    sap.sales_amount AS sales_prev_year,
    sa.gross_profit_amount AS gross_profit_actual,
    st_gp.target_amount AS gross_profit_target,
    sap.gross_profit_amount AS gross_profit_prev_year,
    SAFE_DIVIDE(sa.gross_profit_amount, sa.sales_amount) AS gross_profit_margin_actual,
    SAFE_DIVIDE(st_gp.target_amount, st_sales.target_amount) AS gross_profit_margin_target,
    SAFE_DIVIDE(sap.gross_profit_amount, sap.sales_amount) AS gross_profit_margin_prev_year,
    oe.operating_expense_amount AS operating_expense_actual,
    oet.target_amount AS operating_expense_target,
    oe.operating_expense_prev_year,
    (sa.gross_profit_amount - COALESCE(oe.operating_expense_amount, 0)) AS operating_income_actual,
    oit.target_amount AS operating_income_target,
    (sap.gross_profit_amount - COALESCE(oe.operating_expense_prev_year, 0)) AS operating_income_prev_year,
    noi.rebate_income,
    noi.other_non_operating_income,
    noe.interest_expense AS non_operating_expenses,
    ml.miscellaneous_loss_amount AS miscellaneous_loss,
    hoe.head_office_expense,
    (
      sa.gross_profit_amount
      - COALESCE(oe.operating_expense_amount, 0)
      + COALESCE(noi.rebate_income, 0)
      + COALESCE(noi.other_non_operating_income, 0)
      - COALESCE(noe.interest_expense, 0)
      - COALESCE(ml.miscellaneous_loss_amount, 0)
      - COALESCE(hoe.head_office_expense, 0)
    ) AS recurring_profit_actual,
    rpt.target_amount AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND sa.organization = st_sales.organization AND sa.detail_category = st_sales.detail_category AND st_sales.metric_type = 'sales' AND st_sales.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND sa.organization = st_gp.organization AND sa.detail_category = st_gp.detail_category AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND sa.detail_category = oe.detail_category AND oe.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND sa.detail_category = noi.detail_category AND noi.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
    ON sa.year_month = noe.year_month AND sa.detail_category = noe.detail_category
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND sa.detail_category = ml.detail_category AND ml.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND sa.detail_category = hoe.detail_category AND hoe.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND sa.organization = oet.organization AND sa.detail_category = oet.detail_category AND oet.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND sa.organization = oit.organization AND sa.detail_category = oit.detail_category AND oit.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND sa.organization = rpt.organization AND sa.detail_category = rpt.detail_category AND rpt.branch = '長崎支店'
  WHERE sa.branch = '長崎支店'
    AND sa.detail_category NOT LIKE '%計'

  UNION ALL

  -- 中間レベル（工事営業部: ガラス工事計）
  SELECT
    sa.year_month,
    sa.organization,
    'ガラス工事計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    MAX(oe.operating_expense_amount) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(oe.operating_expense_prev_year),
    (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
    MAX(noi.rebate_income) AS rebate_income,
    MAX(noi.other_non_operating_income),
    MAX(noe.interest_expense) AS non_operating_expenses,
    MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
    MAX(hoe.head_office_expense),
    (
      SUM(sa.gross_profit_amount)
      - COALESCE(MAX(oe.operating_expense_amount), 0)
      + COALESCE(MAX(noi.rebate_income), 0)
      + COALESCE(MAX(noi.other_non_operating_income), 0)
      - COALESCE(MAX(noe.interest_expense), 0)
      - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
      - COALESCE(MAX(hoe.head_office_expense), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '工事営業部' AND st_sales.detail_category = 'ガラス工事計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '工事営業部' AND st_gp.detail_category = 'ガラス工事計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND oe.detail_category = 'ガラス工事計' AND oe.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND noi.detail_category = 'ガラス工事計' AND noi.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
    ON sa.year_month = noe.year_month AND noe.detail_category = 'ガラス工事計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND ml.detail_category = 'ガラス工事計' AND ml.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND hoe.detail_category = 'ガラス工事計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '工事営業部' AND oet.detail_category = 'ガラス工事計' AND oet.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '工事営業部' AND oit.detail_category = 'ガラス工事計' AND oit.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '工事営業部' AND rpt.detail_category = 'ガラス工事計' AND rpt.branch = '長崎支店'
  WHERE sa.branch = '長崎支店'
    AND sa.organization = '工事営業部'
    AND sa.detail_category IN ('佐々木(大成・鹿島他)', '岡本(清水他)', '小笠原(三井住友他)', '高石(内装・リニューアル)', '浅井(清水他)')
  GROUP BY sa.year_month, sa.organization

  UNION ALL

  -- 組織レベル（工事営業部計）
  SELECT
    sa.year_month,
    sa.organization,
    '工事営業部計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    MAX(oe.operating_expense_amount) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(oe.operating_expense_prev_year),
    (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
    MAX(noi.rebate_income) AS rebate_income,
    MAX(noi.other_non_operating_income),
    MAX(noe.interest_expense) AS non_operating_expenses,
    MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
    MAX(hoe.head_office_expense),
    (
      SUM(sa.gross_profit_amount)
      - COALESCE(MAX(oe.operating_expense_amount), 0)
      + COALESCE(MAX(noi.rebate_income), 0)
      + COALESCE(MAX(noi.other_non_operating_income), 0)
      - COALESCE(MAX(noe.interest_expense), 0)
      - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
      - COALESCE(MAX(hoe.head_office_expense), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '工事営業部' AND st_sales.detail_category = '工事営業部計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '工事営業部' AND st_gp.detail_category = '工事営業部計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND oe.detail_category = '工事営業部計' AND oe.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND noi.detail_category = '工事営業部計' AND noi.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
    ON sa.year_month = noe.year_month AND noe.detail_category = '工事営業部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND ml.detail_category = '工事営業部計' AND ml.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND hoe.detail_category = '工事営業部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '工事営業部' AND oet.detail_category = '工事営業部計' AND oet.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '工事営業部' AND oit.detail_category = '工事営業部計' AND oit.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '工事営業部' AND rpt.detail_category = '工事営業部計' AND rpt.branch = '長崎支店'
  WHERE sa.branch = '長崎支店'
    AND sa.organization = '工事営業部'
  GROUP BY sa.year_month, sa.organization

  UNION ALL

  -- 硝子建材営業部計
  SELECT
    sa.year_month,
    sa.organization,
    '硝子建材営業部計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    MAX(oe.operating_expense_amount) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(oe.operating_expense_prev_year),
    (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
    MAX(noi.rebate_income) AS rebate_income,
    MAX(noi.other_non_operating_income),
    MAX(noe.interest_expense) AS non_operating_expenses,
    MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
    MAX(hoe.head_office_expense),
    (
      SUM(sa.gross_profit_amount)
      - COALESCE(MAX(oe.operating_expense_amount), 0)
      + COALESCE(MAX(noi.rebate_income), 0)
      + COALESCE(MAX(noi.other_non_operating_income), 0)
      - COALESCE(MAX(noe.interest_expense), 0)
      - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
      - COALESCE(MAX(hoe.head_office_expense), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '硝子建材営業部' AND st_sales.detail_category = '硝子建材営業部計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '硝子建材営業部' AND st_gp.detail_category = '硝子建材営業部計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND oe.detail_category = '硝子建材営業部計' AND oe.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND noi.detail_category = '硝子建材営業部計' AND noi.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
    ON sa.year_month = noe.year_month AND noe.detail_category = '硝子建材営業部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND ml.detail_category = '硝子建材営業部計' AND ml.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND hoe.detail_category = '硝子建材営業部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '硝子建材営業部' AND oet.detail_category = '硝子建材営業部計' AND oet.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '硝子建材営業部' AND oit.detail_category = '硝子建材営業部計' AND oit.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '硝子建材営業部' AND rpt.detail_category = '硝子建材営業部計' AND rpt.branch = '長崎支店'
  WHERE sa.branch = '長崎支店'
    AND sa.organization = '硝子建材営業部'
  GROUP BY sa.year_month, sa.organization

  UNION ALL

  -- 支店レベル（長崎支店計）- 工事営業部計と硝子建材営業部計を集計
  SELECT
    dept.year_month,
    '長崎支店' AS organization,
    '長崎支店計' AS detail_category,
    SUM(dept.sales_actual) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(dept.sales_prev_year) AS sales_prev_year,
    SUM(dept.gross_profit_actual) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(dept.gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(dept.gross_profit_actual), SUM(dept.sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(dept.gross_profit_prev_year), SUM(dept.sales_prev_year)) AS gross_profit_margin_prev_year,
    SUM(dept.operating_expense_actual) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    SUM(dept.operating_expense_prev_year) AS operating_expense_prev_year,
    SUM(dept.operating_income_actual) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    SUM(dept.operating_income_prev_year) AS operating_income_prev_year,
    SUM(dept.rebate_income) AS rebate_income,
    SUM(dept.other_non_operating_income) AS other_non_operating_income,
    SUM(dept.non_operating_expenses) AS non_operating_expenses,
    SUM(dept.miscellaneous_loss) AS miscellaneous_loss,
    SUM(dept.head_office_expense) AS head_office_expense,
    SUM(dept.recurring_profit_actual) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM (
    -- ネストしたサブクエリで工事営業部計と硝子建材営業部計を抽出
    SELECT
      sa.year_month,
      sa.organization,
      '工事営業部計' AS detail_category,
      SUM(sa.sales_amount) AS sales_actual,
      SUM(sap.sales_amount) AS sales_prev_year,
      SUM(sa.gross_profit_amount) AS gross_profit_actual,
      SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
      MAX(oe.operating_expense_amount) AS operating_expense_actual,
      MAX(oe.operating_expense_prev_year) AS operating_expense_prev_year,
      (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
      (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
      MAX(noi.rebate_income) AS rebate_income,
      MAX(noi.other_non_operating_income) AS other_non_operating_income,
      MAX(noe.interest_expense) AS non_operating_expenses,
      MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
      MAX(hoe.head_office_expense) AS head_office_expense,
      (
        SUM(sa.gross_profit_amount)
        - COALESCE(MAX(oe.operating_expense_amount), 0)
        + COALESCE(MAX(noi.rebate_income), 0)
        + COALESCE(MAX(noi.other_non_operating_income), 0)
        - COALESCE(MAX(noe.interest_expense), 0)
        - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
        - COALESCE(MAX(hoe.head_office_expense), 0)
      ) AS recurring_profit_actual
    FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
      ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
      ON sa.year_month = oe.year_month AND oe.detail_category = '工事営業部計' AND oe.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
      ON sa.year_month = noi.year_month AND noi.detail_category = '工事営業部計' AND noi.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
      ON sa.year_month = noe.year_month AND noe.detail_category = '工事営業部計'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
      ON sa.year_month = ml.year_month AND ml.detail_category = '工事営業部計' AND ml.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
      ON sa.year_month = hoe.year_month AND hoe.detail_category = '工事営業部計'
    WHERE sa.branch = '長崎支店'
      AND sa.organization = '工事営業部'
    GROUP BY sa.year_month, sa.organization

    UNION ALL

    SELECT
      sa.year_month,
      sa.organization,
      '硝子建材営業部計' AS detail_category,
      SUM(sa.sales_amount) AS sales_actual,
      SUM(sap.sales_amount) AS sales_prev_year,
      SUM(sa.gross_profit_amount) AS gross_profit_actual,
      SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
      MAX(oe.operating_expense_amount) AS operating_expense_actual,
      MAX(oe.operating_expense_prev_year) AS operating_expense_prev_year,
      (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
      (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
      MAX(noi.rebate_income) AS rebate_income,
      MAX(noi.other_non_operating_income) AS other_non_operating_income,
      MAX(noe.interest_expense) AS non_operating_expenses,
      MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
      MAX(hoe.head_office_expense) AS head_office_expense,
      (
        SUM(sa.gross_profit_amount)
        - COALESCE(MAX(oe.operating_expense_amount), 0)
        + COALESCE(MAX(noi.rebate_income), 0)
        + COALESCE(MAX(noi.other_non_operating_income), 0)
        - COALESCE(MAX(noe.interest_expense), 0)
        - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
        - COALESCE(MAX(hoe.head_office_expense), 0)
      ) AS recurring_profit_actual
    FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
      ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
      ON sa.year_month = oe.year_month AND oe.detail_category = '硝子建材営業部計' AND oe.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
      ON sa.year_month = noi.year_month AND noi.detail_category = '硝子建材営業部計' AND noi.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` noe
      ON sa.year_month = noe.year_month AND noe.detail_category = '硝子建材営業部計'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
      ON sa.year_month = ml.year_month AND ml.detail_category = '硝子建材営業部計' AND ml.branch = '長崎支店'
    LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
      ON sa.year_month = hoe.year_month AND hoe.detail_category = '硝子建材営業部計'
    WHERE sa.branch = '長崎支店'
      AND sa.organization = '硝子建材営業部'
    GROUP BY sa.year_month, sa.organization
  ) dept
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON dept.year_month = st_sales.year_month AND st_sales.organization = '長崎支店' AND st_sales.detail_category = '長崎支店計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON dept.year_month = st_gp.year_month AND st_gp.organization = '長崎支店' AND st_gp.detail_category = '長崎支店計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON dept.year_month = oet.year_month AND oet.organization = '長崎支店' AND oet.detail_category = '長崎支店計' AND oet.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON dept.year_month = oit.year_month AND oit.organization = '長崎支店' AND oit.detail_category = '長崎支店計' AND oit.branch = '長崎支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON dept.year_month = rpt.year_month AND rpt.organization = '長崎支店' AND rpt.detail_category = '長崎支店計' AND rpt.branch = '長崎支店'
  GROUP BY dept.year_month

)

-- ============================================================
-- 福岡支店の集計処理
-- ============================================================
,fukuoka_aggregated AS (
  -- 詳細レベル（部門レベル）
  SELECT
    sa.year_month,
    sa.organization,
    sa.detail_category,
    sa.sales_amount AS sales_actual,
    st_sales.target_amount AS sales_target,
    sap.sales_amount AS sales_prev_year,
    sa.gross_profit_amount AS gross_profit_actual,
    st_gp.target_amount AS gross_profit_target,
    sap.gross_profit_amount AS gross_profit_prev_year,
    SAFE_DIVIDE(sa.gross_profit_amount, sa.sales_amount) AS gross_profit_margin_actual,
    SAFE_DIVIDE(st_gp.target_amount, st_sales.target_amount) AS gross_profit_margin_target,
    SAFE_DIVIDE(sap.gross_profit_amount, sap.sales_amount) AS gross_profit_margin_prev_year,
    oe.operating_expense_amount AS operating_expense_actual,
    oet.target_amount AS operating_expense_target,
    oe.operating_expense_prev_year,
    (sa.gross_profit_amount - COALESCE(oe.operating_expense_amount, 0)) AS operating_income_actual,
    oit.target_amount AS operating_income_target,
    (sap.gross_profit_amount - COALESCE(oe.operating_expense_prev_year, 0)) AS operating_income_prev_year,
    noi.rebate_income,
    noi.other_non_operating_income,
    noe.interest_expense AS non_operating_expenses,
    ml.miscellaneous_loss_amount AS miscellaneous_loss,
    hoe.head_office_expense,
    (
      sa.gross_profit_amount
      - COALESCE(oe.operating_expense_amount, 0)
      + COALESCE(noi.rebate_income, 0)
      + COALESCE(noi.other_non_operating_income, 0)
      - COALESCE(noe.interest_expense, 0)
      - COALESCE(ml.miscellaneous_loss_amount, 0)
      - COALESCE(hoe.head_office_expense, 0)
    ) AS recurring_profit_actual,
    rpt.target_amount AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND sa.organization = st_sales.organization AND sa.detail_category = st_sales.detail_category AND st_sales.metric_type = 'sales' AND st_sales.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND sa.organization = st_gp.organization AND sa.detail_category = st_gp.detail_category AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND sa.detail_category = oe.detail_category AND oe.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND sa.detail_category = noi.detail_category AND noi.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` noe
    ON sa.year_month = noe.year_month AND sa.detail_category = noe.detail_category
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND sa.detail_category = ml.detail_category AND ml.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND sa.detail_category = hoe.detail_category
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND sa.organization = oet.organization AND sa.detail_category = oet.detail_category AND oet.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND sa.organization = oit.organization AND sa.detail_category = oit.detail_category AND oit.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND sa.organization = rpt.organization AND sa.detail_category = rpt.detail_category AND rpt.branch = '福岡支店'
  WHERE sa.branch = '福岡支店'
    AND sa.detail_category NOT LIKE '%計'

  UNION ALL

  -- 組織レベル（工事部計）
  SELECT
    sa.year_month,
    sa.organization,
    '工事部計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    MAX(oe.operating_expense_amount) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(oe.operating_expense_prev_year),
    (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
    MAX(noi.rebate_income) AS rebate_income,
    MAX(noi.other_non_operating_income),
    MAX(noe.interest_expense) AS non_operating_expenses,
    MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
    MAX(hoe.head_office_expense),
    (
      SUM(sa.gross_profit_amount)
      - COALESCE(MAX(oe.operating_expense_amount), 0)
      + COALESCE(MAX(noi.rebate_income), 0)
      + COALESCE(MAX(noi.other_non_operating_income), 0)
      - COALESCE(MAX(noe.interest_expense), 0)
      - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
      - COALESCE(MAX(hoe.head_office_expense), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '工事部' AND st_sales.detail_category = '工事部計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '工事部' AND st_gp.detail_category = '工事部計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND oe.detail_category = '工事部計' AND oe.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND noi.detail_category = '工事部計' AND noi.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` noe
    ON sa.year_month = noe.year_month AND noe.detail_category = '工事部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND ml.detail_category = '工事部計' AND ml.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND hoe.detail_category = '工事部計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '工事部' AND oet.detail_category = '工事部計' AND oet.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '工事部' AND oit.detail_category = '工事部計' AND oit.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '工事部' AND rpt.detail_category = '工事部計' AND rpt.branch = '福岡支店'
  WHERE sa.branch = '福岡支店'
    AND sa.organization = '工事部'
  GROUP BY sa.year_month, sa.organization

  UNION ALL

  -- 組織レベル（硝子樹脂計）
  SELECT
    sa.year_month,
    sa.organization,
    '硝子樹脂計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    MAX(oe.operating_expense_amount) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    MAX(oe.operating_expense_prev_year),
    (SUM(sa.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_amount), 0)) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (SUM(sap.gross_profit_amount) - COALESCE(MAX(oe.operating_expense_prev_year), 0)) AS operating_income_prev_year,
    MAX(noi.rebate_income) AS rebate_income,
    MAX(noi.other_non_operating_income),
    MAX(noe.interest_expense) AS non_operating_expenses,
    MAX(ml.miscellaneous_loss_amount) AS miscellaneous_loss,
    MAX(hoe.head_office_expense),
    (
      SUM(sa.gross_profit_amount)
      - COALESCE(MAX(oe.operating_expense_amount), 0)
      + COALESCE(MAX(noi.rebate_income), 0)
      + COALESCE(MAX(noi.other_non_operating_income), 0)
      - COALESCE(MAX(noe.interest_expense), 0)
      - COALESCE(MAX(ml.miscellaneous_loss_amount), 0)
      - COALESCE(MAX(hoe.head_office_expense), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '硝子樹脂部' AND st_sales.detail_category = '硝子樹脂計' AND st_sales.metric_type = 'sales' AND st_sales.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '硝子樹脂部' AND st_gp.detail_category = '硝子樹脂計' AND st_gp.metric_type = 'gross_profit' AND st_gp.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
    ON sa.year_month = oe.year_month AND oe.detail_category = '硝子樹脂計' AND oe.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON sa.year_month = noi.year_month AND noi.detail_category = '硝子樹脂計' AND noi.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` noe
    ON sa.year_month = noe.year_month AND noe.detail_category = '硝子樹脂計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON sa.year_month = ml.year_month AND ml.detail_category = '硝子樹脂計' AND ml.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` hoe
    ON sa.year_month = hoe.year_month AND hoe.detail_category = '硝子樹脂計'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '硝子樹脂部' AND oet.detail_category = '硝子樹脂計' AND oet.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '硝子樹脂部' AND oit.detail_category = '硝子樹脂計' AND oit.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '硝子樹脂部' AND rpt.detail_category = '硝子樹脂計' AND rpt.branch = '福岡支店'
  WHERE sa.branch = '福岡支店'
    AND sa.organization = '硝子樹脂部'
  GROUP BY sa.year_month, sa.organization

  UNION ALL

  -- GSセンター（売上なし、費用のみ）
  SELECT
    oe.year_month,
    'GSセンター' AS organization,
    'GSセンター' AS detail_category,
    0 AS sales_actual,
    NULL AS sales_target,
    0 AS sales_prev_year,
    0 AS gross_profit_actual,
    NULL AS gross_profit_target,
    0 AS gross_profit_prev_year,
    NULL AS gross_profit_margin_actual,
    NULL AS gross_profit_margin_target,
    NULL AS gross_profit_margin_prev_year,
    oe.operating_expense_amount AS operating_expense_actual,
    oet.target_amount AS operating_expense_target,
    NULL AS operating_expense_prev_year,
    (0 - COALESCE(oe.operating_expense_amount, 0)) AS operating_income_actual,
    oit.target_amount AS operating_income_target,
    (0 - COALESCE(oe.operating_expense_amount, 0)) AS operating_income_prev_year,
    noi.rebate_income,
    noi.other_non_operating_income,
    NULL AS non_operating_expenses,
    ml.miscellaneous_loss_amount AS miscellaneous_loss,
    NULL AS head_office_expense,
    (
      0
      - COALESCE(oe.operating_expense_amount, 0)
      + COALESCE(noi.rebate_income, 0)
      + COALESCE(noi.other_non_operating_income, 0)
      - COALESCE(ml.miscellaneous_loss_amount, 0)
    ) AS recurring_profit_actual,
    rpt.target_amount AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses` oe
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.non_operating_income` noi
    ON oe.year_month = noi.year_month AND noi.detail_category = 'GSセンター' AND noi.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` ml
    ON oe.year_month = ml.year_month AND ml.detail_category = 'GSセンター' AND ml.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON oe.year_month = oet.year_month AND oet.organization = '硝子樹脂部' AND oet.detail_category = 'GSセンター' AND oet.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON oe.year_month = oit.year_month AND oit.organization = '硝子樹脂部' AND oit.detail_category = 'GSセンター' AND oit.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON oe.year_month = rpt.year_month AND rpt.organization = '硝子樹脂部' AND rpt.detail_category = 'GSセンター' AND rpt.branch = '福岡支店'
  WHERE oe.branch = '福岡支店'
    AND oe.detail_category = 'GSセンター'

  UNION ALL

  -- 支店レベル（福岡支店計）※各部署の合計を集計
  SELECT
    sa.year_month,
    '福岡支店' AS organization,
    '福岡支店計' AS detail_category,
    SUM(sa.sales_amount) AS sales_actual,
    MAX(st_sales.target_amount) AS sales_target,
    SUM(sap.sales_amount) AS sales_prev_year,
    SUM(sa.gross_profit_amount) AS gross_profit_actual,
    MAX(st_gp.target_amount) AS gross_profit_target,
    SUM(sap.gross_profit_amount) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(sa.gross_profit_amount), SUM(sa.sales_amount)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(MAX(st_gp.target_amount), MAX(st_sales.target_amount)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(sap.gross_profit_amount), SUM(sap.sales_amount)) AS gross_profit_margin_prev_year,
    (
      SELECT SUM(operating_expense_amount)
      FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
      WHERE branch = '福岡支店' AND year_month = sa.year_month
    ) AS operating_expense_actual,
    MAX(oet.target_amount) AS operating_expense_target,
    (
      SELECT SUM(operating_expense_prev_year)
      FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
      WHERE branch = '福岡支店' AND year_month = sa.year_month
    ) AS operating_expense_prev_year,
    (
      SUM(sa.gross_profit_amount) - COALESCE((
        SELECT SUM(operating_expense_amount)
        FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
    ) AS operating_income_actual,
    MAX(oit.target_amount) AS operating_income_target,
    (
      SUM(sap.gross_profit_amount) - COALESCE((
        SELECT SUM(operating_expense_prev_year)
        FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
    ) AS operating_income_prev_year,
    (
      SELECT SUM(rebate_income)
      FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
      WHERE branch = '福岡支店' AND year_month = sa.year_month
    ) AS rebate_income,
    (
      SELECT SUM(other_non_operating_income)
      FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
      WHERE branch = '福岡支店' AND year_month = sa.year_month
    ) AS other_non_operating_income,
    NULL AS non_operating_expenses,
    (
      SELECT SUM(miscellaneous_loss_amount)
      FROM `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss`
      WHERE branch = '福岡支店' AND year_month = sa.year_month
    ) AS miscellaneous_loss,
    (
      SELECT SUM(head_office_expense)
      FROM `data-platform-prod-475201.corporate_data_dwh.head_office_expenses`
      WHERE year_month = sa.year_month
        AND detail_category IN ('工事部計', '硝子樹脂計', 'GSセンター', '福北センター')
    ) AS head_office_expense,
    (
      SUM(sa.gross_profit_amount)
      - COALESCE((
        SELECT SUM(operating_expense_amount)
        FROM `data-platform-prod-475201.corporate_data_dwh.operating_expenses`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
      + COALESCE((
        SELECT SUM(rebate_income)
        FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
      + COALESCE((
        SELECT SUM(other_non_operating_income)
        FROM `data-platform-prod-475201.corporate_data_dwh.non_operating_income`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
      - COALESCE((
        SELECT SUM(miscellaneous_loss_amount)
        FROM `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss`
        WHERE branch = '福岡支店' AND year_month = sa.year_month
      ), 0)
      - COALESCE((
        SELECT SUM(head_office_expense)
        FROM `data-platform-prod-475201.corporate_data_dwh.head_office_expenses`
        WHERE year_month = sa.year_month
          AND detail_category IN ('工事部計', '硝子樹脂計', 'GSセンター', '福北センター')
      ), 0)
    ) AS recurring_profit_actual,
    MAX(rpt.target_amount) AS recurring_profit_target
  FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual` sa
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` sap
    ON sa.year_month = sap.year_month AND sa.organization = sap.organization AND sa.detail_category = sap.detail_category AND sap.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_sales
    ON sa.year_month = st_sales.year_month AND st_sales.organization = '福岡支店' AND st_sales.detail_category = '福岡支店計' AND st_sales.metric_type = '売上高' AND st_sales.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` st_gp
    ON sa.year_month = st_gp.year_month AND st_gp.organization = '福岡支店' AND st_gp.detail_category = '福岡支店計' AND st_gp.metric_type = '売上総利益' AND st_gp.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_expenses_target` oet
    ON sa.year_month = oet.year_month AND oet.organization = '福岡支店' AND oet.detail_category = '福岡支店計' AND oet.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.operating_income_target` oit
    ON sa.year_month = oit.year_month AND oit.organization = '福岡支店' AND oit.detail_category = '福岡支店計' AND oit.branch = '福岡支店'
  LEFT JOIN `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` rpt
    ON sa.year_month = rpt.year_month AND rpt.organization = '福岡支店' AND rpt.detail_category = '福岡支店計' AND rpt.branch = '福岡支店'
  WHERE sa.branch = '福岡支店'
  GROUP BY sa.year_month
)

-- ============================================================
-- 最終出力: 東京・長崎・福岡の3支店をUNION ALL
-- ============================================================
SELECT '東京支店' AS branch, * FROM tokyo_aggregated
UNION ALL
SELECT '長崎支店' AS branch, * FROM nagasaki_aggregated
UNION ALL
SELECT '福岡支店' AS branch, * FROM fukuoka_aggregated
;
