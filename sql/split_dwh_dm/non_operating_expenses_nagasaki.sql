/*
============================================================
DWH: 営業外費用（社内利息）- 長崎支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: billing_balance, internal_interest, stocks, ms_allocation_ratio
対象支店: 長崎支店
集計単位: 工事営業部、硝子建材営業部

計算ロジック（2025-12-25 仕様書準拠に修正）:
  計算式: ①×②＋③×④＋⑤×⑥＋⑦×⑧（％）＋⑨×⑧（％）

  参照条件:
  - 実行月の2か月前 = DATE_SUB(year_month, INTERVAL 1 MONTH)
  - source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(year_month, INTERVAL 1 MONTH)) AS INT64)

  【工事営業部】
  ① 売掛金: billing_balance, branch_code = 61
  ② 売掛金利率: internal_interest, 売掛金のinterest_rate
  ③ 未落手形: billing_balance, branch_code = 61
  ④ 未落手形利率: internal_interest, 未決済手形のinterest_rate
  ⑤ 在庫・未成工事: stocks, 長崎, 工事, 期末未成工事+当月在庫
  ⑥ 在庫利率: internal_interest, 棚卸在庫・未成工事支出金のinterest_rate
  ⑦ 建物利息: internal_interest, 建物+土地のinterest合計
  ⑧ 案分比率: ms_allocation_ratio, 長崎, 工事, 減価償却案分
  ⑨ 償却資産利息: internal_interest, 償却資産のinterest
  ⑩ 在庫損益・前受け金: ss_inventory_advance_nagasaki（スプレッドシート連携）

  【硝子建材営業部】
  ① 売掛金: billing_balance, branch_code IN (65, 66)
  ② 売掛金利率: internal_interest, 売掛金のinterest_rate
  ③ 未落手形: billing_balance, branch_code IN (65, 66)
  ④ 未落手形利率: internal_interest, 未決済手形のinterest_rate
  ⑤ 在庫・未成工事: stocks, 長崎, 硝子建材, 期末未成工事+当月在庫
  ⑥ 在庫利率: internal_interest, 棚卸在庫・未成工事支出金のinterest_rate
  ⑦ 建物利息: internal_interest, 建物のinterest
  ⑧ 案分比率: ms_allocation_ratio, 長崎, 硝子建材, 減価償却案分
  ⑨ 償却資産利息: internal_interest, 償却資産のinterest
  ⑩ 在庫損益・前受け金: ss_inventory_advance_nagasaki（スプレッドシート連携）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - branch: 支店名（長崎支店）
  - detail_category: 詳細分類（工事営業部計、硝子建材営業部計、長崎支店計）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` AS
WITH
-- ============================================================
-- 対象年月の生成（internal_interestから取得）
-- ============================================================
target_months AS (
  SELECT DISTINCT year_month
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
),

-- ============================================================
-- billing_balance: 売掛金・未落手形（実行月2か月前参照）
-- ============================================================

-- 工事営業部（branch_code: 61）
billing_construction AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_sales_balance) AS receivables_amount,  -- ①
    SUM(bb.unsettled_bill_balance) AS bills_amount              -- ③
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND bb.branch_code = 61
    AND bb.source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(tm.year_month, INTERVAL 1 MONTH)) AS INT64)
  GROUP BY tm.year_month
),

-- 硝子建材営業部（branch_code: 65, 66）
-- 注: 仕様書は「063または065」だが、実データにbranch_code 63は存在しない
-- 長崎硝子建材営１=65、長崎硝子建材営２=66
billing_glass AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_sales_balance) AS receivables_amount,  -- ①
    SUM(bb.unsettled_bill_balance) AS bills_amount              -- ③
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND bb.branch_code IN (65, 66)
    AND bb.source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(tm.year_month, INTERVAL 1 MONTH)) AS INT64)
  GROUP BY tm.year_month
),

-- ============================================================
-- internal_interest: 利率・利息
-- ============================================================

-- ②売掛金利率
receivables_rate AS (
  SELECT
    year_month,
    interest_rate  -- ②
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金'
),

-- ④未落手形利率
bills_rate AS (
  SELECT
    year_month,
    interest_rate  -- ④
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '未決済手形'
),

-- ⑥在庫利率
inventory_rate AS (
  SELECT
    year_month,
    interest_rate  -- ⑥
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
),

-- ⑦建物利息（工事営業部用: 建物+土地）
construction_building_interest AS (
  SELECT
    year_month,
    SUM(interest) AS building_interest  -- ⑦
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown IN ('建物', '土地')
  GROUP BY year_month
),

-- ⑦建物利息（硝子建材営業部用: 建物のみ）
glass_building_interest AS (
  SELECT
    year_month,
    interest AS building_interest  -- ⑦
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '建物'
),

-- ⑨償却資産利息
depreciation_interest AS (
  SELECT
    year_month,
    interest AS depreciation_interest  -- ⑨
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '償却資産'
),

-- ============================================================
-- stocks: 在庫（実行月2か月前参照）
-- ============================================================

-- ⑤在庫・未成工事（工事部門）
construction_inventory AS (
  SELECT
    tm.year_month,
    COALESCE(SUM(s.amount), 0) AS inventory_amount  -- ⑤
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND s.branch = '長崎'
    AND s.department = '工事'
    AND s.category IN ('期末未成工事', '当月在庫')
  GROUP BY tm.year_month
),

-- ⑤在庫・未成工事（硝子建材部門）
glass_inventory AS (
  SELECT
    tm.year_month,
    COALESCE(SUM(s.amount), 0) AS inventory_amount  -- ⑤
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND s.branch = '長崎'
    AND s.department = '硝子建材'
    AND s.category IN ('期末未成工事', '当月在庫')
  GROUP BY tm.year_month
),

-- ============================================================
-- ms_allocation_ratio: 案分比率（source_folder条件付き）
-- ============================================================

-- ⑧工事部案分比率
-- 注: 実行月と同じsource_folderを参照（PDF準拠）
construction_allocation AS (
  SELECT
    tm.year_month,
    ar.ratio AS allocation_ratio  -- ⑧
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.ms_allocation_ratio` ar
    ON ar.year_month = tm.year_month
    AND ar.branch = '長崎'
    AND ar.department = '工事'
    AND ar.category = '減価償却案分'
    AND ar.source_folder = CAST(FORMAT_DATE('%Y%m', tm.year_month) AS INT64)
),

-- ⑧硝子建材部案分比率
-- 注: 実行月と同じsource_folderを参照（PDF準拠）
glass_allocation AS (
  SELECT
    tm.year_month,
    ar.ratio AS allocation_ratio  -- ⑧
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.ms_allocation_ratio` ar
    ON ar.year_month = tm.year_month
    AND ar.branch = '長崎'
    AND ar.department = '硝子建材'
    AND ar.category = '減価償却案分'
    AND ar.source_folder = CAST(FORMAT_DATE('%Y%m', tm.year_month) AS INT64)
),

-- ============================================================
-- スプレッドシート連携: 在庫損益・前受け金
-- ============================================================

-- ⑩在庫損益・前受け金（工事営業部用）
construction_inventory_profit_loss AS (
  SELECT
    posting_month AS year_month,
    COALESCE(inventory_profit_loss, 0) AS inventory_profit_loss,
    COALESCE(advance_received, 0) AS advance_received
  FROM `data-platform-prod-475201.corporate_data.ss_inventory_advance_nagasaki`
  WHERE category = '工事営業部計'
),

-- ⑩在庫損益・前受け金（硝子建材営業部用）
glass_inventory_profit_loss AS (
  SELECT
    posting_month AS year_month,
    COALESCE(inventory_profit_loss, 0) AS inventory_profit_loss,
    COALESCE(advance_received, 0) AS advance_received
  FROM `data-platform-prod-475201.corporate_data.ss_inventory_advance_nagasaki`
  WHERE category = '硝子建材営業部計'
),

-- ============================================================
-- 工事営業部の計算: (①+⑩在庫損益+前受け金)×②＋③×④＋⑤×⑥＋⑦×⑧＋⑨×⑧
-- ============================================================
construction_interest AS (
  SELECT
    tm.year_month,
    '長崎支店' AS branch,
    '工事営業部計' AS detail_category,
    (
      -- (①売掛金+⑩在庫損益+前受け金)×②売掛金利率
      (COALESCE(bc.receivables_amount, 0) + COALESCE(cipl.inventory_profit_loss, 0) + COALESCE(cipl.advance_received, 0))
        * COALESCE(rr.interest_rate, 0)
      -- ③×④ 未落手形利息
      + COALESCE(bc.bills_amount, 0) * COALESCE(br.interest_rate, 0)
      -- ⑤×⑥ 在庫利息
      + COALESCE(ci.inventory_amount, 0) * COALESCE(ir.interest_rate, 0)
      -- ⑦×⑧ 建物利息
      + COALESCE(cbi.building_interest, 0) * COALESCE(ca.allocation_ratio, 0)
      -- ⑨×⑧ 償却資産利息
      + COALESCE(di.depreciation_interest, 0) * COALESCE(ca.allocation_ratio, 0)
    ) AS interest_expense
  FROM target_months tm
  LEFT JOIN billing_construction bc ON tm.year_month = bc.year_month
  LEFT JOIN receivables_rate rr ON tm.year_month = rr.year_month
  LEFT JOIN bills_rate br ON tm.year_month = br.year_month
  LEFT JOIN construction_inventory ci ON tm.year_month = ci.year_month
  LEFT JOIN inventory_rate ir ON tm.year_month = ir.year_month
  LEFT JOIN construction_building_interest cbi ON tm.year_month = cbi.year_month
  LEFT JOIN construction_allocation ca ON tm.year_month = ca.year_month
  LEFT JOIN depreciation_interest di ON tm.year_month = di.year_month
  LEFT JOIN construction_inventory_profit_loss cipl ON tm.year_month = cipl.year_month
),

-- ============================================================
-- 硝子建材営業部の計算: (①+⑩在庫損益+前受け金)×②＋③×④＋⑤×⑥＋⑦×⑧＋⑨×⑧
-- ============================================================
glass_interest AS (
  SELECT
    tm.year_month,
    '長崎支店' AS branch,
    '硝子建材営業部計' AS detail_category,
    (
      -- (①売掛金+⑩在庫損益+前受け金)×②売掛金利率
      (COALESCE(bg.receivables_amount, 0) + COALESCE(gipl.inventory_profit_loss, 0) + COALESCE(gipl.advance_received, 0))
        * COALESCE(rr.interest_rate, 0)
      -- ③×④ 未落手形利息
      + COALESCE(bg.bills_amount, 0) * COALESCE(br.interest_rate, 0)
      -- ⑤×⑥ 在庫利息
      + COALESCE(gi.inventory_amount, 0) * COALESCE(ir.interest_rate, 0)
      -- ⑦×⑧ 建物利息
      + COALESCE(gbi.building_interest, 0) * COALESCE(ga.allocation_ratio, 0)
      -- ⑨×⑧ 償却資産利息
      + COALESCE(di.depreciation_interest, 0) * COALESCE(ga.allocation_ratio, 0)
    ) AS interest_expense
  FROM target_months tm
  LEFT JOIN billing_glass bg ON tm.year_month = bg.year_month
  LEFT JOIN receivables_rate rr ON tm.year_month = rr.year_month
  LEFT JOIN bills_rate br ON tm.year_month = br.year_month
  LEFT JOIN glass_inventory gi ON tm.year_month = gi.year_month
  LEFT JOIN inventory_rate ir ON tm.year_month = ir.year_month
  LEFT JOIN glass_building_interest gbi ON tm.year_month = gbi.year_month
  LEFT JOIN glass_allocation ga ON tm.year_month = ga.year_month
  LEFT JOIN depreciation_interest di ON tm.year_month = di.year_month
  LEFT JOIN glass_inventory_profit_loss gipl ON tm.year_month = gipl.year_month
)

-- ============================================================
-- 統合
-- ============================================================
SELECT year_month, branch, detail_category, interest_expense FROM construction_interest
UNION ALL
SELECT year_month, branch, detail_category, interest_expense FROM glass_interest
UNION ALL
-- 長崎支店計
SELECT
  year_month,
  '長崎支店' AS branch,
  '長崎支店計' AS detail_category,
  SUM(interest_expense) AS interest_expense
FROM (
  SELECT year_month, interest_expense FROM construction_interest
  UNION ALL
  SELECT year_month, interest_expense FROM glass_interest
)
GROUP BY year_month
ORDER BY year_month, detail_category;
