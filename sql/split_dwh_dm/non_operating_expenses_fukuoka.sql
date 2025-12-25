/*
============================================================
DWH: 営業外費用（社内利息）- 福岡支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: billing_balance, internal_interest, stocks, ms_allocation_ratio
対象支店: 福岡支店
集計単位: 福岡支店計、工事部計、硝子樹脂計、福北センター

計算ロジック（2025-12-25 仕様書準拠に修正）:
  計算式: ①×②＋③×④＋⑥/⑤×⑦＋⑧×⑨

  参照条件:
  - 実行月の2か月前 = DATE_SUB(year_month, INTERVAL 1 MONTH)
  - source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(year_month, INTERVAL 1 MONTH)) AS INT64)

  【工事部計】
  ① 売掛金: billing_balance, branch_code IN (30, 34)
  ② 売掛金利率: internal_interest, 売掛金・工事・第2工事のinterest_rate
  ③ 未落手形: billing_balance, branch_code IN (30, 34)
  ④ 未落手形利率: internal_interest, 未決済手形のinterest_rate
  ⑤ 在庫（全部署）: stocks, 福岡, 未成工事在庫
  ⑥ 在庫（工事部）: stocks, 福岡, 工事/ビル/内装工事
  ⑦ 在庫利息: internal_interest, 棚卸在庫・未成工事支出金のinterest
  ⑧ 固定資産利息: internal_interest, 土地/建物/償却資産のinterest合計
  ⑨ 案分比率: ms_allocation_ratio, 工事

  【硝子樹脂計】
  ① 売掛金: billing_balance, branch_code IN (31, 32)
  ② 売掛金利率: internal_interest, 売掛金・硝子樹脂建材のinterest_rate
  ③ 未落手形: billing_balance, branch_code IN (31, 32)
  ④ 未落手形利率: internal_interest, 未決済手形のinterest_rate
  ⑤ 在庫（全部署）: stocks, 福岡, 未成工事在庫
  ⑥ 在庫（硝子樹脂部）: stocks, 福岡, 硝子建材/樹脂
  ⑦ 在庫利息: internal_interest, 棚卸在庫・未成工事支出金のinterest
  ⑧ 固定資産利息: internal_interest, 土地/建物/償却資産のinterest合計
  ⑨ 案分比率: ms_allocation_ratio, 硝子建材+樹脂建材

  【福北センター】
  ① 社内利息: internal_interest, 売掛金・福北センターのinterest

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（福岡支店計、工事部計、硝子樹脂計、福北センター）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` AS
WITH
-- ============================================================
-- 対象年月の生成（internal_interestから取得）
-- ============================================================
target_months AS (
  SELECT DISTINCT year_month
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
),

-- ============================================================
-- 福岡支店計（社内利息A計）
-- ============================================================
branch_total_interest AS (
  SELECT
    year_month,
    '福岡支店計' AS detail_category,
    interest AS interest_expense
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '社内利息（A)計'
),

-- ============================================================
-- 福北センター
-- ============================================================
fukuhoku_interest AS (
  SELECT
    year_month,
    '福北センター' AS detail_category,
    interest AS interest_expense
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・福北ｾﾝﾀｰ'
),

-- ============================================================
-- billing_balance: 売掛金・未落手形（実行月2か月前参照）
-- ============================================================

-- 工事部計（branch_code: 30, 34）
billing_construction AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_accounts_receivable) AS accounts_receivable,  -- ①
    SUM(bb.unsettled_bill_balance) AS outstanding_bills                -- ③
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND bb.branch_code IN (30, 34)
    AND bb.source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(tm.year_month, INTERVAL 1 MONTH)) AS INT64)
  GROUP BY tm.year_month
),

-- 硝子樹脂計（branch_code: 31, 32）
billing_glass AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_accounts_receivable) AS accounts_receivable,  -- ①
    SUM(bb.unsettled_bill_balance) AS outstanding_bills                -- ③
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND bb.branch_code IN (31, 32)
    AND bb.source_folder = CAST(FORMAT_DATE('%Y%m', DATE_SUB(tm.year_month, INTERVAL 1 MONTH)) AS INT64)
  GROUP BY tm.year_month
),

-- ============================================================
-- internal_interest: 利率・利息
-- ============================================================

-- ②売掛金利率（工事・第2工事）
rate_construction_receivables AS (
  SELECT
    year_month,
    interest_rate AS receivables_rate  -- ②
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・工事・第2工事'
),

-- ②売掛金利率（硝子樹脂建材）
rate_glass_receivables AS (
  SELECT
    year_month,
    interest_rate AS receivables_rate  -- ②
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・硝子樹脂建材'
),

-- ④未落手形利率
rate_bills AS (
  SELECT
    year_month,
    interest_rate AS bills_rate  -- ④
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '未決済手形'
),

-- ⑦在庫利息
inventory_interest AS (
  SELECT
    year_month,
    interest AS inventory_interest  -- ⑦
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
),

-- ⑧固定資産利息（土地+建物+償却資産）
fixed_asset_interest AS (
  SELECT
    year_month,
    SUM(interest) AS fixed_asset_interest  -- ⑧
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown IN ('土地', '建物', '償却資産')
  GROUP BY year_month
),

-- ============================================================
-- stocks: 在庫（実行月2か月前参照）
-- ============================================================

-- ⑤在庫・未成工事（全部署）
total_inventory AS (
  SELECT
    tm.year_month,
    SUM(s.amount) AS total_amount  -- ⑤
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND s.branch = '福岡'
    AND s.category = '未成工事在庫'
    AND s.department IN ('工事', 'ビル', '内装工事', '硝子建材', '樹脂')
  GROUP BY tm.year_month
),

-- ⑥在庫・未成工事（工事部）
construction_inventory AS (
  SELECT
    tm.year_month,
    SUM(s.amount) AS construction_amount  -- ⑥
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND s.branch = '福岡'
    AND s.category = '未成工事在庫'
    AND s.department IN ('工事', 'ビル', '内装工事')
  GROUP BY tm.year_month
),

-- ⑥在庫・未成工事（硝子樹脂部）
glass_inventory AS (
  SELECT
    tm.year_month,
    SUM(s.amount) AS glass_amount  -- ⑥
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = DATE_SUB(tm.year_month, INTERVAL 1 MONTH)
    AND s.branch = '福岡'
    AND s.category = '未成工事在庫'
    AND s.department IN ('硝子建材', '樹脂')
  GROUP BY tm.year_month
),

-- ============================================================
-- ms_allocation_ratio: 案分比率（source_folder条件付き）
-- ============================================================

-- ⑨工事部案分比率
-- 注: 実行月と同じsource_folderを参照（PDF準拠）
construction_allocation AS (
  SELECT
    tm.year_month,
    ar.ratio AS allocation_ratio  -- ⑨
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.ms_allocation_ratio` ar
    ON ar.year_month = tm.year_month
    AND ar.branch = '福岡'
    AND ar.department = '工事'
    AND ar.category = '社内利息案分'
    AND ar.source_folder = CAST(FORMAT_DATE('%Y%m', tm.year_month) AS INT64)
),

-- ⑨硝子樹脂部案分比率（硝子建材+樹脂建材）
-- 注: 実行月と同じsource_folderを参照（PDF準拠）
glass_allocation AS (
  SELECT
    tm.year_month,
    SUM(ar.ratio) AS allocation_ratio  -- ⑨
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.ms_allocation_ratio` ar
    ON ar.year_month = tm.year_month
    AND ar.branch = '福岡'
    AND ar.department IN ('硝子建材', '樹脂建材')
    AND ar.category = '社内利息案分'
    AND ar.source_folder = CAST(FORMAT_DATE('%Y%m', tm.year_month) AS INT64)
  GROUP BY tm.year_month
),

-- ============================================================
-- 工事部計の計算: ①×②＋③×④＋⑥/⑤×⑦＋⑧×⑨
-- ============================================================
construction_interest AS (
  SELECT
    tm.year_month,
    '工事部計' AS detail_category,
    (
      -- ①×② 売掛金利息
      COALESCE(bc.accounts_receivable, 0) * COALESCE(rcr.receivables_rate, 0)
      -- ③×④ 未落手形利息
      + COALESCE(bc.outstanding_bills, 0) * COALESCE(rb.bills_rate, 0)
      -- ⑥/⑤×⑦ 在庫利息（部門按分）
      + SAFE_DIVIDE(COALESCE(ci.construction_amount, 0), COALESCE(ti.total_amount, 1))
        * COALESCE(ii.inventory_interest, 0)
      -- ⑧×⑨ 固定資産利息（案分）
      + COALESCE(fai.fixed_asset_interest, 0) * COALESCE(ca.allocation_ratio, 0)
    ) AS interest_expense
  FROM target_months tm
  LEFT JOIN billing_construction bc ON tm.year_month = bc.year_month
  LEFT JOIN rate_construction_receivables rcr ON tm.year_month = rcr.year_month
  LEFT JOIN rate_bills rb ON tm.year_month = rb.year_month
  LEFT JOIN total_inventory ti ON tm.year_month = ti.year_month
  LEFT JOIN construction_inventory ci ON tm.year_month = ci.year_month
  LEFT JOIN inventory_interest ii ON tm.year_month = ii.year_month
  LEFT JOIN fixed_asset_interest fai ON tm.year_month = fai.year_month
  LEFT JOIN construction_allocation ca ON tm.year_month = ca.year_month
),

-- ============================================================
-- 硝子樹脂計の計算: ①×②＋③×④＋⑥/⑤×⑦＋⑧×⑨
-- ============================================================
glass_interest AS (
  SELECT
    tm.year_month,
    '硝子樹脂計' AS detail_category,
    (
      -- ①×② 売掛金利息
      COALESCE(bg.accounts_receivable, 0) * COALESCE(rgr.receivables_rate, 0)
      -- ③×④ 未落手形利息
      + COALESCE(bg.outstanding_bills, 0) * COALESCE(rb.bills_rate, 0)
      -- ⑥/⑤×⑦ 在庫利息（部門按分）
      + SAFE_DIVIDE(COALESCE(gi.glass_amount, 0), COALESCE(ti.total_amount, 1))
        * COALESCE(ii.inventory_interest, 0)
      -- ⑧×⑨ 固定資産利息（案分）
      + COALESCE(fai.fixed_asset_interest, 0) * COALESCE(ga.allocation_ratio, 0)
    ) AS interest_expense
  FROM target_months tm
  LEFT JOIN billing_glass bg ON tm.year_month = bg.year_month
  LEFT JOIN rate_glass_receivables rgr ON tm.year_month = rgr.year_month
  LEFT JOIN rate_bills rb ON tm.year_month = rb.year_month
  LEFT JOIN total_inventory ti ON tm.year_month = ti.year_month
  LEFT JOIN glass_inventory gi ON tm.year_month = gi.year_month
  LEFT JOIN inventory_interest ii ON tm.year_month = ii.year_month
  LEFT JOIN fixed_asset_interest fai ON tm.year_month = fai.year_month
  LEFT JOIN glass_allocation ga ON tm.year_month = ga.year_month
)

-- ============================================================
-- 統合
-- ============================================================
SELECT year_month, detail_category, interest_expense FROM branch_total_interest
UNION ALL
SELECT year_month, detail_category, interest_expense FROM construction_interest
UNION ALL
SELECT year_month, detail_category, interest_expense FROM glass_interest
UNION ALL
SELECT year_month, detail_category, interest_expense FROM fukuhoku_interest;
