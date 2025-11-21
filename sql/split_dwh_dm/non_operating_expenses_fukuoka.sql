/*
============================================================
DWH: 営業外費用（社内利息）- 福岡支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: billing_balance, internal_interest, stocks, ms_allocation_ratio
対象支店: 福岡支店
集計単位: 工事部計、硝子樹脂計、福北センター

計算ロジック:
  【工事部計】
  社内利息 = ①売掛金×②売掛金利率 + ③未落手形×④未落手形利率
           + (⑤在庫全体×⑥工事部比率)×⑦在庫利息
           + ⑧土地建物償却資産利息×⑨工事部案分比率

  【硝子樹脂計】
  社内利息 = ①売掛金×②売掛金利率 + ③未落手形×④未落手形利率
           + (⑤在庫全体×⑥硝子樹脂部比率)×⑦在庫利息
           + ⑧土地建物償却資産利息×⑨硝子樹脂部案分比率

  【福北センター】
  社内利息 = 固定値（internal_interestから取得）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（工事部計、硝子樹脂計、福北センター）
  - interest_expense: 社内利息（円）

注意事項:
  - 計算対象月の前月データを参照（例: 2025-09-01レポート → 2025-08-01データ参照）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` AS
WITH
-- 対象年月の定義（計算対象月の前月）
target_months AS (
  SELECT DISTINCT
    DATE_ADD(sales_month, INTERVAL 1 MONTH) AS year_month,
    sales_month AS reference_month
  FROM `data-platform-prod-475201.corporate_data.billing_balance`
  WHERE branch_code IN (30, 31, 32, 34)
),

-- ============================================================
-- 【工事部計】の計算
-- ============================================================

-- ① 売掛金（営業所コード=030, 034）
construction_receivables AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_sales_balance) AS receivables_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (30, 34)
  GROUP BY tm.year_month
),

-- ② 売掛金利率（売掛金・工事・第2工事）
construction_receivables_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・工事・第2工事'
),

-- ③ 未落手形（営業所コード=030, 034）
construction_bills AS (
  SELECT
    tm.year_month,
    SUM(bb.unsettled_bill_balance) AS bills_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (30, 34)
  GROUP BY tm.year_month
),

-- ④ 未落手形利率
bills_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '未決済手形'
),

-- ⑤ 在庫・未成工事（全部署）
total_inventory AS (
  SELECT
    DATE_ADD(year_month, INTERVAL 1 MONTH) AS year_month,
    SUM(amount) AS total_inventory_amount
  FROM `data-platform-prod-475201.corporate_data.stocks`
  WHERE branch = '福岡'
    AND category = '未成工事在庫'
  GROUP BY year_month
),

-- ⑥ 在庫・未成工事（工事部）
construction_inventory AS (
  SELECT
    DATE_ADD(year_month, INTERVAL 1 MONTH) AS year_month,
    SUM(amount) AS construction_inventory_amount
  FROM `data-platform-prod-475201.corporate_data.stocks`
  WHERE branch = '福岡'
    AND department IN ('工事', 'ビル', '内装工事')
    AND category = '未成工事在庫'
  GROUP BY year_month
),

-- ⑦ 在庫利息（棚卸在庫・未成工事支出金）
inventory_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
),

-- ⑧ 土地・建物・償却資産利息（合計）
construction_fixed_asset_interest AS (
  SELECT
    year_month,
    SUM(interest) AS fixed_asset_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown IN ('土地', '建物', '償却資産')
  GROUP BY year_month
),

-- ⑨ 案分比率（工事部）- 月別に取得
construction_allocation_ratio AS (
  SELECT
    year_month,
    ratio AS allocation_ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '福岡'
    AND department = '工事'
    AND category = '社内利息案分'
),

-- 工事部計の社内利息計算
construction_interest AS (
  SELECT
    cr.year_month,
    '工事部計' AS detail_category,
    (
      -- ①×②: 売掛金 × 売掛金利率
      COALESCE(cr.receivables_amount, 0) * COALESCE(crr.interest_rate, 0)
      -- ③×④: 未落手形 × 未落手形利率
      + COALESCE(cb.bills_amount, 0) * COALESCE(br.interest_rate, 0)
      -- (⑤×⑥/⑤)×⑦: 在庫按分 × 在庫利息
      + COALESCE(ti.total_inventory_amount, 0)
        * COALESCE(ci.construction_inventory_amount, 0)
        / NULLIF(COALESCE(ti.total_inventory_amount, 0), 0)
        * COALESCE(ir.interest_rate, 0)
      -- ⑧×⑨: 固定資産利息 × 案分比率
      + COALESCE(cfai.fixed_asset_interest, 0) * COALESCE(car.allocation_ratio, 0)
    ) AS interest_expense
  FROM construction_receivables cr
  LEFT JOIN construction_receivables_rate crr ON cr.year_month = crr.year_month
  LEFT JOIN construction_bills cb ON cr.year_month = cb.year_month
  LEFT JOIN bills_rate br ON cr.year_month = br.year_month
  LEFT JOIN total_inventory ti ON cr.year_month = ti.year_month
  LEFT JOIN construction_inventory ci ON cr.year_month = ci.year_month
  LEFT JOIN inventory_rate ir ON cr.year_month = ir.year_month
  LEFT JOIN construction_fixed_asset_interest cfai ON cr.year_month = cfai.year_month
  LEFT JOIN construction_allocation_ratio car ON cr.year_month = car.year_month
),

-- ============================================================
-- 【硝子樹脂計】の計算
-- ============================================================

-- ① 売掛金（営業所コード=031, 032）
glass_receivables AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_sales_balance) AS receivables_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (31, 32)
  GROUP BY tm.year_month
),

-- ② 売掛金利率（売掛金・工事・硝子樹脂建材）
glass_receivables_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・工事・硝子樹脂建材'
),

-- ③ 未落手形（営業所コード=031, 032）
glass_bills AS (
  SELECT
    tm.year_month,
    SUM(bb.unsettled_bill_balance) AS bills_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (31, 32)
  GROUP BY tm.year_month
),

-- ⑥ 在庫・未成工事（硝子樹脂部）
glass_inventory AS (
  SELECT
    DATE_ADD(year_month, INTERVAL 1 MONTH) AS year_month,
    SUM(amount) AS glass_inventory_amount
  FROM `data-platform-prod-475201.corporate_data.stocks`
  WHERE branch = '福岡'
    AND department IN ('硝子建材', '樹脂')
    AND category = '未成工事在庫'
  GROUP BY year_month
),

-- ⑧ 土地・建物・償却資産利息（硝子樹脂部も同じ値を使用）

-- ⑨ 案分比率（硝子建材+樹脂建材の合計）- 月別に取得
glass_allocation_ratio AS (
  SELECT
    year_month,
    SUM(ratio) AS allocation_ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '福岡'
    AND department IN ('硝子建材', '樹脂建材')
    AND category = '社内利息案分'
  GROUP BY year_month
),

-- 硝子樹脂計の社内利息計算
glass_interest AS (
  SELECT
    gr.year_month,
    '硝子樹脂計' AS detail_category,
    (
      -- ①×②: 売掛金 × 売掛金利率
      COALESCE(gr.receivables_amount, 0) * COALESCE(grr.interest_rate, 0)
      -- ③×④: 未落手形 × 未落手形利率
      + COALESCE(gb.bills_amount, 0) * COALESCE(br.interest_rate, 0)
      -- (⑤×⑥/⑤)×⑦: 在庫按分 × 在庫利息
      + COALESCE(ti.total_inventory_amount, 0)
        * COALESCE(gi.glass_inventory_amount, 0)
        / NULLIF(COALESCE(ti.total_inventory_amount, 0), 0)
        * COALESCE(ir.interest_rate, 0)
      -- ⑧×⑨: 固定資産利息 × 案分比率
      + COALESCE(cfai.fixed_asset_interest, 0) * COALESCE(gar.allocation_ratio, 0)
    ) AS interest_expense
  FROM glass_receivables gr
  LEFT JOIN glass_receivables_rate grr ON gr.year_month = grr.year_month
  LEFT JOIN glass_bills gb ON gr.year_month = gb.year_month
  LEFT JOIN bills_rate br ON gr.year_month = br.year_month
  LEFT JOIN total_inventory ti ON gr.year_month = ti.year_month
  LEFT JOIN glass_inventory gi ON gr.year_month = gi.year_month
  LEFT JOIN inventory_rate ir ON gr.year_month = ir.year_month
  LEFT JOIN construction_fixed_asset_interest cfai ON gr.year_month = cfai.year_month
  LEFT JOIN glass_allocation_ratio gar ON gr.year_month = gar.year_month
),

-- ============================================================
-- 【福北センター】の計算
-- ============================================================

-- ① 固定値（売掛金・福北センター）
fukuhoku_interest AS (
  SELECT
    year_month,
    '福北センター' AS detail_category,
    interest AS interest_expense
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・福北センター'
)

-- ============================================================
-- 統合
-- ============================================================
SELECT * FROM construction_interest
UNION ALL
SELECT * FROM glass_interest
UNION ALL
SELECT * FROM fukuhoku_interest;
