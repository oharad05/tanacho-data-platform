/*
============================================================
DWH: 営業外費用（社内利息）- 長崎支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: billing_balance, internal_interest, stocks, ms_allocation_ratio
対象支店: 長崎支店
集計単位: 工事営業部、硝子建材営業部

計算ロジック:
  工事営業部・硝子建材営業部共通:
  社内利息 = ①売掛金×②売掛金利率 + ③未落手形×④未落手形利率 + ⑤在庫×⑥在庫利率 + ⑦建物利息×⑧案分比率 + ⑨償却資産利息×⑧案分比率

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - branch: 支店名（長崎支店）
  - detail_category: 詳細分類（工事営業部、硝子建材営業部、長崎支店計）
  - interest_expense: 社内利息（円）

注意事項:
  - 計算対象月の前月データを参照（例: 2025-09-01レポート → 2025-08-01データ参照）
  - 半期の前受け金加算は今回の実装対象外
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` AS
WITH
-- 対象年月の定義（計算対象月の前月）
target_months AS (
  SELECT DISTINCT
    DATE_ADD(sales_month, INTERVAL 1 MONTH) AS year_month,
    sales_month AS reference_month
  FROM `data-platform-prod-475201.corporate_data.billing_balance`
  WHERE branch_code IN (61, 65, 66)
),

-- 【工事営業部】の計算
-- ① 売掛金（営業所コード=061）
construction_receivables AS (
  SELECT
    tm.year_month,
    bb.current_month_sales_balance AS receivables_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code = 61
),

-- ② 売掛金利率
receivables_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金'
),

-- ③ 未落手形（営業所コード=061）
construction_bills AS (
  SELECT
    tm.year_month,
    bb.unsettled_bill_balance AS bills_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code = 61
),

-- ④ 未落手形利率
bills_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '未決済手形'
),

-- ⑤ 在庫・未成工事（工事部門）
construction_inventory AS (
  SELECT
    tm.year_month,
    COALESCE(SUM(s.amount), 0) AS inventory_amount
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = tm.reference_month
    AND s.branch = '長崎'
    AND s.department = '工事'
    AND s.category IN ('期末未成工事', '当月在庫')
  GROUP BY tm.year_month
),

-- ⑥ 在庫利率
inventory_rate AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
),

-- ⑦ 建物利息
building_interest AS (
  SELECT
    year_month,
    interest_rate AS building_interest_amount
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '建物'
),

-- ⑧ 案分比率（工事部門）
construction_allocation_ratio AS (
  SELECT
    year_month,
    ratio AS allocation_ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '長崎'
    AND department = '工事'
    AND category = '減価償却案分'
),

-- ⑨ 償却資産利息
depreciation_interest AS (
  SELECT
    year_month,
    interest_rate AS depreciation_interest_amount
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '償却資産'
),

-- 工事営業部の社内利息計算
construction_interest AS (
  SELECT
    cr.year_month,
    '長崎支店' AS branch,
    '工事営業部計' AS detail_category,
    (
      COALESCE(cr.receivables_amount, 0) * COALESCE(rr.interest_rate, 0) +
      COALESCE(cb.bills_amount, 0) * COALESCE(br.interest_rate, 0) +
      COALESCE(ci.inventory_amount, 0) * COALESCE(ir.interest_rate, 0) +
      COALESCE(bi.building_interest_amount, 0) * COALESCE(car.allocation_ratio, 0) +
      COALESCE(di.depreciation_interest_amount, 0) * COALESCE(car.allocation_ratio, 0)
    ) AS interest_expense
  FROM construction_receivables cr
  LEFT JOIN receivables_rate rr ON cr.year_month = rr.year_month
  LEFT JOIN construction_bills cb ON cr.year_month = cb.year_month
  LEFT JOIN bills_rate br ON cr.year_month = br.year_month
  LEFT JOIN construction_inventory ci ON cr.year_month = ci.year_month
  LEFT JOIN inventory_rate ir ON cr.year_month = ir.year_month
  LEFT JOIN building_interest bi ON cr.year_month = bi.year_month
  LEFT JOIN construction_allocation_ratio car ON cr.year_month = car.year_month
  LEFT JOIN depreciation_interest di ON cr.year_month = di.year_month
),

-- 【硝子建材営業部】の計算
-- ① 売掛金（営業所コード=065 or 066）
glass_receivables AS (
  SELECT
    tm.year_month,
    SUM(bb.current_month_sales_balance) AS receivables_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (65, 66)
  GROUP BY tm.year_month
),

-- ③ 未落手形（営業所コード=065 or 066）
glass_bills AS (
  SELECT
    tm.year_month,
    SUM(bb.unsettled_bill_balance) AS bills_amount
  FROM target_months tm
  INNER JOIN `data-platform-prod-475201.corporate_data.billing_balance` bb
    ON bb.sales_month = tm.reference_month
  WHERE bb.branch_code IN (65, 66)
  GROUP BY tm.year_month
),

-- ⑤ 在庫・未成工事（硝子建材部門）
glass_inventory AS (
  SELECT
    tm.year_month,
    COALESCE(SUM(s.amount), 0) AS inventory_amount
  FROM target_months tm
  LEFT JOIN `data-platform-prod-475201.corporate_data.stocks` s
    ON s.year_month = tm.reference_month
    AND s.branch = '長崎'
    AND s.department = '硝子建材'
    AND s.category IN ('期末未成工事', '当月在庫')
  GROUP BY tm.year_month
),

-- ⑧ 案分比率（硝子建材部門）
glass_allocation_ratio AS (
  SELECT
    year_month,
    ratio AS allocation_ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '長崎'
    AND department = '硝子建材'
    AND category = '減価償却案分'
),

-- 硝子建材営業部の社内利息計算
glass_interest AS (
  SELECT
    gr.year_month,
    '長崎支店' AS branch,
    '硝子建材営業部計' AS detail_category,
    (
      COALESCE(gr.receivables_amount, 0) * COALESCE(rr.interest_rate, 0) +
      COALESCE(gb.bills_amount, 0) * COALESCE(br.interest_rate, 0) +
      COALESCE(gi.inventory_amount, 0) * COALESCE(ir.interest_rate, 0) +
      COALESCE(bi.building_interest_amount, 0) * COALESCE(gar.allocation_ratio, 0) +
      COALESCE(di.depreciation_interest_amount, 0) * COALESCE(gar.allocation_ratio, 0)
    ) AS interest_expense
  FROM glass_receivables gr
  LEFT JOIN receivables_rate rr ON gr.year_month = rr.year_month
  LEFT JOIN glass_bills gb ON gr.year_month = gb.year_month
  LEFT JOIN bills_rate br ON gr.year_month = br.year_month
  LEFT JOIN glass_inventory gi ON gr.year_month = gi.year_month
  LEFT JOIN inventory_rate ir ON gr.year_month = ir.year_month
  LEFT JOIN building_interest bi ON gr.year_month = bi.year_month
  LEFT JOIN glass_allocation_ratio gar ON gr.year_month = gar.year_month
  LEFT JOIN depreciation_interest di ON gr.year_month = di.year_month
)

-- 統合
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
