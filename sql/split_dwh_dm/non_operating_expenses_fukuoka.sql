/*
============================================================
DWH: 営業外費用（社内利息）- 福岡支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: internal_interest, stocks, ms_allocation_ratio
対象支店: 福岡支店
集計単位: 福岡支店計、工事部計、硝子樹脂計、福北センター

計算ロジック（2025-12-18 修正）:
  【修正前の問題】
  - billing_balance × interest_rate で再計算していた
  - internal_interestには既に計算済みの利息額（interest列）が存在
  - 再計算結果と計算済み利息額に107千円の差異が発生

  【修正後のロジック】
  - internal_interestのinterest列（計算済み利息額）を直接使用

  【福岡支店計】
  - internal_interestの「社内利息（A)計」のinterest列を使用

  【工事部計】
  - 売掛金利息: 売掛金・工事・第2工事のinterest
  - 未決済手形利息: 未決済手形のinterest × 工事部案分比率
  - 在庫利息: 棚卸在庫・未成工事支出金のinterest × (工事部在庫/全体在庫)
  - 固定資産利息: (土地+建物+償却資産)のinterest × 工事部案分比率

  【硝子樹脂計】
  - 売掛金利息: 売掛金・硝子樹脂建材のinterest
  - 未決済手形利息: 未決済手形のinterest × 硝子樹脂部案分比率
  - 在庫利息: 棚卸在庫・未成工事支出金のinterest × (硝子樹脂在庫/全体在庫)
  - 固定資産利息: (土地+建物+償却資産)のinterest × 硝子樹脂部案分比率

  【福北センター】
  - 売掛金・福北センターのinterest

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（福岡支店計、工事部計、硝子樹脂計、福北センター）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` AS
WITH
-- ============================================================
-- internal_interestから計算済み利息額を取得
-- ============================================================

-- 福岡支店計（社内利息A計）
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

-- 売掛金利息（工事・第2工事）
construction_receivables_interest AS (
  SELECT
    year_month,
    interest AS receivables_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・工事・第2工事'
),

-- 売掛金利息（硝子樹脂建材）
glass_receivables_interest AS (
  SELECT
    year_month,
    interest AS receivables_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・硝子樹脂建材'
),

-- 未決済手形利息（全体）
bills_interest AS (
  SELECT
    year_month,
    interest AS bills_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '未決済手形'
),

-- 在庫利息（全体）
inventory_interest AS (
  SELECT
    year_month,
    interest AS inventory_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
),

-- 固定資産利息（土地+建物+償却資産）
fixed_asset_interest AS (
  SELECT
    year_month,
    SUM(interest) AS fixed_asset_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown IN ('土地', '建物', '償却資産')
  GROUP BY year_month
),

-- 福北センター利息
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
-- 案分比率の取得
-- ============================================================

-- 工事部案分比率
construction_allocation_ratio AS (
  SELECT
    year_month,
    ratio AS allocation_ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '福岡'
    AND department = '工事'
    AND category = '社内利息案分'
),

-- 硝子樹脂部案分比率（硝子建材+樹脂建材の合計）
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

-- ============================================================
-- 在庫比率の計算（工事部/全体、硝子樹脂/全体）
-- ============================================================

-- 在庫・未成工事（全部署）
total_inventory AS (
  SELECT
    DATE_ADD(year_month, INTERVAL 1 MONTH) AS year_month,
    SUM(amount) AS total_inventory_amount
  FROM `data-platform-prod-475201.corporate_data.stocks`
  WHERE branch = '福岡'
    AND category = '未成工事在庫'
    AND department IN ('工事', 'ビル', '内装工事', '硝子建材', '樹脂')
  GROUP BY year_month
),

-- 在庫・未成工事（工事部）
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

-- 在庫・未成工事（硝子樹脂部）
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

-- ============================================================
-- 工事部計の計算
-- ============================================================
construction_interest AS (
  SELECT
    cri.year_month,
    '工事部計' AS detail_category,
    (
      -- 売掛金利息（工事・第2工事）
      COALESCE(cri.receivables_interest, 0)
      -- 未決済手形利息 × 工事部案分比率
      + COALESCE(bi.bills_interest, 0) * COALESCE(car.allocation_ratio, 0)
      -- 在庫利息 × (工事部在庫/全体在庫)
      + COALESCE(ii.inventory_interest, 0)
        * SAFE_DIVIDE(COALESCE(ci.construction_inventory_amount, 0), COALESCE(ti.total_inventory_amount, 1))
      -- 固定資産利息 × 工事部案分比率
      + COALESCE(fai.fixed_asset_interest, 0) * COALESCE(car.allocation_ratio, 0)
    ) AS interest_expense
  FROM construction_receivables_interest cri
  LEFT JOIN bills_interest bi ON cri.year_month = bi.year_month
  LEFT JOIN inventory_interest ii ON cri.year_month = ii.year_month
  LEFT JOIN fixed_asset_interest fai ON cri.year_month = fai.year_month
  LEFT JOIN construction_allocation_ratio car ON cri.year_month = car.year_month
  LEFT JOIN total_inventory ti ON cri.year_month = ti.year_month
  LEFT JOIN construction_inventory ci ON cri.year_month = ci.year_month
),

-- ============================================================
-- 硝子樹脂計の計算
-- ============================================================
glass_interest AS (
  SELECT
    gri.year_month,
    '硝子樹脂計' AS detail_category,
    (
      -- 売掛金利息（硝子樹脂建材）
      COALESCE(gri.receivables_interest, 0)
      -- 未決済手形利息 × 硝子樹脂部案分比率
      + COALESCE(bi.bills_interest, 0) * COALESCE(gar.allocation_ratio, 0)
      -- 在庫利息 × (硝子樹脂在庫/全体在庫)
      + COALESCE(ii.inventory_interest, 0)
        * SAFE_DIVIDE(COALESCE(gi.glass_inventory_amount, 0), COALESCE(ti.total_inventory_amount, 1))
      -- 固定資産利息 × 硝子樹脂部案分比率
      + COALESCE(fai.fixed_asset_interest, 0) * COALESCE(gar.allocation_ratio, 0)
    ) AS interest_expense
  FROM glass_receivables_interest gri
  LEFT JOIN bills_interest bi ON gri.year_month = bi.year_month
  LEFT JOIN inventory_interest ii ON gri.year_month = ii.year_month
  LEFT JOIN fixed_asset_interest fai ON gri.year_month = fai.year_month
  LEFT JOIN glass_allocation_ratio gar ON gri.year_month = gar.year_month
  LEFT JOIN total_inventory ti ON gri.year_month = ti.year_month
  LEFT JOIN glass_inventory gi ON gri.year_month = gi.year_month
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
