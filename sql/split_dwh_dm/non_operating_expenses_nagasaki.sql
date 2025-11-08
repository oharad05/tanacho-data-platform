/*
============================================================
DWH: 営業外費用（社内利息） - 長崎支店
============================================================
目的: 月次の営業外費用（社内利息）を集計グループ別に集計(長崎支店)
データソース: internal_interest, stocks
対象月: 前月（CURRENT_DATEから自動計算）
集計単位: 工事営業部計、硝子建材営業部計

計算ロジック:
  - 在庫データ(stocks)から部門別の在庫金額を取得
  - 利率マスタ(internal_interest)から長崎支店の利率を取得
  - 在庫金額 × 利率 で社内利息を計算

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（工事営業部計、硝子建材営業部計）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` AS
WITH
-- 在庫データ（対象月の2ヶ月前のデータを使用）
inventory_data AS (
  SELECT
    DATE_ADD(year_month, INTERVAL 2 MONTH) AS year_month,
    department,
    SUM(amount) AS stock_amount
  FROM `data-platform-prod-475201.corporate_data.stocks`
  WHERE
    branch = '長崎'
  GROUP BY year_month, department
),

-- 利率データ
interest_rates AS (
  SELECT
    year_month,
    interest_rate
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE
    branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown = '棚卸在庫・未成工事支出金'
)

-- 部門別社内利息計算
SELECT
  inv.year_month,
  CASE
    WHEN inv.department_code = 61 THEN '工事営業部計'
    WHEN inv.department_code IN (62, 63) THEN '硝子建材営業部計'
    ELSE '未分類'
  END AS detail_category,
  SUM(inv.stock_amount * COALESCE(ir.interest_rate, 0)) AS interest_expense
FROM inventory_data inv
LEFT JOIN interest_rates ir
  ON inv.year_month = ir.year_month
WHERE inv.department_code IN (61, 62, 63)
GROUP BY inv.year_month, detail_category;
