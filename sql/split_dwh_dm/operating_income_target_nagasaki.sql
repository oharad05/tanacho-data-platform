/*
============================================================
DWH: 営業利益目標 - 長崎支店
============================================================
目的: 月次の営業利益目標を組織・部門別に集計
データソース: profit_plan_term_nagasaki
対象月: 前月（CURRENT_DATEから自動計算）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - organization: 組織（工事営業部、硝子建材営業部、長崎支店）
  - detail_category: 詳細分類（部門名、または「XX計」）
  - target_amount: 目標額（円）

注意:
  - profit_plan_term_nagasakiは横持ち形式のため、縦持ちに変換
  - dwh_sales_target_nagasakiと同じスキーマに統一
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.operating_income_target_nagasaki` AS
WITH profit_plan AS (
  SELECT
    period,
    item,
    nagasaki_branch_total,
    construction_sales_department_total,
    glass_construction_dept,
    building_sash,
    glass_building_material_sales_department_total,
    glass_construction,
    sash_construction,
    glass_sales,
    sash_sales,
    finished_products
  FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
)

-- 営業利益目標
SELECT
  period AS year_month,
  '長崎支店' AS organization,
  '長崎支店計' AS detail_category,
  nagasaki_branch_total AS target_amount
FROM profit_plan
WHERE item = '営業利益'

UNION ALL

SELECT
  period AS year_month,
  '工事営業部' AS organization,
  '工事営業部計' AS detail_category,
  construction_sales_department_total AS target_amount
FROM profit_plan
WHERE item = '営業利益'

UNION ALL

SELECT period, '工事営業部', 'ガラス工事', glass_construction_dept FROM profit_plan WHERE item = '営業利益'
UNION ALL
SELECT period, '工事営業部', 'ビルサッシ', building_sash FROM profit_plan WHERE item = '営業利益'

UNION ALL

SELECT
  period AS year_month,
  '硝子建材営業部' AS organization,
  '硝子建材営業部計' AS detail_category,
  glass_building_material_sales_department_total AS target_amount
FROM profit_plan
WHERE item = '営業利益'

UNION ALL

SELECT period, '硝子建材営業部', '硝子工事', glass_construction FROM profit_plan WHERE item = '営業利益'
UNION ALL
SELECT period, '硝子建材営業部', 'サッシ工事', sash_construction FROM profit_plan WHERE item = '営業利益'
UNION ALL
SELECT period, '硝子建材営業部', '硝子販売', glass_sales FROM profit_plan WHERE item = '営業利益'
UNION ALL
SELECT period, '硝子建材営業部', 'サッシ販売', sash_sales FROM profit_plan WHERE item = '営業利益'
UNION ALL
SELECT period, '硝子建材営業部', '完成品', finished_products FROM profit_plan WHERE item = '営業利益';
