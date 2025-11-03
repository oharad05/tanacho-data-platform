/*
============================================================
DWH: 経常利益目標
============================================================
目的: 月次の経常利益目標を組織・担当者別に集計
データソース: profit_plan_term
対象月: 前月（CURRENT_DATEから自動計算）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - organization: 組織（工事営業部、硝子建材営業部、東京支店）
  - detail_category: 詳細分類（担当者名または部門名、または「XX計」）
  - target_amount: 目標額（円）

注意:
  - profit_plan_termは横持ち形式のため、縦持ちに変換
  - dwh_sales_actualとJOINしやすいように同じスキーマに統一
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_recurring_profit_target` AS
WITH profit_plan AS (
  SELECT
    period,
    item,
    tokyo_branch_total,
    construction_sales_department_total,
    company_sasaki,
    company_asai,
    company_ogasawara,
    company_takaishi,
    company_yamamoto,
    glass_building_material_sales_department,
    glass_construction,
    building_sash,
    glass_sales,
    sash_sales,
    sash_finished_products,
    others
  FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
  WHERE period = DATE('2025-09-01')
)

-- 経常利益目標
SELECT
  period AS year_month,
  '東京支店' AS organization,
  '東京支店計' AS detail_category,
  tokyo_branch_total AS target_amount
FROM profit_plan
WHERE item = '経常利益'

UNION ALL

SELECT
  period AS year_month,
  '工事営業部' AS organization,
  '工事営業部計' AS detail_category,
  construction_sales_department_total AS target_amount
FROM profit_plan
WHERE item = '経常利益'

UNION ALL

SELECT period, '工事営業部', '佐々木（大成・鹿島他）', company_sasaki FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '工事営業部', '浅井（清水他）', company_asai FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '工事営業部', '小笠原（三井住友他）', company_ogasawara FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '工事営業部', '高石（内装・リニューアル）', company_takaishi FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '工事営業部', '山本（改装）', company_yamamoto FROM profit_plan WHERE item = '経常利益'

UNION ALL

SELECT
  period AS year_month,
  '硝子建材営業部' AS organization,
  '硝子建材営業部計' AS detail_category,
  glass_building_material_sales_department AS target_amount
FROM profit_plan
WHERE item = '経常利益'

UNION ALL

SELECT period, '硝子建材営業部', '硝子工事', glass_construction FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '硝子建材営業部', 'ビルサッシ', building_sash FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '硝子建材営業部', '硝子販売', glass_sales FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '硝子建材営業部', 'サッシ販売', sash_sales FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '硝子建材営業部', 'サッシ完成品', sash_finished_products FROM profit_plan WHERE item = '経常利益'
UNION ALL
SELECT period, '硝子建材営業部', 'その他', others FROM profit_plan WHERE item = '経常利益';
