-- sales_target テーブルを作成
CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.sales_target`
PARTITION BY DATE_TRUNC(year_month, MONTH)
CLUSTER BY metric_type, organization, detail_category
AS
SELECT * FROM (
DECLARE target_month DATE DEFAULT DATE('2025-09-01');

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
  WHERE period = target_month
)

-- 売上高目標
SELECT
  period AS year_month,
  'sales' AS metric_type,
  '東京支店' AS organization,
  '東京支店計' AS detail_category,
  tokyo_branch_total AS target_amount
FROM profit_plan
WHERE item = '売上高'

UNION ALL

SELECT period, 'sales', '工事営業部', '工事営業部計', construction_sales_department_total FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '工事営業部', '佐々木（大成・鹿島他）', company_sasaki FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '工事営業部', '岡本（清水他）', company_asai FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '工事営業部', '小笠原（三井住友他）', company_ogasawara FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '工事営業部', '高石（内装・リニューアル）', company_takaishi FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '工事営業部', '山本（改装）', company_yamamoto FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', '硝子工事', glass_construction FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', 'ビルサッシ', building_sash FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', '硝子販売', glass_sales FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', 'サッシ販売', sash_sales FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', 'サッシ完成品', sash_finished_products FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'sales', '硝子建材営業部', 'その他', others FROM profit_plan WHERE item = '売上高'
UNION ALL
SELECT period, 'gross_profit', '東京支店', '東京支店計', tokyo_branch_total FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '工事営業部計', construction_sales_department_total FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '佐々木（大成・鹿島他）', company_sasaki FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '岡本（清水他）', company_asai FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '小笠原（三井住友他）', company_ogasawara FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '高石（内装・リニューアル）', company_takaishi FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '工事営業部', '山本（改装）', company_yamamoto FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', '硝子工事', glass_construction FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', 'ビルサッシ', building_sash FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', '硝子販売', glass_sales FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', 'サッシ販売', sash_sales FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', 'サッシ完成品', sash_finished_products FROM profit_plan WHERE item = '売上総利益'
UNION ALL
SELECT period, 'gross_profit', '硝子建材営業部', 'その他', others FROM profit_plan WHERE item = '売上総利益'
);
