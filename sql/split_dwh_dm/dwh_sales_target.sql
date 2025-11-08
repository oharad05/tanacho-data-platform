/*
============================================================
DWH: 売上高・粗利目標 - 全支店統合版
============================================================
目的: 月次の売上高と粗利目標を支店・組織・担当者/部門別に集計
データソース:
  - 東京: profit_plan_term (横持ち形式)
  - 長崎: sales_target_and_achievements (縦持ち形式)
対象支店: 東京支店、長崎支店

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - branch: 支店名（東京支店、長崎支店）
  - metric_type: 指標タイプ（'sales'=売上高, 'gross_profit'=売上総利益）
  - organization: 組織（工事営業部、硝子建材営業部、東京支店）
  - detail_category: 詳細分類（担当者名または部門名、または「XX計」）
  - target_amount: 目標額（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target` AS
WITH tokyo_profit_plan AS (
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
),

tokyo_target AS (
  -- 売上高目標
  SELECT period AS year_month, '東京支店' AS branch, 'sales' AS metric_type, '東京支店' AS organization, '東京支店計' AS detail_category, tokyo_branch_total AS target_amount
  FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '工事営業部計', construction_sales_department_total FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '佐々木（大成・鹿島他）', company_sasaki FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '浅井（清水他）', company_asai FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '小笠原（三井住友他）', company_ogasawara FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '高石（内装・リニューアル）', company_takaishi FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '工事営業部', '山本（改装）', company_yamamoto FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', '硝子工事', glass_construction FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', 'ビルサッシ', building_sash FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', '硝子販売', glass_sales FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', 'サッシ販売', sash_sales FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', 'サッシ完成品', sash_finished_products FROM tokyo_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '東京支店', 'sales', '硝子建材営業部', 'その他', others FROM tokyo_profit_plan WHERE item = '売上高'

  UNION ALL

  -- 売上総利益目標
  SELECT period, '東京支店', 'gross_profit', '東京支店', '東京支店計', tokyo_branch_total FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '工事営業部計', construction_sales_department_total FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '佐々木（大成・鹿島他）', company_sasaki FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '浅井（清水他）', company_asai FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '小笠原（三井住友他）', company_ogasawara FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '高石（内装・リニューアル）', company_takaishi FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '工事営業部', '山本（改装）', company_yamamoto FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', '硝子工事', glass_construction FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', 'ビルサッシ', building_sash FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', '硝子販売', glass_sales FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', 'サッシ販売', sash_sales FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', 'サッシ完成品', sash_finished_products FROM tokyo_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '東京支店', 'gross_profit', '硝子建材営業部', 'その他', others FROM tokyo_profit_plan WHERE item = '売上総利益'
),

nagasaki_target AS (
  -- 売上高目標
  SELECT
    sales_accounting_period AS year_month,
    '長崎支店' AS branch,
    'sales' AS metric_type,
    CASE
      WHEN branch_code = 61 THEN '工事営業部'
      WHEN branch_code IN (65, 66) THEN '硝子建材営業部'
      ELSE 'その他'
    END AS organization,
    CASE
      -- 工事営業部(061)の部門別
      WHEN branch_code = 61 AND division_code = 11 THEN 'ガラス工事'
      WHEN branch_code = 61 AND division_code = 21 THEN 'ビルサッシ'
      -- 硝子建材営業部(065, 066)の部門別
      WHEN branch_code IN (65, 66) AND division_code = 11 THEN '硝子工事'
      WHEN branch_code IN (65, 66) AND division_code = 20 THEN 'サッシ工事'
      WHEN branch_code IN (65, 66) AND division_code = 10 THEN '硝子販売'
      WHEN branch_code IN (65, 66) AND division_code IN (22, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN '完成品(その他)'
      ELSE '未分類'
    END AS detail_category,
    SUM(sales_target) AS target_amount
  FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
  WHERE branch_code IN (61, 65, 66)
  GROUP BY year_month, branch, metric_type, organization, detail_category

  UNION ALL

  -- 売上総利益目標
  SELECT
    sales_accounting_period AS year_month,
    '長崎支店' AS branch,
    'gross_profit' AS metric_type,
    CASE
      WHEN branch_code = 61 THEN '工事営業部'
      WHEN branch_code IN (65, 66) THEN '硝子建材営業部'
      ELSE 'その他'
    END AS organization,
    CASE
      -- 工事営業部(061)の部門別
      WHEN branch_code = 61 AND division_code = 11 THEN 'ガラス工事'
      WHEN branch_code = 61 AND division_code = 21 THEN 'ビルサッシ'
      -- 硝子建材営業部(065, 066)の部門別
      WHEN branch_code IN (65, 66) AND division_code = 11 THEN '硝子工事'
      WHEN branch_code IN (65, 66) AND division_code = 20 THEN 'サッシ工事'
      WHEN branch_code IN (65, 66) AND division_code = 10 THEN '硝子販売'
      WHEN branch_code IN (65, 66) AND division_code IN (22, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN '完成品(その他)'
      ELSE '未分類'
    END AS detail_category,
    SUM(gross_profit_target) AS target_amount
  FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
  WHERE branch_code IN (61, 65, 66)
  GROUP BY year_month, branch, metric_type, organization, detail_category
)

SELECT * FROM tokyo_target
UNION ALL
SELECT * FROM nagasaki_target;
