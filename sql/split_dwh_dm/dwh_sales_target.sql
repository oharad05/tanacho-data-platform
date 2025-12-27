/*
============================================================
DWH: 売上高・粗利目標 - 全支店統合版
============================================================
目的: 月次の売上高と粗利目標を支店・組織・担当者/部門別に集計
データソース:
  - 東京: profit_plan_term (横持ち形式)
  - 長崎: sales_target_and_achievements (縦持ち形式)
  - 福岡: profit_plan_term_fukuoka (横持ち形式)
対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - branch: 支店名（東京支店、長崎支店、福岡支店）
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
  WHERE source_folder = CAST(FORMAT_DATE('%Y%m', period) AS INT64)
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

nagasaki_profit_plan AS (
  SELECT
    period,
    item,
    nagasaki_branch_total,
    glass_construction_dept,
    building_sash,
    construction_sales_department_total,
    glass_construction,
    sash_construction,
    glass_sales,
    sash_sales,
    finished_products,
    glass_building_material_sales_department_total
  FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
  WHERE source_folder = CAST(FORMAT_DATE('%Y%m', period) AS INT64)
),

nagasaki_target AS (
  -- 売上高目標
  SELECT period AS year_month, '長崎支店' AS branch, 'sales' AS metric_type, '長崎支店' AS organization, '長崎支店計' AS detail_category, nagasaki_branch_total AS target_amount
  FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '工事営業部', '工事営業部計', construction_sales_department_total FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '工事営業部', 'ガラス工事', glass_construction_dept FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '工事営業部', 'ビルサッシ', building_sash FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department_total FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', '硝子工事', glass_construction FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', 'サッシ工事', sash_construction FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', '硝子販売', glass_sales FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', 'サッシ販売', sash_sales FROM nagasaki_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '長崎支店', 'sales', '硝子建材営業部', '完成品(その他)', finished_products FROM nagasaki_profit_plan WHERE item = '売上高'

  UNION ALL

  -- 売上総利益目標
  SELECT period, '長崎支店', 'gross_profit', '長崎支店', '長崎支店計', nagasaki_branch_total FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '工事営業部', '工事営業部計', construction_sales_department_total FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '工事営業部', 'ガラス工事', glass_construction_dept FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '工事営業部', 'ビルサッシ', building_sash FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', '硝子建材営業部計', glass_building_material_sales_department_total FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', '硝子工事', glass_construction FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', 'サッシ工事', sash_construction FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', '硝子販売', glass_sales FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', 'サッシ販売', sash_sales FROM nagasaki_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '長崎支店', 'gross_profit', '硝子建材営業部', '完成品(その他)', finished_products FROM nagasaki_profit_plan WHERE item = '売上総利益'
),

fukuoka_profit_plan AS (
  SELECT
    period,
    item,
    fukuoka_branch_total,
    construction_department_total,
    glass_construction,
    building_sash,
    interior_construction,
    glass_resin_total,
    glass,
    building_materials,
    resin,
    gs_center,
    fukuhoku_center
  FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
  WHERE source_folder = CAST(FORMAT_DATE('%Y%m', period) AS INT64)
),

fukuoka_target AS (
  -- 売上高目標（福岡支店の値は千円単位で格納されているため、1000倍して円単位に変換）
  SELECT period AS year_month, '福岡支店' AS branch, 'sales' AS metric_type, '福岡支店' AS organization, '福岡支店計' AS detail_category, fukuoka_branch_total AS target_amount
  FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '工事部', '工事部計', construction_department_total FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '工事部', '硝子工事', glass_construction FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '工事部', 'ビルサッシ', building_sash FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '工事部', '内装工事', interior_construction FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '硝子樹脂部', '硝子樹脂計', glass_resin_total FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '硝子樹脂部', '硝子', glass FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '硝子樹脂部', '建材', building_materials FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '硝子樹脂部', '樹脂', resin FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', 'GSセンター', 'GSセンター', gs_center FROM fukuoka_profit_plan WHERE item = '売上高'
  UNION ALL
  SELECT period, '福岡支店', 'sales', '福北センター', '福北センター', fukuhoku_center FROM fukuoka_profit_plan WHERE item = '売上高'

  UNION ALL

  -- 売上総利益目標（福岡支店の値は千円単位で格納されているため、1000倍して円単位に変換）
  SELECT period, '福岡支店', 'gross_profit', '福岡支店', '福岡支店計', fukuoka_branch_total FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '工事部', '工事部計', construction_department_total FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '工事部', '硝子工事', glass_construction FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '工事部', 'ビルサッシ', building_sash FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '工事部', '内装工事', interior_construction FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '硝子樹脂部', '硝子樹脂計', glass_resin_total FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '硝子樹脂部', '硝子', glass FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '硝子樹脂部', '建材', building_materials FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '硝子樹脂部', '樹脂', resin FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', 'GSセンター', 'GSセンター', gs_center FROM fukuoka_profit_plan WHERE item = '売上総利益'
  UNION ALL
  SELECT period, '福岡支店', 'gross_profit', '福北センター', '福北センター', fukuhoku_center FROM fukuoka_profit_plan WHERE item = '売上総利益'
)

SELECT * FROM tokyo_target
UNION ALL
SELECT * FROM nagasaki_target
UNION ALL
SELECT * FROM fukuoka_target;
