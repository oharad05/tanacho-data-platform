/*
============================================================
DWH: 損益計算書 入力シート - 全支店統合版
============================================================
目的: 3支店(東京/長崎/福岡)の損益計算書データを統合し、縦持ち形式で格納
データソース:
  - corporate_data.profit_plan_term (東京支店)
  - corporate_data.profit_plan_term_nagasaki (長崎支店)
  - corporate_data.profit_plan_term_fukuoka (福岡支店)

出力スキーマ:
  - period: 期間(DATE型)
  - item: 項目(売上高(千円)、売上総利益(千円)、営業経費(千円)など) ※単位付き
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - detail_category: 詳細分類(部門・担当者名)
  - value: 金額(円) または 比率(売上総利益率の場合)
  - display_flag: 表示フラグ（工事営業部計、硝子建材営業部計、長崎支店計、福岡支店計の場合1）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_profit_plan_term_all` AS

SELECT
  period,
  item,
  branch,
  detail_category,
  value,
  -- 表示フラグ: 工事営業部計、硝子建材営業部計、長崎支店計、福岡支店計の場合1
  CASE
    WHEN detail_category IN ('工事営業部計', '硝子建材営業部計', '長崎支店計', '福岡支店計') THEN 1
    ELSE 0
  END AS display_flag
FROM (

-- 東京支店
SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '東京支店計' AS detail_category,
  tokyo_branch_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE tokyo_branch_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '工事営業部計' AS detail_category,
  construction_sales_department_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE construction_sales_department_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '佐々木（大成・鹿島他）' AS detail_category,
  company_sasaki AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE company_sasaki IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '浅井（清水他）' AS detail_category,
  company_asai AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE company_asai IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '小笠原（三井住友他）' AS detail_category,
  company_ogasawara AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE company_ogasawara IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '高石（内装・リニューアル）' AS detail_category,
  company_takaishi AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE company_takaishi IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  'ガラス工事計' AS detail_category,
  glass_construction_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE glass_construction_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '山本（改装）' AS detail_category,
  company_yamamoto AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE company_yamamoto IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '硝子建材営業部' AS detail_category,
  glass_building_material_sales_department AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE glass_building_material_sales_department IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '硝子工事' AS detail_category,
  glass_construction AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE glass_construction IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  'ビルサッシ' AS detail_category,
  building_sash AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE building_sash IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  '硝子販売' AS detail_category,
  glass_sales AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE glass_sales IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  'サッシ販売' AS detail_category,
  sash_sales AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE sash_sales IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  'サッシ完成品' AS detail_category,
  sash_finished_products AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE sash_finished_products IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '東京支店' AS branch,
  'その他' AS detail_category,
  others AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term`
WHERE others IS NOT NULL

UNION ALL

-- 長崎支店
SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '長崎支店計' AS detail_category,
  nagasaki_branch_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE nagasaki_branch_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '硝子工事課' AS detail_category,
  glass_construction_dept AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE glass_construction_dept IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  'ビルサッシ' AS detail_category,
  building_sash AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE building_sash IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '工事営業部計' AS detail_category,
  construction_sales_department_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE construction_sales_department_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '硝子工事' AS detail_category,
  glass_construction AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE glass_construction IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  'サッシ工事' AS detail_category,
  sash_construction AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE sash_construction IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '硝子販売' AS detail_category,
  glass_sales AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE glass_sales IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  'サッシ販売' AS detail_category,
  sash_sales AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE sash_sales IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '完成品' AS detail_category,
  finished_products AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE finished_products IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '長崎支店' AS branch,
  '硝子建材営業部計' AS detail_category,
  glass_building_material_sales_department_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_nagasaki`
WHERE glass_building_material_sales_department_total IS NOT NULL

UNION ALL

-- 福岡支店
SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '福岡支店計' AS detail_category,
  fukuoka_branch_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE fukuoka_branch_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '工事課計' AS detail_category,
  construction_department_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE construction_department_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '硝子工事' AS detail_category,
  glass_construction AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE glass_construction IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  'ビルサッシ' AS detail_category,
  building_sash AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE building_sash IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '内装工事' AS detail_category,
  interior_construction AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE interior_construction IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '硝子・樹脂計' AS detail_category,
  glass_resin_total AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE glass_resin_total IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '硝子' AS detail_category,
  glass AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE glass IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '建材' AS detail_category,
  building_materials AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE building_materials IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '樹脂' AS detail_category,
  resin AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE resin IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  'GSセンター' AS detail_category,
  gs_center AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE gs_center IS NOT NULL

UNION ALL

SELECT
  period,
  CASE item
    WHEN '売上高' THEN '売上高(千円)'
    WHEN '売上総利益' THEN '売上総利益(千円)'
    WHEN '営業経費' THEN '営業経費(千円)'
    WHEN '営業利益' THEN '営業利益(千円)'
    WHEN '経常利益' THEN '経常利益(千円)'
    WHEN '売上総利益率' THEN '売上総利益率(%)'
    ELSE item
  END AS item,
  '福岡支店' AS branch,
  '福北センター' AS detail_category,
  fukuhoku_center AS value
FROM `data-platform-prod-475201.corporate_data.profit_plan_term_fukuoka`
WHERE fukuhoku_center IS NOT NULL

) AS subquery
;
