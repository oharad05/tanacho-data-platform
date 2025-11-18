/*
============================================================
DataMart: 経営資料（全期間）ダッシュボード用SQL（縦持ち形式） - 全支店統合版
============================================================
目的: 東京支店、長崎支店、福岡支店のDataMartを統合DWH参照で統合
対象データ: 全期間データ
組織階層:
  - 東京支店計 > 工事営業部計/硝子建材営業部計 > 担当者別/部門別
  - 長崎支店計 > 工事営業部計/硝子建材営業部計 > 部門別
  - 福岡支店計 > 工事部計/硝子樹脂部計 > 部門別

出力スキーマ:
  - date: 対象月（DATE型）
  - main_category: 大項目（売上高、売上総利益など）
  - secondary_category: 小項目（前年実績、本年目標、本年実績、またはNULL）
  - main_department: 最上位部門（東京支店、長崎支店、福岡支店）
  - secondary_department: 詳細部門（各支店の組織階層）
  - value: 集計値

データソース:
  - management_documents_all_period（東京支店: 統合DWH参照）
  - management_documents_all_period_nagasaki（長崎支店: 統合DWH参照）
  - management_documents_all_period_fukuoka（福岡支店: 統合DWH参照）

注意事項:
  - 金額は円単位でDBに格納、Looker Studioで千円表示
  - 売上総利益率は小数（0.3 = 30%）で格納
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all` AS
SELECT
  date,
  main_category,
  main_category_sort_order,
  secondary_category,
  secondary_category_graphname,
  secondary_category_sort_order,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_newline,
  secondary_department_sort_order,
  value,
  display_value,
  main_display_flag
FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period`

UNION ALL

SELECT
  date,
  main_category,
  main_category_sort_order,
  secondary_category,
  secondary_category_graphname,
  secondary_category_sort_order,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_newline,
  secondary_department_sort_order,
  value,
  display_value,
  main_display_flag
FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_nagasaki`

UNION ALL

SELECT
  date,
  main_category,
  main_category_sort_order,
  secondary_category,
  secondary_category_graphname,
  secondary_category_sort_order,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_newline,
  secondary_department_sort_order,
  value,
  display_value,
  main_display_flag
FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_fukuoka`;
