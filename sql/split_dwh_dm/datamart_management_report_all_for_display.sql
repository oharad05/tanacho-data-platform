/*
============================================================
DataMart: 経営資料（全期間）ダッシュボード表示用SQL - 全支店統合版
============================================================
目的: management_documents_all_period_allから表示不要なレコードを除外

除外条件:
  - main_display_flag = 0 かつ main_category NOT IN ('売上高', '売上総利益', '売上総利益率')

用途:
  - Looker Studio / スプレッドシートでの表示用
  - 主要部署以外の詳細項目（営業経費、営業利益、経常利益など）を非表示

データソース:
  - management_documents_all_period_all（全データ保持版）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all_for_display` AS
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
FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all`
WHERE NOT (
  main_display_flag = 0
  AND main_category NOT IN ('売上高', '売上総利益', '売上総利益率')
);
