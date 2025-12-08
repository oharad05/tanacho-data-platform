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
  -- 百万円単位の表示値（金額項目のみ、元のvalue値を1000000で割る）
  CASE
    WHEN secondary_category IN ('本年実績(千円)', '本年目標(千円)', '前年実績(千円)', '累積本年実績(千円)', '累積本年目標(千円)')
    THEN value / 1000000
    ELSE NULL
  END AS display_value_divide_million,
  main_display_flag
FROM `data-platform-prod-475201.corporate_data_dm.management_documents_all_period_all`
WHERE NOT (
  main_display_flag = 0
  AND main_category NOT IN ('売上高', '売上総利益', '売上総利益率')
);
