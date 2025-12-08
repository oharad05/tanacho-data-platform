/*
============================================================
DataMart: 累計経営資料（全期間）ダッシュボード表示用SQL - 全支店統合版
============================================================
目的: cumulative_management_documents_all_period_allから表示不要なレコードを除外

除外条件:
  - main_display_flag = 0 かつ main_category NOT IN ('売上高', '売上総利益', '売上総利益率')

用途:
  - Looker Studio / スプレッドシートでの表示用
  - 主要部署以外の詳細項目（営業経費、営業利益、経常利益など）を非表示

データソース:
  - cumulative_management_documents_all_period_all（全データ保持版）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all_for_display` AS
SELECT
  date,
  date_sort_key,
  date_label,
  fiscal_year,
  fiscal_month,
  main_category,
  main_category_sort_order,
  secondary_category,
  secondary_category_sort_order,
  main_department,
  main_department_sort_order,
  secondary_department,
  secondary_department_sort_order,
  main_display_flag,
  monthly_value,
  cumulative_value,
  -- 百万円単位の表示値（金額項目のみ、千円単位の値を1000で割る = 元の値を1000000で割る）
  CASE
    WHEN secondary_category IN ('本年実績(千円)', '本年目標(千円)', '前年実績(千円)', '累積本年実績(千円)', '累積本年目標(千円)')
    THEN cumulative_value / 1000
    ELSE NULL
  END AS display_value_divide_million
FROM `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all`
WHERE NOT (
  main_display_flag = 0
  AND main_category NOT IN ('売上高', '売上総利益', '売上総利益率')
);
