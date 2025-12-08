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
  -- 百万円単位の表示値（金額項目は千円単位の値を1000で割る = 元の値を1000000で割る、%項目はそのまま）
  CASE
    WHEN secondary_category IN ('本年実績(千円)', '本年目標(千円)', '前年実績(千円)', '累積本年実績(千円)', '累積本年目標(千円)')
    THEN cumulative_value / 1000
    WHEN secondary_category LIKE '%(%)'
    THEN cumulative_value
    ELSE NULL
  END AS display_value_divide_million,
  -- 各支店計の表示フラグ
  CASE
    WHEN (main_department = '東京支店' AND secondary_department = '東京支店計')
      OR (main_department = '長崎支店' AND secondary_department = '長崎支店計')
      OR (main_department = '福岡支店' AND secondary_department = '福岡支店計')
    THEN 1
    ELSE 0
  END AS sales_perform_display_flag,
  -- 受注残高表示用フラグ
  CASE
    WHEN (main_department = '東京支店' AND secondary_department = '工事営業部計')
      OR (main_department = '東京支店' AND secondary_department = '硝子建材営業部計')
      OR (main_department = '長崎支店' AND secondary_department = '長崎支店計')
      OR (main_department = '福岡支店' AND secondary_department = '福岡支店計')
    THEN 1
    ELSE 0
  END AS order_backlog_flag,
  -- secondary_categoryから(千円)と(%)を削除したカラム
  REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') AS sales_performance_category_name,
  -- 売上実績表示用ソート順
  CASE
    WHEN REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') = '本年目標' THEN 1
    WHEN REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') = '本年実績' THEN 2
    WHEN REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') = '目標比' THEN 3
    WHEN REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') = '前年実績' THEN 4
    WHEN REGEXP_REPLACE(secondary_category, r'\(千円\)|\(%\)', '') = '前年比' THEN 5
    ELSE NULL
  END AS sales_performance_category_sort_order
FROM `data-platform-prod-475201.corporate_data_dm.cumulative_management_documents_all_period_all`
WHERE NOT (
  main_display_flag = 0
  AND main_category NOT IN ('売上高', '売上総利益', '売上総利益率')
);
