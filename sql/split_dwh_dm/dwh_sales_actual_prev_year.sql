/*
============================================================
DWH: 売上高・粗利実績(前年実績) - 全支店統合版
============================================================
目的: 月次の売上高と粗利の前年実績を支店・組織・担当者/部門別に集計
データソース: dwh_sales_actual（1年前のデータを参照）
対象支店: 東京支店、長崎支店、福岡支店

計算方法:
  dwh_sales_actualのデータを1年シフトして前年実績として使用
  例: 2024-09のdwh_sales_actual → 2025-09の前年実績

出力スキーマ:
  - year_month: 対象年月(DATE型) ※1年後の月として出力
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - organization: 組織(工事営業部、硝子建材営業部など)
  - detail_category: 詳細分類(担当者名または部門名)
  - sales_amount: 前年売上高(円)
  - gross_profit_amount: 前年粗利額(円)
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` AS
SELECT
  DATE_ADD(year_month, INTERVAL 1 YEAR) AS year_month,  -- 1年後の月として出力（2024-09 → 2025-09の前年実績）
  branch,
  organization,
  detail_category,
  sales_amount,
  gross_profit_amount
FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual`;
