/*
============================================================
DWH: 売上高・粗利目標 - 長崎支店
============================================================
目的: 月次の売上高と粗利目標を組織・部門別に集計(長崎支店)
データソース: sales_target_and_achievements
対象月: 前月（CURRENT_DATEから自動計算）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - metric_type: 指標タイプ（'sales'=売上高, 'gross_profit'=売上総利益）
  - organization: 組織（工事営業部、硝子建材営業部）
  - detail_category: 詳細分類（部門名）
  - target_amount: 目標額（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_sales_target_nagasaki` AS
-- 売上高目標
SELECT
  sales_accounting_period AS year_month,
  'sales' AS metric_type,
  CASE
    WHEN branch_code = 061 THEN '工事営業部'
    WHEN branch_code IN (065, 066) THEN '硝子建材営業部'
    ELSE 'その他'
  END AS organization,
  CASE
    -- 工事営業部(061)の部門別
    WHEN branch_code = 061 AND division_code = 11 THEN 'ガラス工事'
    WHEN branch_code = 061 AND division_code = 21 THEN 'ビルサッシ'
    -- 硝子建材営業部(065, 066)の部門別
    WHEN branch_code IN (065, 066) AND division_code = 11 THEN '硝子工事'
    WHEN branch_code IN (065, 066) AND division_code = 20 THEN 'サッシ工事'
    WHEN branch_code IN (065, 066) AND division_code = 10 THEN '硝子販売'
    WHEN branch_code IN (065, 066) AND division_code = 20 THEN 'サッシ販売'
    WHEN branch_code IN (065, 066) AND division_code IN (22, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN '完成品(その他)'
    ELSE '未分類'
  END AS detail_category,
  SUM(sales_target) AS target_amount
FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
WHERE branch_code IN (061, 065, 066)
GROUP BY year_month, metric_type, organization, detail_category

UNION ALL

-- 売上総利益目標
SELECT
  sales_accounting_period AS year_month,
  'gross_profit' AS metric_type,
  CASE
    WHEN branch_code = 061 THEN '工事営業部'
    WHEN branch_code IN (065, 066) THEN '硝子建材営業部'
    ELSE 'その他'
  END AS organization,
  CASE
    -- 工事営業部(061)の部門別
    WHEN branch_code = 061 AND division_code = 11 THEN 'ガラス工事'
    WHEN branch_code = 061 AND division_code = 21 THEN 'ビルサッシ'
    -- 硝子建材営業部(065, 066)の部門別
    WHEN branch_code IN (065, 066) AND division_code = 11 THEN '硝子工事'
    WHEN branch_code IN (065, 066) AND division_code = 20 THEN 'サッシ工事'
    WHEN branch_code IN (065, 066) AND division_code = 10 THEN '硝子販売'
    WHEN branch_code IN (065, 066) AND division_code = 20 THEN 'サッシ販売'
    WHEN branch_code IN (065, 066) AND division_code IN (22, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN '完成品(その他)'
    ELSE '未分類'
  END AS detail_category,
  SUM(gross_profit_target) AS target_amount
FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
WHERE branch_code IN (061, 065, 066)
GROUP BY year_month, metric_type, organization, detail_category;
