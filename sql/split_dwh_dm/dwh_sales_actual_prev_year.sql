/*
============================================================
DWH: 売上高・粗利実績（前年実績）
============================================================
目的: 前年同月の売上高と粗利実績を組織・担当者別に集計
データソース: sales_target_and_achievements
対象月: 前月の1年前（CURRENT_DATEから自動計算）

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - organization: 組織（工事営業部、硝子建材営業部）
  - detail_category: 詳細分類（担当者名または部門名）
  - sales_amount: 売上高（円）
  - gross_profit_amount: 粗利額（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual_prev_year` AS
SELECT
  sales_accounting_period AS year_month,
  -- 組織識別
  CASE
    WHEN branch_code = 11 THEN '工事営業部'
    WHEN branch_code = 25 THEN '硝子建材営業部'
    ELSE 'その他'
  END AS organization,
  -- 担当者・部門別分類
  CASE
    -- 工事営業部の担当者別
    WHEN branch_code = 11 AND staff_name = '佐々木康裕' THEN '佐々木（大成・鹿島他）'
    WHEN branch_code = 11 AND staff_name = '岡本一郎' THEN '岡本（清水他）'
    WHEN branch_code = 11 AND staff_name = '小笠原洋介' THEN '小笠原（三井住友他）'
    WHEN branch_code = 11 AND staff_name = '高石麻友子' THEN '高石（内装・リニューアル）'
    WHEN branch_code = 11 AND staff_name = '浅井一作' THEN '浅井（清水他）'
    WHEN branch_code = 11 AND staff_name = '山本' THEN '山本（改装）'
    -- 硝子建材営業部の部門別
    WHEN branch_code = 25 AND division_code = 11 THEN '硝子工事'
    WHEN branch_code = 25 AND division_code = 21 THEN 'ビルサッシ'
    WHEN branch_code = 25 AND division_code = 10 THEN '硝子販売'
    WHEN branch_code = 25 AND division_code = 20 THEN 'サッシ販売'
    WHEN branch_code = 25 AND division_code = 22 THEN 'サッシ完成品'
    WHEN branch_code = 25 AND division_code IN (12, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN 'その他'
    ELSE '未分類'
  END AS detail_category,
  -- 金額（円単位）- 前年実績カラムを使用
  SUM(prev_year_sales_actual) AS sales_amount,
  SUM(prev_year_gross_profit_actual) AS gross_profit_amount
FROM
  `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
WHERE
  branch_code IN (11, 25)  -- 営業所コード: 011=工事営業部, 025=硝子建材営業部
GROUP BY
  year_month,
  organization,
  detail_category;
