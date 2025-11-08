/*
============================================================
DWH: 営業経費(本年実績) - 長崎支店
============================================================
目的: 月次の営業経費を集計グループ別に集計(長崎支店)
      業務部(63)の費用を案分比率に基づき工事営業部と硝子建材営業部に配分
データソース: department_summary, ms_allocation_ratio
対象月: 前月(CURRENT_DATEから自動計算)
集計単位: 工事営業部計、硝子建材営業部計

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - detail_category: 詳細分類(工事営業部計、硝子建材営業部計)
  - operating_expense_amount: 営業経費額(円)

【不明点】
1. department_summaryテーブルに長崎支店用のカラムが存在するか要確認
   想定カラム名: construction_department_nagasaki(部門61), glass_sales_department_nagasaki(部門62), operations_department_nagasaki(部門63)
2. 案分比率マスタのテーブル名とスキーマ要確認
   想定: ms_allocation_ratio (year_month, branch, department, ratio)
3. 案分対象の経費コードが東京と同じか要確認
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.operating_expenses_nagasaki` AS
WITH direct_expenses AS (
  -- 直接費用の集計
  SELECT
    sales_accounting_period,
    -- 工事営業部(部門61)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN construction_department
        ELSE 0
      END
    ) AS construction_direct,

    -- 硝子建材営業部(部門62)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN glass_building_material_sales_department
        ELSE 0
      END
    ) AS glass_sales_direct,

    -- 業務部(部門63)の費用(案分対象)
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN operations_department
        ELSE 0
      END
    ) AS operations_total
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY sales_accounting_period
),

allocation_ratios AS (
  -- 案分比率の取得
  SELECT
    year_month,
    department,
    ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`  -- 【要確認】テーブル名
  WHERE branch = '長崎'  -- 【要確認】branchカラムの値
),

allocated_expenses AS (
  -- 業務部費用の案分計算
  SELECT
    d.sales_accounting_period AS year_month,
    d.construction_direct + (d.operations_total * COALESCE(r_construction.ratio, 0)) AS construction_total,
    d.glass_sales_direct + (d.operations_total * COALESCE(r_glass.ratio, 0)) AS glass_sales_total
  FROM direct_expenses d
  LEFT JOIN allocation_ratios r_construction
    ON d.sales_accounting_period = r_construction.year_month
    AND r_construction.department = '工事営業部'
  LEFT JOIN allocation_ratios r_glass
    ON d.sales_accounting_period = r_glass.year_month
    AND r_glass.department = '硝子建材営業部'
)

SELECT year_month, '工事営業部計' AS detail_category, construction_total AS operating_expense_amount FROM allocated_expenses
UNION ALL
SELECT year_month, '硝子建材営業部計', glass_sales_total FROM allocated_expenses;
