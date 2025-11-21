/*
============================================================
DWH: 本店管理費 - 全支店統合版
============================================================
目的: 月次の本店管理費を集計グループ別に集計
データソース: department_summary
対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - detail_category: 詳細分類(担当者名または部門名)
  - head_office_expense: 本店管理費(円)
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` AS
WITH tokyo_expenses AS (
  -- 東京支店: 課単位で集計
  SELECT
    sales_accounting_period AS year_month,
    '東京支店' AS branch,
    -- ガラス工事計: 工事営業１課 + 業務課
    SUM(
      CASE
        WHEN code = '8366' THEN construction_sales_section_1 + operations_section
        ELSE 0
      END
    ) AS glass_construction_expense,

    -- 山本(改装): 改修課
    SUM(
      CASE
        WHEN code = '8366' THEN renovation_section
        ELSE 0
      END
    ) AS yamamoto_expense,

    -- 硝子建材営業部: 硝子建材営業課
    SUM(
      CASE
        WHEN code = '8366' THEN glass_building_material_sales_section
        ELSE 0
      END
    ) AS glass_sales_expense
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month, branch
),

tokyo_unpivoted AS (
  SELECT year_month, branch, 'ガラス工事計' AS detail_category, glass_construction_expense AS head_office_expense FROM tokyo_expenses
  UNION ALL
  SELECT year_month, branch, '山本（改装）', yamamoto_expense FROM tokyo_expenses
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部', glass_sales_expense FROM tokyo_expenses
),

nagasaki_expenses AS (
  -- 長崎支店: 部門単位で集計
  SELECT
    sales_accounting_period AS year_month,
    '長崎支店' AS branch,
    -- 工事営業部
    SUM(
      CASE
        WHEN code = '8366' THEN construction_department
        ELSE 0
      END
    ) AS construction_expense,

    -- 硝子建材営業部
    SUM(
      CASE
        WHEN code = '8366' THEN glass_building_material_sales_department
        ELSE 0
      END
    ) AS glass_sales_expense
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month, branch
),

nagasaki_unpivoted AS (
  SELECT year_month, branch, '工事営業部計' AS detail_category, construction_expense AS head_office_expense FROM nagasaki_expenses
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部計', glass_sales_expense FROM nagasaki_expenses
),

fukuoka_expenses AS (
  -- 福岡支店: 部門単位で集計
  SELECT
    sales_accounting_period AS year_month,
    '福岡支店' AS branch,
    -- 工事部: 「工事営業部」カラムを使用（営業経費と同様）
    SUM(
      CASE
        WHEN code = '8366' THEN construction_sales_department
        ELSE 0
      END
    ) AS construction_expense,

    -- 硝子樹脂部（硝子建材 + 樹脂建材）
    SUM(
      CASE
        WHEN code = '8366' THEN glass_building_material + resin
        ELSE 0
      END
    ) AS glass_resin_expense,

    -- GSセンター
    SUM(
      CASE
        WHEN code = '8366' THEN gs
        ELSE 0
      END
    ) AS gs_expense,

    -- 福北センター
    SUM(
      CASE
        WHEN code = '8366' THEN (
          fukuhoku_daiwa_glass + fukuhoku_daiwa_welding + fukuhoku_daiwa_branch +
          fukuhoku_nagawa + fukuhoku_moroguchi + fukuhoku_techno + fukuhoku_common
        )
        ELSE 0
      END
    ) AS fukuhoku_expense
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month, branch
),

fukuoka_unpivoted AS (
  SELECT year_month, branch, '工事部計' AS detail_category, construction_expense AS head_office_expense FROM fukuoka_expenses
  UNION ALL
  SELECT year_month, branch, '硝子樹脂計', glass_resin_expense FROM fukuoka_expenses
  UNION ALL
  SELECT year_month, branch, 'GSセンター', gs_expense FROM fukuoka_expenses
  UNION ALL
  SELECT year_month, branch, '福北センター', fukuhoku_expense FROM fukuoka_expenses
)

SELECT * FROM tokyo_unpivoted
UNION ALL
SELECT * FROM nagasaki_unpivoted
UNION ALL
SELECT * FROM fukuoka_unpivoted;
