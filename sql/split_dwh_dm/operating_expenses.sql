/*
============================================================
DWH: 営業経費(本年実績) - 全支店統合版
============================================================
目的: 月次の営業経費を集計グループ別に集計
データソース: department_summary, ms_allocation_ratio
対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - detail_category: 詳細分類(担当者名または部門名)
  - operating_expense_amount: 営業経費額(円)
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.operating_expenses` AS
WITH tokyo_expenses AS (
  -- 東京支店: 課単位で集計
  SELECT
    sales_accounting_period AS year_month,
    '東京支店' AS branch,
    -- ガラス工事計: 工事営業１課 + 業務課
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN construction_sales_section_1 + operations_section
        ELSE 0
      END
    ) AS glass_construction_total,

    -- 山本(改装): 改修課
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN renovation_section
        ELSE 0
      END
    ) AS yamamoto_total,

    -- 硝子建材営業部: 硝子建材営業課
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN glass_building_material_sales_section
        ELSE 0
      END
    ) AS glass_sales_total
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month, branch
),

tokyo_unpivoted AS (
  SELECT year_month, branch, 'ガラス工事計' AS detail_category, glass_construction_total AS operating_expense_amount FROM tokyo_expenses
  UNION ALL
  SELECT year_month, branch, '山本（改装）', yamamoto_total FROM tokyo_expenses
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部', glass_sales_total FROM tokyo_expenses
),

nagasaki_direct_expenses AS (
  -- 長崎支店: 部門単位で集計 + 業務部案分
  SELECT
    sales_accounting_period AS year_month,
    -- 工事営業部(部門61)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
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
          '8331', '8333', '8334', '8335', '8338', '8339',
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
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN operations_department
        ELSE 0
      END
    ) AS operations_total
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month
),

nagasaki_allocation_ratios AS (
  -- 案分比率の取得
  SELECT
    year_month,
    department,
    ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '長崎'
),

nagasaki_allocated AS (
  -- 業務部費用の案分計算
  SELECT
    d.year_month,
    '長崎支店' AS branch,
    d.construction_direct + (d.operations_total * COALESCE(r_construction.ratio, 0)) AS construction_total,
    d.glass_sales_direct + (d.operations_total * COALESCE(r_glass.ratio, 0)) AS glass_sales_total
  FROM nagasaki_direct_expenses d
  LEFT JOIN nagasaki_allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事営業部'
  LEFT JOIN nagasaki_allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材営業部'
),

nagasaki_unpivoted AS (
  SELECT year_month, branch, '工事営業部計' AS detail_category, construction_total AS operating_expense_amount FROM nagasaki_allocated
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部計', glass_sales_total FROM nagasaki_allocated
),

fukuoka_direct_expenses AS (
  -- 福岡支店: 部門単位で集計 + 業務部案分
  SELECT
    sales_accounting_period AS year_month,
    -- 工事部(construction_department)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN construction_department
        ELSE 0
      END
    ) AS construction_direct,

    -- 硝子樹脂部(glass_building_material_sales_department)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN glass_building_material_sales_department
        ELSE 0
      END
    ) AS glass_resin_direct,

    -- GSセンター(gs)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN gs
        ELSE 0
      END
    ) AS gs_direct,

    -- 福北センター(fukuhoku合計)の直接費用
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN (
          fukuhoku_daiwa_glass + fukuhoku_daiwa_welding + fukuhoku_daiwa_branch +
          fukuhoku_nagawa + fukuhoku_moroguchi + fukuhoku_techno + fukuhoku_common
        )
        ELSE 0
      END
    ) AS fukuhoku_direct,

    -- 業務部(operations_department)の費用(案分対象)
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338', '8339',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN operations_department
        ELSE 0
      END
    ) AS operations_total
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  GROUP BY year_month
),

fukuoka_allocation_ratios AS (
  -- 案分比率の取得(業務部門案分のみ)、硝子樹脂は合算
  SELECT
    year_month,
    CASE
      WHEN department IN ('硝子建材', '樹脂建材') THEN '硝子樹脂'
      ELSE department
    END AS department,
    SUM(ratio) AS ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '福岡'
    AND category = '業務部門案分'
  GROUP BY year_month,
    CASE
      WHEN department IN ('硝子建材', '樹脂建材') THEN '硝子樹脂'
      ELSE department
    END
),

fukuoka_allocated AS (
  -- 業務部費用の案分計算
  SELECT
    d.year_month,
    '福岡支店' AS branch,
    d.construction_direct + (d.operations_total * COALESCE(r_construction.ratio, 0)) AS construction_total,
    d.glass_resin_direct + (d.operations_total * COALESCE(r_glass.ratio, 0)) AS glass_resin_total,
    d.gs_direct + (d.operations_total * COALESCE(r_gs.ratio, 0)) AS gs_total,
    d.fukuhoku_direct AS fukuhoku_total  -- 福北センターは案分なし
  FROM fukuoka_direct_expenses d
  LEFT JOIN fukuoka_allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事'
  LEFT JOIN fukuoka_allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子樹脂'
  LEFT JOIN fukuoka_allocation_ratios r_gs
    ON d.year_month = r_gs.year_month
    AND r_gs.department = 'GSセンター'
),

fukuoka_unpivoted AS (
  SELECT year_month, branch, '工事部計' AS detail_category, construction_total AS operating_expense_amount FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, '硝子樹脂計', glass_resin_total FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, 'GSセンター', gs_total FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, '福北センター', fukuhoku_total FROM fukuoka_allocated
)

-- 全支店のデータを統合し、前年データも付与
SELECT
  curr.year_month,
  curr.branch,
  curr.detail_category,
  curr.operating_expense_amount,
  prev.operating_expense_amount AS operating_expense_prev_year
FROM (
  SELECT * FROM tokyo_unpivoted
  UNION ALL
  SELECT * FROM nagasaki_unpivoted
  UNION ALL
  SELECT * FROM fukuoka_unpivoted
) curr
LEFT JOIN (
  SELECT * FROM tokyo_unpivoted
  UNION ALL
  SELECT * FROM nagasaki_unpivoted
  UNION ALL
  SELECT * FROM fukuoka_unpivoted
) prev
  ON DATE_ADD(prev.year_month, INTERVAL 1 YEAR) = curr.year_month
  AND prev.detail_category = curr.detail_category
  AND prev.branch = curr.branch;
