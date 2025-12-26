/*
============================================================
DWH: 営業外収入(リベート・その他) - 全支店統合版
============================================================
目的: 月次の営業外収入(リベート収入とその他収入)を集計グループ別に集計
データソース:
  - 東京支店: ledger_income (元帳_雑収入)
  - 長崎支店: department_summary (部門集計表 コード8730, 8870)
  - 案分比率: ms_allocation_ratio (業務部門案分)
対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - detail_category: 詳細分類(担当者名または部門名)
  - rebate_income: リベート収入(円)
  - other_non_operating_income: その他営業外収入(円)
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_income` AS
WITH tokyo_income AS (
  -- 東京支店: 課単位で集計
  SELECT
    DATE(accounting_month) AS year_month,
    '東京支店' AS branch,
    -- ガラス工事計: 工事営業１課(11) + 業務課(18) (全角・半角対応)
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_construction_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_construction_other,

    -- 山本(改装): 改修課(13) (全角・半角対応)
    SUM(
      CASE
        WHEN own_department_code = 13 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS yamamoto_rebate,
    SUM(
      CASE
        WHEN own_department_code = 13 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS yamamoto_other,

    -- 硝子建材営業部: 硝子建材営業課(20)のみ (仕様書に準拠)
    SUM(
      CASE
        WHEN own_department_code = 20 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_rebate,
    SUM(
      CASE
        WHEN own_department_code = 20 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_other
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
  GROUP BY year_month, branch
),

tokyo_unpivoted AS (
  SELECT year_month, branch, 'ガラス工事計' AS detail_category, glass_construction_rebate AS rebate_income, glass_construction_other AS other_non_operating_income FROM tokyo_income
  UNION ALL
  SELECT year_month, branch, '山本（改装）', yamamoto_rebate, yamamoto_other FROM tokyo_income
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部', glass_sales_rebate, glass_sales_other FROM tokyo_income
),

nagasaki_department_data AS (
  -- 長崎支店: 部門集計表から取得 (コード8730=雑収入リベート, 8870=雑損失その他)
  -- construction_department = データソース#6のY列（工事部）を使用
  SELECT
    sales_accounting_period AS year_month,
    code,
    COALESCE(construction_department, 0) AS construction_dept_amount,
    COALESCE(glass_building_material_sales_department, 0) AS glass_dept_amount,
    COALESCE(operations_department, 0) AS operations_dept_amount
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  WHERE code IN ('8730', '8870')  -- 8730=雑収入(リベート), 8870=雑損失(その他)
),

nagasaki_direct_income AS (
  -- 長崎支店: コード別に集計
  SELECT
    year_month,
    MAX(CASE WHEN code = '8730' THEN construction_dept_amount ELSE 0 END) AS construction_rebate_direct,
    MAX(CASE WHEN code = '8870' THEN construction_dept_amount ELSE 0 END) AS construction_other_direct,
    MAX(CASE WHEN code = '8730' THEN glass_dept_amount ELSE 0 END) AS glass_sales_rebate_direct,
    MAX(CASE WHEN code = '8870' THEN glass_dept_amount ELSE 0 END) AS glass_sales_other_direct,
    MAX(CASE WHEN code = '8730' THEN operations_dept_amount ELSE 0 END) AS operations_rebate,
    MAX(CASE WHEN code = '8870' THEN operations_dept_amount ELSE 0 END) AS operations_other
  FROM nagasaki_department_data
  GROUP BY year_month
),

nagasaki_allocation_ratios AS (
  -- 案分比率の取得 (業務部門案分)
  -- year_monthと同一のyyyymmを持つsource_folderのレコードを使用
  SELECT
    year_month,
    department,
    ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '長崎'
    AND category = '業務部門案分'
    AND source_folder = CAST(FORMAT_DATE('%Y%m', year_month) AS INT64)
),

nagasaki_allocated AS (
  -- 業務部収入の案分計算
  SELECT
    d.year_month,
    '長崎支店' AS branch,
    d.construction_rebate_direct + (d.operations_rebate * COALESCE(r_construction.ratio, 0)) AS construction_rebate_total,
    d.construction_other_direct + (d.operations_other * COALESCE(r_construction.ratio, 0)) AS construction_other_total,
    d.glass_sales_rebate_direct + (d.operations_rebate * COALESCE(r_glass.ratio, 0)) AS glass_sales_rebate_total,
    d.glass_sales_other_direct + (d.operations_other * COALESCE(r_glass.ratio, 0)) AS glass_sales_other_total
  FROM nagasaki_direct_income d
  LEFT JOIN nagasaki_allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事'
  LEFT JOIN nagasaki_allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材'
),

nagasaki_unpivoted AS (
  SELECT year_month, branch, '工事営業部計' AS detail_category, construction_rebate_total AS rebate_income, construction_other_total AS other_non_operating_income FROM nagasaki_allocated
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部計', glass_sales_rebate_total, glass_sales_other_total FROM nagasaki_allocated
),

fukuoka_income AS (
  -- 福岡支店: 元帳_雑収入から取得（仕様書に準拠）
  -- 部門コード: 31=工事, 33=硝子, 34=樹脂, 38=GSセンター, 40-46=福北センター
  SELECT
    DATE(accounting_month) AS year_month,
    '福岡支店' AS branch,
    -- 工事部計: 自部門コード=31(工事営業), 35(内装工事)
    SUM(
      CASE
        WHEN own_department_code IN (31, 35) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS construction_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (31, 35) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS construction_other,

    -- 硝子樹脂計: 自部門コード=33(硝子) + 34(樹脂)
    SUM(
      CASE
        WHEN own_department_code IN (33, 34) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_resin_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (33, 34) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS glass_resin_other,

    -- GSセンター: 自部門コード=38
    SUM(
      CASE
        WHEN own_department_code = 38 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS gs_rebate,
    SUM(
      CASE
        WHEN own_department_code = 38 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS gs_other,

    -- 福北センター: 自部門コード=40,41,42,43,44,45,46
    SUM(
      CASE
        WHEN own_department_code IN (40, 41, 42, 43, 44, 45, 46) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS fukuhoku_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (40, 41, 42, 43, 44, 45, 46) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート|ﾘﾍﾞｰﾄ')
        THEN amount
        ELSE 0
      END
    ) AS fukuhoku_other
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
  GROUP BY year_month, branch
),

fukuoka_unpivoted AS (
  SELECT year_month, branch, '工事部計' AS detail_category, construction_rebate AS rebate_income, construction_other AS other_non_operating_income FROM fukuoka_income
  UNION ALL
  SELECT year_month, branch, '硝子樹脂計', glass_resin_rebate, glass_resin_other FROM fukuoka_income
  UNION ALL
  SELECT year_month, branch, 'GSセンター', gs_rebate, gs_other FROM fukuoka_income
  UNION ALL
  SELECT year_month, branch, '福北センター', fukuhoku_rebate, fukuhoku_other FROM fukuoka_income
)

-- 全支店のデータを統合し、前年データも付与
SELECT
  curr.year_month,
  curr.branch,
  curr.detail_category,
  curr.rebate_income,
  curr.other_non_operating_income,
  prev.rebate_income AS rebate_income_prev_year,
  prev.other_non_operating_income AS other_non_operating_income_prev_year
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
