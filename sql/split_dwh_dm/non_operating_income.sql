/*
============================================================
DWH: 営業外収入(リベート・その他) - 全支店統合版
============================================================
目的: 月次の営業外収入(リベート収入とその他収入)を集計グループ別に集計
データソース: ledger_income, ms_allocation_ratio
対象支店: 東京支店、長崎支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名(東京支店、長崎支店)
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
    -- ガラス工事計: 工事営業１課(11) + 業務課(18)
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_construction_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_construction_other,

    -- 山本(改装): 改修課(13)
    SUM(
      CASE
        WHEN own_department_code = 13 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS yamamoto_rebate,
    SUM(
      CASE
        WHEN own_department_code = 13 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS yamamoto_other,

    -- 硝子建材営業部: 硝子建材営業課(20) or 硝子建材営業部(62)
    SUM(
      CASE
        WHEN own_department_code IN (20, 62) AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_rebate,
    SUM(
      CASE
        WHEN own_department_code IN (20, 62) AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
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

nagasaki_direct_income AS (
  -- 長崎支店: 部門単位で集計 + 業務部案分
  SELECT
    DATE(accounting_month) AS year_month,
    -- 工事営業部(61)のリベート
    SUM(
      CASE
        WHEN own_department_code = 61 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS construction_rebate_direct,
    -- 工事営業部(61)のその他
    SUM(
      CASE
        WHEN own_department_code = 61 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS construction_other_direct,

    -- 硝子建材営業部(62)のリベート
    SUM(
      CASE
        WHEN own_department_code = 62 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_rebate_direct,
    -- 硝子建材営業部(62)のその他
    SUM(
      CASE
        WHEN own_department_code = 62 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_other_direct,

    -- 業務部(63)のリベート(案分対象)
    SUM(
      CASE
        WHEN own_department_code = 63 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS operations_rebate,
    -- 業務部(63)のその他(案分対象)
    SUM(
      CASE
        WHEN own_department_code = 63 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS operations_other
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
  WHERE own_department_code IN (61, 62, 63)  -- 長崎支店の部門コード
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
    AND r_construction.department = '工事営業部'
  LEFT JOIN nagasaki_allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材営業部'
),

nagasaki_unpivoted AS (
  SELECT year_month, branch, '工事営業部計' AS detail_category, construction_rebate_total AS rebate_income, construction_other_total AS other_non_operating_income FROM nagasaki_allocated
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部計', glass_sales_rebate_total, glass_sales_other_total FROM nagasaki_allocated
)

SELECT * FROM tokyo_unpivoted
UNION ALL
SELECT * FROM nagasaki_unpivoted;
