/*
============================================================
DWH: 営業外費用（社内利息）
============================================================
目的: 月次の営業外費用（社内利息）を集計グループ別に集計
データソース: billing_balance, internal_interest, department_summary
対象月: 2025-09-01
集計単位: ガラス工事計、山本（改装）、硝子建材営業部

計算ロジック:
  - 山本（改装）: 売掛残高 × 利率で計算
  - ガラス工事計: 部門集計表から取得し、山本分を除く
  - 硝子建材営業部: 部門集計表から取得

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（ガラス工事計、山本（改装）、硝子建材営業部）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses` AS
WITH
-- 山本（改装）の社内利息計算
yamamoto_interest AS (
  SELECT
    '山本（改装）' AS detail_category,
    -- 売掛残高 × 利率
    bb.current_month_sales_balance * ii.interest_rate AS interest_expense
  FROM
    `data-platform-prod-475201.corporate_data.billing_balance` AS bb
  INNER JOIN
    `data-platform-prod-475201.corporate_data.internal_interest` AS ii
    ON bb.sales_month = ii.year_month
  WHERE
    bb.sales_month = DATE('2025-07-01')  -- 2か月前のデータ
    AND bb.branch_code = 13  -- 改修課
    AND ii.year_month = DATE('2025-07-01')
    AND ii.branch = '東京支店'
    AND ii.category = '売掛金'
  LIMIT 1
),

-- 部門集計表からの社内利息
department_interest AS (
  WITH aggregated AS (
    SELECT
      -- ガラス工事計: 工事営業１課
      SUM(
        CASE
          WHEN code = '9250' THEN construction_sales_section_1
          ELSE 0
        END
      ) AS glass_construction_interest,

      -- 硝子建材営業部: 硝子建材営業課
      SUM(
        CASE
          WHEN code = '9250' THEN glass_building_material_sales_section
          ELSE 0
        END
      ) AS glass_sales_interest
    FROM `data-platform-prod-475201.corporate_data.department_summary`
    WHERE sales_accounting_period = DATE('2025-09-01')
  )
  SELECT 'ガラス工事計' AS detail_category, glass_construction_interest AS interest_from_summary FROM aggregated
  UNION ALL
  SELECT '硝子建材営業部', glass_sales_interest FROM aggregated
),

-- ガラス工事計の社内利息（山本分を除く）
glass_interest AS (
  SELECT
    'ガラス工事計' AS detail_category,
    di.interest_from_summary - COALESCE(yi.interest_expense, 0) AS interest_expense
  FROM
    department_interest di
  LEFT JOIN
    yamamoto_interest yi
  ON di.detail_category = 'ガラス工事計'
  WHERE
    di.detail_category = 'ガラス工事計'
)

-- 統合
SELECT DATE('2025-09-01') AS year_month, detail_category, interest_expense FROM yamamoto_interest
UNION ALL
SELECT DATE('2025-09-01'), detail_category, interest_expense FROM glass_interest
UNION ALL
SELECT DATE('2025-09-01'), detail_category, interest_from_summary AS interest_expense
FROM department_interest
WHERE detail_category = '硝子建材営業部';
