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
-- 仕様: 9月レポート（実行日10/1）の場合
--   - billing_balance: sales_month = 8/1（実行日の2ヶ月前）
--   - internal_interest: year_month = 9/1（作成月の前月）
--   - 出力: year_month = 9/1（レポート月）
yamamoto_interest AS (
  SELECT DISTINCT
    ii.year_month AS year_month,  -- レポート月 = internal_interestの月
    '東京支店' AS branch,
    '山本（改装）' AS detail_category,
    -- 売掛残高 × 利率
    bb.current_month_sales_balance * ii.interest_rate AS interest_expense
  FROM
    `data-platform-prod-475201.corporate_data.billing_balance` AS bb
  INNER JOIN
    `data-platform-prod-475201.corporate_data.internal_interest` AS ii
    ON DATE_ADD(bb.sales_month, INTERVAL 1 MONTH) = ii.year_month  -- bb月+1ヶ月 = ii月
  WHERE
    bb.branch_code = 13  -- 改修課
    AND bb.source_folder = CAST(FORMAT_DATE('%Y%m', bb.sales_month) AS INT64)  -- 累積型テーブル対応
    AND ii.branch = '東京支店'
    AND ii.category = '社内利息（A）'
    AND ii.breakdown = '売掛金'
),

-- 部門集計表からの社内利息（全期間）
department_interest AS (
  SELECT
    sales_accounting_period AS year_month,
    'ガラス工事計' AS detail_category,
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
  GROUP BY year_month
),

-- ガラス工事計の社内利息（山本分を除く）
glass_interest AS (
  SELECT
    di.year_month,
    '東京支店' AS branch,
    'ガラス工事計' AS detail_category,
    di.glass_construction_interest - COALESCE(yi.interest_expense, 0) AS interest_expense
  FROM
    department_interest di
  LEFT JOIN
    yamamoto_interest yi
  ON di.year_month = yi.year_month
)

-- 統合
SELECT year_month, branch, detail_category, interest_expense FROM yamamoto_interest
UNION ALL
SELECT year_month, branch, detail_category, interest_expense FROM glass_interest
UNION ALL
SELECT year_month, '東京支店' AS branch, '硝子建材営業部' AS detail_category, glass_sales_interest AS interest_expense
FROM department_interest;
