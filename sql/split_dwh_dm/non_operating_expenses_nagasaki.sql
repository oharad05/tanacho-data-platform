/*
============================================================
DWH: 営業外費用（社内利息）- 長崎支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: internal_interest, ms_allocation_ratio
対象支店: 長崎支店
集計単位: 工事営業部、硝子建材営業部

計算ロジック（2025-12-29 修正）:
  internal_interestテーブルの「社内利息（A）計」から直接取得し、
  ms_allocation_ratioの社内利息案分比率で部門別に配分

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - branch: 支店名（長崎支店）
  - detail_category: 詳細分類（工事営業部計、硝子建材営業部計、長崎支店計）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_nagasaki` AS
WITH
-- ============================================================
-- internal_interestから社内利息（A）計を直接取得
-- ============================================================
internal_interest_total AS (
  SELECT
    year_month,
    interest AS total_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '長崎支店'
    AND category = '社内利息（A）'
    AND breakdown LIKE '%社内利息（A%計%'
),

-- ============================================================
-- ms_allocation_ratio: 減価償却案分比率を使用（長崎支店には社内利息案分がないため）
-- source_folder = 当月のyyyymm, year_month = 当月の日付
-- ============================================================
allocation_ratio AS (
  SELECT
    ii.year_month,
    ar.department,
    ar.ratio
  FROM internal_interest_total ii
  LEFT JOIN `data-platform-prod-475201.corporate_data.ms_allocation_ratio` ar
    ON ar.source_folder = CAST(FORMAT_DATE('%Y%m', ii.year_month) AS INT64)
    AND ar.year_month = ii.year_month
    AND ar.branch = '長崎'
    AND ar.category = '減価償却案分'
),

-- ============================================================
-- 部門別社内利息の計算
-- ============================================================
department_interest AS (
  SELECT
    ii.year_month,
    '長崎支店' AS branch,
    CASE ar.department
      WHEN '工事' THEN '工事営業部計'
      WHEN '硝子建材' THEN '硝子建材営業部計'
    END AS detail_category,
    ii.total_interest * ar.ratio AS interest_expense
  FROM internal_interest_total ii
  LEFT JOIN allocation_ratio ar ON ii.year_month = ar.year_month
  WHERE ar.department IS NOT NULL
)

-- ============================================================
-- 統合
-- ============================================================
SELECT year_month, branch, detail_category, interest_expense
FROM department_interest
UNION ALL
-- 長崎支店計（internal_interestから直接取得）
SELECT
  year_month,
  '長崎支店' AS branch,
  '長崎支店計' AS detail_category,
  total_interest AS interest_expense
FROM internal_interest_total
ORDER BY year_month, detail_category;
