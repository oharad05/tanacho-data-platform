/*
============================================================
DWH: 営業外費用（社内利息）- 福岡支店
============================================================
目的: 月次の営業外費用（社内利息A・B）を部門別に集計
データソース: internal_interest, ms_allocation_ratio
対象支店: 福岡支店
集計単位: 福岡支店計、工事部計、硝子樹脂計、福北センター

計算ロジック（2025-12-29 修正）:
  internal_interestテーブルの「社内利息（A）計」から直接取得し、
  ms_allocation_ratioの社内利息案分比率で部門別に配分
  福北センターはinternal_interestから直接取得

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（福岡支店計、工事部計、硝子樹脂計、福北センター）
  - interest_expense: 社内利息（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_expenses_fukuoka` AS
WITH
-- ============================================================
-- internal_interestから社内利息（A）計を直接取得
-- ============================================================
internal_interest_total AS (
  SELECT
    year_month,
    interest AS total_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown LIKE '%社内利息（A%計%'
),

-- ============================================================
-- 福北センター（internal_interestから直接取得）
-- ============================================================
fukuhoku_interest AS (
  SELECT
    year_month,
    interest AS fukuhoku_interest
  FROM `data-platform-prod-475201.corporate_data.internal_interest`
  WHERE branch = '福岡支店'
    AND category = '社内利息（A）'
    AND breakdown = '売掛金・福北ｾﾝﾀｰ'
),

-- ============================================================
-- ms_allocation_ratio: 社内利息案分比率
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
    AND ar.branch = '福岡'
    AND ar.category = '社内利息案分'
),

-- ============================================================
-- 工事部計・硝子樹脂計の計算
-- (福岡支店計 - 福北センター) × 案分比率
-- ============================================================
distributable_interest AS (
  SELECT
    ii.year_month,
    ii.total_interest - COALESCE(fi.fukuhoku_interest, 0) AS distributable_amount
  FROM internal_interest_total ii
  LEFT JOIN fukuhoku_interest fi ON ii.year_month = fi.year_month
),

department_interest AS (
  SELECT
    di.year_month,
    ar.department,
    CASE ar.department
      WHEN '工事' THEN '工事部計'
      WHEN '硝子建材' THEN '硝子樹脂計_硝子'
      WHEN '樹脂建材' THEN '硝子樹脂計_樹脂'
    END AS detail_category_raw,
    di.distributable_amount * ar.ratio AS interest_expense
  FROM distributable_interest di
  LEFT JOIN allocation_ratio ar ON di.year_month = ar.year_month
  WHERE ar.department IS NOT NULL
),

-- 硝子建材と樹脂建材を合算して硝子樹脂計にする
glass_resin_combined AS (
  SELECT
    year_month,
    '硝子樹脂計' AS detail_category,
    SUM(interest_expense) AS interest_expense
  FROM department_interest
  WHERE detail_category_raw IN ('硝子樹脂計_硝子', '硝子樹脂計_樹脂')
  GROUP BY year_month
),

construction_dept AS (
  SELECT
    year_month,
    '工事部計' AS detail_category,
    interest_expense
  FROM department_interest
  WHERE detail_category_raw = '工事部計'
)

-- ============================================================
-- 統合
-- ============================================================
-- 福岡支店計（internal_interestから直接取得）
SELECT
  year_month,
  '福岡支店計' AS detail_category,
  CAST(total_interest AS FLOAT64) AS interest_expense
FROM internal_interest_total
UNION ALL
-- 工事部計
SELECT year_month, detail_category, interest_expense
FROM construction_dept
UNION ALL
-- 硝子樹脂計
SELECT year_month, detail_category, interest_expense
FROM glass_resin_combined
UNION ALL
-- 福北センター
SELECT
  fi.year_month,
  '福北センター' AS detail_category,
  CAST(fi.fukuhoku_interest AS FLOAT64) AS interest_expense
FROM fukuhoku_interest fi
ORDER BY year_month, detail_category;
