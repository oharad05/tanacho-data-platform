/*
============================================================
DWH: 営業外収入（リベート・その他）
============================================================
目的: 月次の営業外収入（リベート収入とその他収入）を集計グループ別に集計
データソース: ledger_income
対象月: 前月（CURRENT_DATEから自動計算）
集計単位: ガラス工事計、山本（改装）、硝子建材営業部

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（ガラス工事計、山本（改装）、硝子建材営業部）
  - rebate_income: リベート収入（円）
  - other_non_operating_income: その他営業外収入（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_income` AS
WITH aggregated AS (
  SELECT
    DATE(accounting_month) AS year_month,
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

    -- 山本（改装）: 改修課(13)
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
  GROUP BY year_month
)

SELECT year_month, 'ガラス工事計' AS detail_category, glass_construction_rebate AS rebate_income, glass_construction_other AS other_non_operating_income FROM aggregated
UNION ALL
SELECT year_month, '山本（改装）', yamamoto_rebate, yamamoto_other FROM aggregated
UNION ALL
SELECT year_month, '硝子建材営業部', glass_sales_rebate, glass_sales_other FROM aggregated;
