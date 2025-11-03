/*
============================================================
DWH: 営業経費（本年実績）
============================================================
目的: 月次の営業経費を集計グループ別に集計
データソース: department_summary
対象月: 前月（CURRENT_DATEから自動計算）
集計単位: ガラス工事計、山本（改装）、硝子建材営業部

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（ガラス工事計、山本（改装）、硝子建材営業部）
  - operating_expense_amount: 営業経費額（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.operating_expenses` AS
WITH aggregated AS (
  SELECT
    -- ガラス工事計: 工事営業１課 + 業務課
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN construction_sales_section_1 + operations_section
        ELSE 0
      END
    ) AS glass_construction_total,

    -- 山本（改装）: 改修課
    SUM(
      CASE
        WHEN code IN (
          '8331', '8333', '8334', '8335', '8338',
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
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN glass_building_material_sales_section
        ELSE 0
      END
    ) AS glass_sales_total
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  WHERE sales_accounting_period = DATE('2025-09-01')
)

SELECT DATE('2025-09-01') AS year_month, 'ガラス工事計' AS detail_category, glass_construction_total AS operating_expense_amount FROM aggregated
UNION ALL
SELECT DATE('2025-09-01'), '山本（改装）', yamamoto_total FROM aggregated
UNION ALL
SELECT DATE('2025-09-01'), '硝子建材営業部', glass_sales_total FROM aggregated;
