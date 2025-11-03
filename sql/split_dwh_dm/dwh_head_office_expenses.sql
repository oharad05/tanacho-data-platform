/*
============================================================
DWH: 本店管理費
============================================================
目的: 月次の本店管理費を集計グループ別に集計
データソース: department_summary
対象月: 前月（CURRENT_DATEから自動計算）
集計単位: ガラス工事計、山本（改装）、硝子建材営業部

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（ガラス工事計、山本（改装）、硝子建材営業部）
  - head_office_expense: 本店管理費（円）
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.head_office_expenses` AS
WITH aggregated AS (
  SELECT
    -- ガラス工事計: 工事営業１課 + 業務課
    SUM(
      CASE
        WHEN code = '8366' THEN construction_sales_section_1 + operations_section
        ELSE 0
      END
    ) AS glass_construction_expense,

    -- 山本（改装）: 改修課
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
  WHERE sales_accounting_period = DATE('2025-09-01')
)

SELECT DATE('2025-09-01') AS year_month, 'ガラス工事計' AS detail_category, glass_construction_expense AS head_office_expense FROM aggregated
UNION ALL
SELECT DATE('2025-09-01'), '山本（改装）', yamamoto_expense FROM aggregated
UNION ALL
SELECT DATE('2025-09-01'), '硝子建材営業部', glass_sales_expense FROM aggregated;
