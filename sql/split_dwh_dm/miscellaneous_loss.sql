/*
============================================================
DWH: 営業外費用(雑損失) - 全支店統合版
============================================================
目的: 月次の営業外費用(雑損失)を集計グループ別に集計
データソース:
  - 東京支店: ledger_loss
  - 長崎支店: ledger_loss + ms_allocation_ratio
  - 福岡支店: department_summary (code 8870) + ms_allocation_ratio
対象支店: 東京支店、長崎支店、福岡支店

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - branch: 支店名(東京支店、長崎支店、福岡支店)
  - detail_category: 詳細分類(担当者名または部門名)
  - miscellaneous_loss_amount: 雑損失額(円)
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss` AS
WITH tokyo_loss AS (
  -- 東京支店: 課単位で集計
  SELECT
    DATE(accounting_month) AS year_month,
    '東京支店' AS branch,
    -- ガラス工事計: 工事営業１課(11) + 業務課(18)
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) THEN amount
        ELSE 0
      END
    ) AS glass_construction_loss,

    -- 山本(改装): 改修課(13)
    SUM(
      CASE
        WHEN own_department_code = 13 THEN amount
        ELSE 0
      END
    ) AS yamamoto_loss,

    -- 硝子建材営業部: 硝子建材営業課(20)のみ
    SUM(
      CASE
        WHEN own_department_code = 20 THEN amount
        ELSE 0
      END
    ) AS glass_sales_loss
  FROM `data-platform-prod-475201.corporate_data.ledger_loss`
  GROUP BY year_month, branch
),

tokyo_unpivoted AS (
  SELECT year_month, branch, 'ガラス工事計' AS detail_category, glass_construction_loss AS miscellaneous_loss_amount FROM tokyo_loss
  UNION ALL
  SELECT year_month, branch, '山本（改装）', yamamoto_loss FROM tokyo_loss
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部', glass_sales_loss FROM tokyo_loss
),

nagasaki_direct_loss AS (
  -- 長崎支店: 部門単位で集計 + 業務部案分
  SELECT
    DATE(accounting_month) AS year_month,
    -- 工事営業部(61)の雑損失
    SUM(
      CASE
        WHEN own_department_code = 61 THEN amount
        ELSE 0
      END
    ) AS construction_loss_direct,

    -- 硝子建材営業部(62)の雑損失
    SUM(
      CASE
        WHEN own_department_code = 62 THEN amount
        ELSE 0
      END
    ) AS glass_sales_loss_direct,

    -- 業務部(63)の雑損失(案分対象)
    SUM(
      CASE
        WHEN own_department_code = 63 THEN amount
        ELSE 0
      END
    ) AS operations_loss
  FROM `data-platform-prod-475201.corporate_data.ledger_loss`
  WHERE own_department_code IN (61, 62, 63)  -- 長崎支店の部門コード
  GROUP BY year_month
),

nagasaki_allocation_ratios AS (
  -- 案分比率の取得（業務部門案分のみ）
  -- 同じ月のsource_folderを参照
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
  -- 業務部損失の案分計算
  SELECT
    d.year_month,
    '長崎支店' AS branch,
    d.construction_loss_direct + (d.operations_loss * COALESCE(r_construction.ratio, 0)) AS construction_loss_total,
    d.glass_sales_loss_direct + (d.operations_loss * COALESCE(r_glass.ratio, 0)) AS glass_sales_loss_total
  FROM nagasaki_direct_loss d
  LEFT JOIN nagasaki_allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事'
  LEFT JOIN nagasaki_allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材'
),

nagasaki_unpivoted AS (
  SELECT year_month, branch, '工事営業部計' AS detail_category, construction_loss_total AS miscellaneous_loss_amount FROM nagasaki_allocated
  UNION ALL
  SELECT year_month, branch, '硝子建材営業部計', glass_sales_loss_total FROM nagasaki_allocated
),

fukuoka_department_data AS (
  -- 福岡支店: 部門集計表から取得 (コード8870=雑損失)
  -- 修正: カラム名を正しいものに変更
  --   construction_department → construction_sales_department + construction
  --   glass_building_material_sales_department → glass_building_material + resin
  --   operations_department → operations
  SELECT
    sales_accounting_period AS year_month,
    code,
    COALESCE(construction_sales_department, 0) + COALESCE(construction, 0) AS construction_dept_amount,
    COALESCE(glass_building_material, 0) + COALESCE(resin, 0) AS glass_dept_amount,
    COALESCE(operations, 0) AS operations_dept_amount,
    COALESCE(gs, 0) AS gs_amount,
    COALESCE(
      fukuhoku_daiwa_glass + fukuhoku_daiwa_welding + fukuhoku_daiwa_branch +
      fukuhoku_nagawa + fukuhoku_moroguchi + fukuhoku_techno + fukuhoku_common, 0
    ) AS fukuhoku_amount
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  WHERE code = '8870'  -- 8870=雑損失
),

fukuoka_direct_loss AS (
  SELECT
    year_month,
    MAX(CASE WHEN code = '8870' THEN construction_dept_amount ELSE 0 END) AS construction_loss_direct,
    MAX(CASE WHEN code = '8870' THEN glass_dept_amount ELSE 0 END) AS glass_loss_direct,
    MAX(CASE WHEN code = '8870' THEN operations_dept_amount ELSE 0 END) AS operations_loss,
    MAX(CASE WHEN code = '8870' THEN gs_amount ELSE 0 END) AS gs_loss_direct,
    MAX(CASE WHEN code = '8870' THEN fukuhoku_amount ELSE 0 END) AS fukuhoku_loss_direct
  FROM fukuoka_department_data
  GROUP BY year_month
),

fukuoka_allocation_ratios AS (
  -- 案分比率の取得(業務部門案分のみ)、硝子樹脂は合算
  -- 同じ月のsource_folderを参照
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
    AND source_folder = CAST(FORMAT_DATE('%Y%m', year_month) AS INT64)
  GROUP BY year_month,
    CASE
      WHEN department IN ('硝子建材', '樹脂建材') THEN '硝子樹脂'
      ELSE department
    END
),

fukuoka_allocated AS (
  -- 業務部損失の案分計算
  SELECT
    d.year_month,
    '福岡支店' AS branch,
    d.construction_loss_direct + (d.operations_loss * COALESCE(r_construction.ratio, 0)) AS construction_loss_total,
    d.glass_loss_direct + (d.operations_loss * COALESCE(r_glass.ratio, 0)) AS glass_loss_total,
    d.gs_loss_direct + (d.operations_loss * COALESCE(r_gs.ratio, 0)) AS gs_loss_total,
    d.fukuhoku_loss_direct AS fukuhoku_loss_total  -- 福北センターは案分なし
  FROM fukuoka_direct_loss d
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
  SELECT year_month, branch, '工事部計' AS detail_category, construction_loss_total AS miscellaneous_loss_amount FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, '硝子樹脂計', glass_loss_total FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, 'GSセンター', gs_loss_total FROM fukuoka_allocated
  UNION ALL
  SELECT year_month, branch, '福北センター', fukuhoku_loss_total FROM fukuoka_allocated
)

SELECT * FROM tokyo_unpivoted
UNION ALL
SELECT * FROM nagasaki_unpivoted
UNION ALL
SELECT * FROM fukuoka_unpivoted;
