/*
============================================================
DWH: 営業外費用（雑損失）
============================================================
目的: 月次の営業外費用（雑損失）を集計グループ別に集計
データソース: ledger_loss
対象月: 前月（CURRENT_DATEから自動計算）
集計単位: ガラス工事計、山本（改装）、硝子建材営業部

出力スキーマ:
  - year_month: 対象年月（DATE型）
  - detail_category: 詳細分類（ガラス工事計、山本（改装）、硝子建材営業部）
  - miscellaneous_loss_amount: 雑損失額（円）
============================================================
*/

DECLARE target_month DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH);

WITH aggregated AS (
  SELECT
    -- ガラス工事計: 工事営業１課(11) + 業務課(18)
    SUM(
      CASE
        WHEN own_department_code IN (11, 18) THEN amount
        ELSE 0
      END
    ) AS glass_construction_loss,

    -- 山本（改装）: 改修課(13)
    SUM(
      CASE
        WHEN own_department_code = 13 THEN amount
        ELSE 0
      END
    ) AS yamamoto_loss,

    -- 硝子建材営業部: 硝子建材営業課(20) or 硝子建材営業部(62)
    SUM(
      CASE
        WHEN own_department_code IN (20, 62) THEN amount
        ELSE 0
      END
    ) AS glass_sales_loss
  FROM `data-platform-prod-475201.corporate_data.ledger_loss`
  WHERE DATE(accounting_month) = target_month
)

SELECT target_month AS year_month, 'ガラス工事計' AS detail_category, glass_construction_loss AS miscellaneous_loss_amount FROM aggregated
UNION ALL
SELECT target_month, '山本（改装）', yamamoto_loss FROM aggregated
UNION ALL
SELECT target_month, '硝子建材営業部', glass_sales_loss FROM aggregated;
