/*
============================================================
DWH: 営業外費用(雑損失) - 長崎支店
============================================================
目的: 月次の営業外費用(雑損失)を集計グループ別に集計(長崎支店)
      業務部(63)の損失を案分比率に基づき工事営業部と硝子建材営業部に配分
データソース: ledger_loss, ms_allocation_ratio
対象月: 前月(CURRENT_DATEから自動計算)
集計単位: 工事営業部計、硝子建材営業部計

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - detail_category: 詳細分類(工事営業部計、硝子建材営業部計)
  - miscellaneous_loss_amount: 雑損失額(円)

【不明点】
1. ledger_lossテーブルに長崎支店のデータが含まれるか要確認
2. 長崎支店の部門コードが61, 62, 63で正しいか要確認
3. 業務部(63)の損失も案分対象に含まれるか要確認
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss_nagasaki` AS
WITH direct_loss AS (
  -- 直接損失の集計
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

allocation_ratios AS (
  -- 案分比率の取得
  SELECT
    year_month,
    department,
    ratio
  FROM `data-platform-prod-475201.corporate_data.ms_allocation_ratio`
  WHERE branch = '長崎'
),

allocated_loss AS (
  -- 業務部損失の案分計算
  SELECT
    d.year_month,
    d.construction_loss_direct + (d.operations_loss * COALESCE(r_construction.ratio, 0)) AS construction_loss_total,
    d.glass_sales_loss_direct + (d.operations_loss * COALESCE(r_glass.ratio, 0)) AS glass_sales_loss_total
  FROM direct_loss d
  LEFT JOIN allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事営業部'
  LEFT JOIN allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材営業部'
)

SELECT year_month, '工事営業部計' AS detail_category, construction_loss_total AS miscellaneous_loss_amount FROM allocated_loss
UNION ALL
SELECT year_month, '硝子建材営業部計', glass_sales_loss_total FROM allocated_loss;
