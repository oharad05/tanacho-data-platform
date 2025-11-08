/*
============================================================
DWH: 営業外収入(リベート・その他) - 長崎支店
============================================================
目的: 月次の営業外収入(リベート収入とその他収入)を集計グループ別に集計(長崎支店)
      業務部(63)の収入を案分比率に基づき工事営業部と硝子建材営業部に配分
データソース: ledger_income, ms_allocation_ratio
対象月: 前月(CURRENT_DATEから自動計算)
集計単位: 工事営業部計、硝子建材営業部計

出力スキーマ:
  - year_month: 対象年月(DATE型)
  - detail_category: 詳細分類(工事営業部計、硝子建材営業部計)
  - rebate_income: リベート収入(円)
  - other_non_operating_income: その他営業外収入(円)

【不明点】
1. ledger_incomeテーブルに長崎支店のデータが含まれるか要確認
2. 長崎支店の部門コードが61, 62, 63で正しいか要確認
3. 業務部(63)の収入も案分対象に含まれるか要確認
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.non_operating_income_nagasaki` AS
WITH direct_income AS (
  -- 直接収入の集計
  SELECT
    DATE(accounting_month) AS year_month,
    -- 工事営業部(61)のリベート
    SUM(
      CASE
        WHEN own_department_code = 61 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS construction_rebate_direct,
    -- 工事営業部(61)のその他
    SUM(
      CASE
        WHEN own_department_code = 61 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS construction_other_direct,

    -- 硝子建材営業部(62)のリベート
    SUM(
      CASE
        WHEN own_department_code = 62 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_rebate_direct,
    -- 硝子建材営業部(62)のその他
    SUM(
      CASE
        WHEN own_department_code = 62 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS glass_sales_other_direct,

    -- 業務部(63)のリベート(案分対象)
    SUM(
      CASE
        WHEN own_department_code = 63 AND REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS operations_rebate,
    -- 業務部(63)のその他(案分対象)
    SUM(
      CASE
        WHEN own_department_code = 63 AND NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS operations_other
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
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

allocated_income AS (
  -- 業務部収入の案分計算
  SELECT
    d.year_month,
    d.construction_rebate_direct + (d.operations_rebate * COALESCE(r_construction.ratio, 0)) AS construction_rebate_total,
    d.construction_other_direct + (d.operations_other * COALESCE(r_construction.ratio, 0)) AS construction_other_total,
    d.glass_sales_rebate_direct + (d.operations_rebate * COALESCE(r_glass.ratio, 0)) AS glass_sales_rebate_total,
    d.glass_sales_other_direct + (d.operations_other * COALESCE(r_glass.ratio, 0)) AS glass_sales_other_total
  FROM direct_income d
  LEFT JOIN allocation_ratios r_construction
    ON d.year_month = r_construction.year_month
    AND r_construction.department = '工事営業部'
  LEFT JOIN allocation_ratios r_glass
    ON d.year_month = r_glass.year_month
    AND r_glass.department = '硝子建材営業部'
)

SELECT year_month, '工事営業部計' AS detail_category, construction_rebate_total AS rebate_income, construction_other_total AS other_non_operating_income FROM allocated_income
UNION ALL
SELECT year_month, '硝子建材営業部計', glass_sales_rebate_total, glass_sales_other_total FROM allocated_income;
