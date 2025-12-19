

/*
============================================================
DataMart: 担当者別_4.当月工事契約受注件数
============================================================
目的: 月別の工事契約受注件数・金額サマリー

データソース:
  - construction_progress_days_amount

表示項目:
  - 月（契約月）
  - 契約件数
  - 契約金額
  - 予定粗利
  - 予定粗利率

集計単位:
  - 支店 × 担当者 × 契約月
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dm.construction_contracts_by_staff`
PARTITION BY property_period
CLUSTER BY main_department, staff_code
AS

WITH base_data AS (
  SELECT
    property_period,
    -- 支店判定（営業所単位）
    -- 東京支店：025 硝子建材営業部
    -- 福岡支店：031 福岡硝子建材営業
    -- 長崎支店：065 長崎硝子建材営１ + 066 長崎硝子建材営２
    CASE
      WHEN CAST(branch_code AS INT64) = 25 THEN '東京支店'
      WHEN CAST(branch_code AS INT64) IN (65, 66) THEN '長崎支店'
      WHEN CAST(branch_code AS INT64) = 31 THEN '福岡支店'
      ELSE 'その他'
    END AS main_department,
    CAST(branch_code AS INT64) AS branch_code,
    branch_name,
    CAST(staff_code AS INT64) AS staff_code,
    staff_name,
    property_number,
    property_name_1,
    contract_date,
    -- 契約月を抽出
    DATE_TRUNC(contract_date, MONTH) AS contract_month,
    CAST(contract_amount AS FLOAT64) AS contract_amount,
    CAST(planned_gross_profit_amount AS FLOAT64) AS planned_gross_profit_amount,
    CAST(planned_gross_profit_rate_display AS FLOAT64) AS planned_gross_profit_rate
  FROM `data-platform-prod-475201.corporate_data.construction_progress_days_amount`
),

-- 担当者×契約月別に集計
monthly_contracts AS (
  SELECT
    property_period,
    main_department,
    branch_code,
    branch_name,
    staff_code,
    staff_name,
    contract_month,
    -- 契約月の月番号（表示用）
    EXTRACT(MONTH FROM contract_month) AS contract_month_number,
    COUNT(DISTINCT property_number) AS contract_count,
    SUM(contract_amount) AS total_contract_amount,
    SUM(planned_gross_profit_amount) AS total_planned_gross_profit
  FROM base_data
  WHERE main_department != 'その他'
    AND contract_date IS NOT NULL
  GROUP BY property_period, main_department, branch_code, branch_name, staff_code, staff_name, contract_month
)

SELECT
  property_period,
  main_department,
  CASE main_department
    WHEN '東京支店' THEN 1
    WHEN '長崎支店' THEN 2
    WHEN '福岡支店' THEN 3
  END AS main_department_sort_order,
  branch_code,
  branch_name,
  staff_code,
  staff_name,
  contract_month,
  contract_month_number,
  -- 月表示用（例: "9月"）
  CONCAT(CAST(contract_month_number AS STRING), '月') AS contract_month_display,

  -- 契約件数
  contract_count,

  -- 契約金額（千円単位）
  ROUND(total_contract_amount / 1000, 0) AS contract_amount,

  -- 予定粗利（千円単位）
  ROUND(total_planned_gross_profit / 1000, 0) AS planned_gross_profit,

  -- 予定粗利率（%）
  CASE WHEN total_contract_amount > 0
    THEN ROUND(total_planned_gross_profit / total_contract_amount * 100, 1)
    ELSE NULL
  END AS planned_gross_profit_rate

FROM monthly_contracts;
