-- sales_target_and_achievementsテーブルを正しいスキーマで再作成
CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data.sales_target_and_achievements` (
sales_accounting_period DATE,
branch_code INT64,
branch_name STRING,
department_code INT64,
department_name STRING,
staff_code INT64,
staff_name STRING,
major_department_code INT64,
major_department_name STRING,
division_code INT64,
division_name STRING,
sales_target INT64,
sales_actual INT64,
gross_profit_target INT64,
gross_profit_actual INT64,
prev_year_sales_target INT64,
prev_year_sales_actual INT64,
prev_year_gross_profit_target INT64,
prev_year_gross_profit_actual INT64
)
PARTITION BY sales_accounting_period
CLUSTER BY branch_code;