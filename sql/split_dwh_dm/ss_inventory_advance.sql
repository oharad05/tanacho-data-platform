/*
============================================================
DWH: 在庫損益・前受け金（全支店統合）
============================================================
目的: 東京・長崎・福岡の在庫損益・前受け金データを統合
データソース:
  - corporate_data.ss_inventory_advance_tokyo
  - corporate_data.ss_inventory_advance_nagasaki
  - corporate_data.ss_inventory_advance_fukuoka

出力スキーマ:
  - posting_month: 計上月（DATE型）
  - branch_name: 支店名（STRING型）
  - sales_office: 営業所（STRING型）
  - category: カテゴリ（STRING型）
  - inventory_profit_loss: 在庫損益（INTEGER型）
  - advance_received: 前受け金（INTEGER型）
  - input_status: 入力状態（STRING型）
  - created_at: 作成日時（TIMESTAMP型）
  - source_branch: ソース支店識別子（STRING型）

注意: 各テーブルのスキーマが異なるため、共通カラムのみを使用
  - 東京: order_backlogなし、updated_atあり
  - 長崎: order_backlogあり、updated_atあり
  - 福岡: order_backlogあり、updated_atなし
============================================================
*/

CREATE OR REPLACE TABLE `data-platform-prod-475201.corporate_data_dwh.ss_inventory_advance` AS

-- 東京支店
SELECT
  posting_month,
  branch_name,
  sales_office,
  category,
  inventory_profit_loss,
  advance_received,
  input_status,
  created_at,
  '東京' AS source_branch
FROM `data-platform-prod-475201.corporate_data.ss_inventory_advance_tokyo`

UNION ALL

-- 長崎支店
SELECT
  posting_month,
  branch_name,
  sales_office,
  category,
  inventory_profit_loss,
  advance_received,
  input_status,
  created_at,
  '長崎' AS source_branch
FROM `data-platform-prod-475201.corporate_data.ss_inventory_advance_nagasaki`

UNION ALL

-- 福岡支店
SELECT
  posting_month,
  branch_name,
  sales_office,
  category,
  inventory_profit_loss,
  advance_received,
  input_status,
  created_at,
  '福岡' AS source_branch
FROM `data-platform-prod-475201.corporate_data.ss_inventory_advance_fukuoka`

ORDER BY posting_month, source_branch, category;
