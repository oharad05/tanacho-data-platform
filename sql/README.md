# 経営資料ダッシュボードSQL

## 概要

BigQueryのデータをLooker Studioで可視化するためのSQLクエリです。
月次の損益計算書（P/L）を組織階層別に集計します。

## ファイル構成

```
sql/
├── README.md                                    # このファイル
├── scripts/                                      # 実行スクリプト
│   ├── update_dwh.sh                             # DWH更新スクリプト
│   ├── update_datamart.sh                        # DataMart更新スクリプト
│   └── update_all.sh                             # 一括更新スクリプト
└── split_dwh_dm/                                 # DWH/DM SQLファイル
    ├── dwh_sales_actual.sql                      # 売上実績
    ├── dwh_sales_actual_prev_year.sql            # 売上実績（前年）
    ├── dwh_sales_target.sql                      # 売上目標
    ├── dwh_operating_expenses.sql                # 営業経費
    ├── dwh_non_operating_income.sql              # 営業外収入
    ├── dwh_non_operating_expenses.sql            # 営業外費用
    ├── dwh_miscellaneous_loss.sql                # 雑損失
    ├── dwh_head_office_expenses.sql              # 本店管理費
    ├── dwh_recurring_profit_target.sql           # 経常利益目標
    └── datamart_management_report_vertical.sql   # 経営資料（縦持ち）
```

## データアーキテクチャ

```
corporate_data（生データ）
    ↓
corporate_data_dwh（中間テーブル）
    ↓
corporate_data_dm（Looker Studio用DataMart）
```

- **corporate_data**: Google Driveから取り込んだ生データ
- **corporate_data_dwh**: 指標別に加工した中間テーブル（9テーブル）
- **corporate_data_dm**: Looker Studioで参照する最終テーブル（縦持ち形式）

## 使用方法

### 1. 日付パラメータの自動化

全てのSQLファイルは `DECLARE` 文で日付を自動計算します：

```sql
DECLARE target_month DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH);
```

- **target_month**: 実行日の前月の1日（例: 10月に実行 → 2025-09-01）
- **prev_year_month**: 前年同月（例: 2024-09-01）
- **two_months_ago**: 2か月前（営業外費用の計算用）

**手動で日付を変更する場合**:
```sql
-- 例: 2025年9月のデータを処理したい場合
DECLARE target_month DATE DEFAULT DATE('2025-09-01');
```

### 2. コマンドラインからの実行

#### 2-1. 全テーブル一括更新（推奨）

```bash
cd /Users/oharayousuke/tanacho-pipeline
bash sql/scripts/update_all.sh
```

DWH → DataMart の順に全テーブルを更新します。

#### 2-2. DWHのみ更新

```bash
bash sql/scripts/update_dwh.sh
```

9つの中間テーブル（dwh_*）を更新します。

#### 2-3. DataMartのみ更新

```bash
bash sql/scripts/update_datamart.sh
```

Looker Studio用の最終テーブル（management_documents_current_month_tbl）を更新します。

**注意**: DataMart更新前にDWHが最新である必要があります。

### 3. BigQuery Scheduled Queryでの自動化

#### 3-1. Scheduled Queryの作成

```bash
# 1. BigQueryコンソールにアクセス
# 2. 左メニュー「スケジュールされたクエリ」→「クエリをスケジュール」

# 3. 以下の設定を入力:
# - 名前: update_management_report_monthly
# - スケジュール: 月次 - 毎月1日 午前3時
# - データセット: corporate_data_dm
# - テーブル: management_documents_current_month_tbl
# - 書き込み設定: 上書き（WRITE_TRUNCATE）
```

#### 3-2. クエリ内容

`sql/scripts/update_all.sh` の内容を順番にScheduled Queryとして登録します。

**推奨スケジュール**:
- **DWH更新**: 毎月1日 午前2時
- **DataMart更新**: 毎月1日 午前3時（DWH更新の1時間後）

#### 3-3. 実行順序の制御

BigQuery Scheduled Queryには依存関係設定がないため、以下のいずれかで対応：

**Option A: Cloud Composerを使用（推奨）**
```python
# Apache Airflow DAG
dwh_tasks = [create_dwh_sales_actual, create_dwh_sales_target, ...]
datamart_task = create_datamart

dwh_tasks >> datamart_task  # 依存関係を設定
```

**Option B: Scheduled Queryを時間差で実行**
- DWH: 毎月1日 02:00
- DataMart: 毎月1日 03:00（1時間差）

**Option C: シェルスクリプトをCloud Functionsで実行**
```bash
# Cloud Functionsから update_all.sh を実行
# Cloud Schedulerで月次実行をトリガー
```

### 4. Looker Studioでの利用

1. Looker Studioで新規データソースを作成
2. BigQueryコネクタを選択
3. テーブルを選択:
   - プロジェクト: `data-platform-prod-475201`
   - データセット: `corporate_data_dm`
   - テーブル: `management_documents_current_month_tbl`
4. main_category_sort_order, secondary_category_sort_order, secondary_department_sort_orderを使用して表示順を制御

## DataMart出力スキーマ（縦持ち形式）

DataMart（management_documents_current_month_tbl）は**縦持ち形式**で出力されます：

| カラム名 | 型 | 説明 | 例 |
|---------|-----|------|-----|
| `date` | DATE | 対象年月 | 2025-09-01 |
| `main_category` | STRING | 主要指標 | 売上高 |
| `main_category_sort_order` | INTEGER | 主要指標の表示順 | 1 |
| `secondary_category` | STRING | 区分（前年実績/本年目標/本年実績/前年比/目標比） | 本年実績 |
| `secondary_category_sort_order` | INTEGER | 区分の表示順 | 3 |
| `main_department` | STRING | 主要部門 | 東京支店 |
| `secondary_department` | STRING | 詳細部門 | 佐々木（大成・鹿島他） |
| `secondary_department_sort_order` | INTEGER | 詳細部門の表示順 | 3 |
| `value` | FLOAT | 実際の値（円単位） | 150000000.0 |
| `display_value` | FLOAT | 表示用の値（千円/パーセント） | 150000.0 |

### 主要指標（main_category）の種類と表示順

| 順序 | 指標名 | 単位 |
|-----|--------|------|
| 1 | 売上高 | 千円 |
| 2 | 売上総利益 | 千円 |
| 3 | 売上総利益率 | % |
| 4 | 営業経費 | 千円 |
| 5 | 営業利益 | 千円 |
| 6 | リベート収入 | 千円 |
| 7 | その他営業外収入 | 千円 |
| 8 | 営業外費用 | 千円 |
| 9 | 本店管理費 | 千円 |
| 10 | 雑損失 | 千円 |
| 11 | 経常利益 | 千円 |

### 区分（secondary_category）の種類と表示順

| 順序 | 区分名 | 説明 |
|-----|--------|------|
| 1 | 前年実績 | 前年同月の実績値 |
| 2 | 本年目標 | 本年の目標値 |
| 3 | 本年実績 | 本年の実績値 |
| 4 | 前年比 | 本年実績 ÷ 前年実績 × 100 (%) |
| 5 | 目標比 | 本年実績 ÷ 本年目標 × 100 (%) |

### 値の表示形式

- **value**: 内部計算用の生の値（円単位、比率は小数）
- **display_value**: Looker Studioでの表示用の値
  - 金額系: 千円単位（value ÷ 1000）
  - 利益率・前年比・目標比: パーセント表示（value × 100）

## 組織階層

```
東京支店計
├── 工事営業部計
│   ├── ガラス工事計
│   │   ├── 佐々木（大成・鹿島他）
│   │   ├── 浅井（清水他）
│   │   ├── 小笠原（三井住友他）
│   │   └── 高石（内装・リニューアル）
│   └── 山本（改装）
└── 硝子建材営業部計
    ├── 硝子工事
    ├── ビルサッシ
    ├── 硝子販売
    ├── サッシ販売
    ├── サッシ完成品
    └── その他
```

### secondary_department_sort_order（表示順）

Looker Studioでの表示順序を制御するための並び順：

| 順序 | 部門名（secondary_department） |
|-----|------------------------------|
| 1 | 東京支店計 |
| 2 | 工事営業部計 |
| 3 | 佐々木（大成・鹿島他）|
| 4 | 浅井（清水他）|
| 5 | 小笠原（三井住友他）|
| 6 | 高石（内装・リニューアル）|
| 7 | ガラス工事計 |
| 8 | 山本（改装）|
| 9 | 硝子建材営業部計 |
| 10 | 硝子工事 |
| 11 | ビルサッシ |
| 12 | 硝子販売 |
| 13 | サッシ販売 |
| 14 | サッシ完成品 |
| 15 | その他 |
| 99 | 未分類・その他 |

**Looker Studioでの使用方法:**
- 表やグラフの「並べ替え」で `main_category_sort_order`、`secondary_category_sort_order`、`secondary_department_sort_order` を選択し、「昇順」を設定
- これにより、損益計算書の階層構造に沿った順番で表示される

### secondary_category_sort_order（区分の表示順）

| 順序 | 区分名 |
|-----|--------|
| 1 | 前年実績 |
| 2 | 本年目標 |
| 3 | 本年実績 |
| 4 | 前年比 |
| 5 | 目標比 |

## 月次運用フロー

### 手動実行の場合

```bash
# 1. 前月データがcorporate_dataに揃っているか確認
bq query --use_legacy_sql=false "
  SELECT MAX(year_month) as latest_data
  FROM \`data-platform-prod-475201.corporate_data.sales_target_and_achievements\`
"

# 2. DWHとDataMartを一括更新
cd /Users/oharayousuke/tanacho-pipeline
bash sql/scripts/update_all.sh

# 3. Looker Studioで結果確認
# → management_documents_current_month_tbl の date カラムを確認
```

### 自動実行の場合（Scheduled Query）

1. **事前準備**: 生データ（corporate_data）の月次更新が完了していることを確認
2. **DWH更新**: 毎月1日 02:00に自動実行（Scheduled Query）
3. **DataMart更新**: 毎月1日 03:00に自動実行（DWHの1時間後）
4. **確認**: Looker Studioダッシュボードで最新データを確認

### トラブルシューティング

**Q: 特定の月のデータを再処理したい**
```sql
-- SQLファイルの先頭を以下のように変更:
DECLARE target_month DATE DEFAULT DATE('2025-08-01');  -- 再処理したい月を指定
```

**Q: DWH更新は成功したがDataMart更新が失敗した**
```bash
# DataMartのみ再実行
bash sql/scripts/update_datamart.sh
```

**Q: データが0になっている**
```sql
-- 各DWHテーブルを個別に確認
SELECT * FROM `data-platform-prod-475201.corporate_data_dwh.dwh_sales_actual`
WHERE year_month = '2025-09-01'
LIMIT 10;
```

## データソーステーブル

| テーブル名 | 用途 |
|-----------|------|
| `sales_target_and_achievements` | 売上・粗利実績 |
| `profit_plan_term` | 目標値、前年実績 |
| `department_summary` | 営業経費、本店管理費、社内利息 |
| `ledger_income` | 営業外収入（リベート等） |
| `ledger_loss` | 営業外費用（雑損失） |
| `billing_balance` | 売掛残高（社内利息計算用） |
| `internal_interest` | 社内金利率 |

## 計算ロジック

### 売上総利益率
```sql
売上総利益率 = 売上総利益 ÷ 売上高
```

### 営業利益
```sql
営業利益 = 売上総利益 - 営業経費
```

### 経常利益
```sql
経常利益 = 営業利益
         + 営業外収入（リベート）
         + 営業外収入（その他）
         - 営業外費用（社内利息）
         - 本店管理費
```

### 社内利息（山本改装）
```sql
社内利息 = 2か月前の当月売上残高 × 売掛金利率
```

## 注意事項

### データ単位
- **DB保存**: 円単位で保存
- **表示**: 千円単位で表示（カラム名末尾に `_k` 付き）
- **利益率**: パーセント表示（カラム名末尾に `_pct` 付き）

### 日付条件
- **対象月**: 前月のデータを集計
- **社内利息**: 2か月前のデータを使用
- **累積**: 期首（4月）からの累計
- **営業外収入**: `ledger_income.accounting_month` カラムでフィルタリング（パーティションキー）
- **営業外費用（雑損失）**: `ledger_loss.accounting_month` カラムでフィルタリング（パーティションキー）

### リベート判定
- 全角「リベート」と半角「リベート」の両方を判定
- `ledger_income.description_comment`カラムで判定
- `ledger_income` テーブルは `accounting_month` カラムでフィルタリング（`slip_date` ではない）

### 雑損失判定
- `ledger_loss` テーブルは `accounting_month` カラムでフィルタリング（`slip_date` ではない）
- `own_department_code` で部門別に集計

### 前年実績データ
現在は `profit_plan_term` テーブルから取得していますが、将来的には以下の対応が必要：
- TKSデータからの自動取得
- 過去データの計算・登録
- 実装方法は別途相談

## トラブルシューティング

### クエリが遅い場合
1. パーティション列でフィルタリングされているか確認
2. 不要な組織のデータを除外
3. 必要なカラムのみを SELECT

### データが表示されない場合
1. `target_month` が正しく設定されているか確認
2. 各テーブルにデータが存在するか確認
3. 営業所コード、部門コードが正しいか確認

### 金額が合わない場合
1. 単位変換（千円→円）が正しいか確認
2. 組織階層の集計ロジックを確認
3. 元データの品質を確認

## 今後の拡張

### 予定している機能
- [ ] 累積値の計算（期首からの合計）
- [ ] 予算比・前年比の計算
- [ ] 月次推移の取得
- [ ] 担当者名のマスタテーブル化
- [ ] 部門コードのマスタテーブル化

### 参照
- 要件定義: `データソース一覧・加工内容.xlsx` の `#1_経営資料` シート
- データパイプライン: `/README.md`
- テーブル定義: `/columns/` ディレクトリ
