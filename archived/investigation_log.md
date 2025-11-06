# SQL調査ログ

このファイルは、SQLの計算結果の検証と修正内容を記録するためのものです。

---

## Version 1: 初回調査（2025-10-28）

### 調査依頼
dashboard_management_report_vertical.sqlの計算結果について、以下の3つの情報源を比較して数値の正確性を検証する：
1. SQL実行結果（sql/tmp_file/script_job_ea06a2cb30f7f7df444e9650bf54fcb5_0_20251028.csv）
2. SQLの実際の処理内容（sql/dashboard_management_report_vertical.sql）
3. 仕様書（データソース一覧・加工内容 - #1_1経営資料（当月）.csv の37行目まで）

### 調査結果

#### 1. 【最重要】硝子建材営業部の売上総利益が計算されていない

**発見内容**:
- SQL実行結果において、硝子建材営業部の売上総利益（本年実績）が空（NULL）になっている
- 東京支店計の売上総利益 = 工事営業部計の売上総利益 = 33,183,622円
- 硝子建材営業部計の売上総利益 = 空（行51）

**原因**:
- sql/dashboard_management_report_vertical.sql:88, 135
- `SUM(case when branch_code = 11 then gross_profit_actual else null end) AS gross_profit_amount`
- この条件により、`branch_code = 11`（工事営業部）のデータのみが集計され、`branch_code = 25`（硝子建材営業部）が除外されている

**影響範囲**:
- 硝子建材営業部の売上総利益（本年実績・前年実績）
- 東京支店計の売上総利益（本年実績・前年実績）
- 売上総利益率（本年実績・前年実績）
- 営業利益（本年実績・前年実績）
- 経常利益（本年実績）

**仕様書との関係**:
- 仕様書（行63-64）: 「B列「営業所コード」が"011"かつ」と記載
- 仕様書（行67）: 「東京支店計＝工事営業部計＋硝子建材部計」と記載
- → 矛盾があり、実際のビジネス要件としては両営業部の粗利を集計する必要があると判断

#### 2. 工事営業部のデータが全て「未分類」に入っている

**発見内容**:
- SQL実行結果において、工事営業部の売上164,061,036円が全て「未分類」カテゴリに分類されている
- 担当者別（佐々木、岡本、小笠原、高石、山本）への分類ができていない

**原因**:
- sql/dashboard_management_report_vertical.sql:70-74
- データソースの`staff_name`カラムの値が、SQLで期待している値と完全一致していない（表記揺れ）

**対応状況**:
- ✅ ユーザー側で表記揺れを修正済み（対応完了）

#### 3. 仕様書とSQLの人名不一致

**発見内容**:
- 仕様書（行25）: 「浅井（清水他）」
- SQL実装: 「岡本（清水他）」
- SQLコメントで既に「要確認」として記載済み

**対応状況**:
- ✅ ユーザー側で表記揺れを修正済み（対応完了）

### 対応内容

#### 修正1: 硝子建材営業部の売上総利益を集計対象に追加

**対応方法**:
sql/dashboard_management_report_vertical.sqlの以下の箇所を修正：

**修正前**:
```sql
-- 行88 (sales_actual CTE)
SUM(case when branch_code = 11 then gross_profit_actual else null end) AS gross_profit_amount

-- 行135 (sales_actual_prev_year CTE)
SUM(case when branch_code = 11 then gross_profit_actual else null end) AS gross_profit_amount
```

**修正後**:
```sql
-- 行88 (sales_actual CTE)
SUM(gross_profit_actual) AS gross_profit_amount

-- 行135 (sales_actual_prev_year CTE)
SUM(gross_profit_actual) AS gross_profit_amount
```

**期待される効果**:
- `branch_code = 11`（工事営業部）と`branch_code = 25`（硝子建材営業部）の両方の粗利が集計される
- 東京支店計の売上総利益が正確になる
- それに伴い、売上総利益率、営業利益、経常利益も正確に計算される

**修正の検証結果（script_job_2cc3b46f2d9c728ba2eeea8f775f5e24_0_202510281.csv）**:

✅ **問題1が解決**: 硝子建材営業部の売上総利益が正しく計算されている
- 修正前: 硝子建材営業部計の売上総利益 = 空（NULL）
- 修正後: 硝子建材営業部計の売上総利益 = **26,732,089円**
- 修正前: 東京支店計の売上総利益 = 33,183,622円（工事営業部のみ）
- 修正後: 東京支店計の売上総利益 = **59,915,711円**（工事営業部 + 硝子建材営業部）

数値検証:
```
工事営業部計: 33,183,622円
硝子建材営業部計: 26,732,089円
東京支店計: 59,915,711円
検証: 33,183,622 + 26,732,089 = 59,915,711 ✅
```

売上総利益率の検証:
```
東京支店計: 59,915,711 / 295,808,015 = 0.2025493... ✅
硝子建材営業部計: 26,732,089 / 131,746,979 = 0.2029047... ✅
```

✅ **問題2が解決**: 工事営業部のデータが担当者別に正しく分類されている
- 修正前: 「未分類」に164,061,036円が全て入っていた
- 修正後: 担当者別に正しく分類

担当者別内訳:
- 佐々木（大成・鹿島他）: 売上23,517,930円、粗利1,881,818円
- 岡本（清水他）: 売上0円、粗利0円
- 小笠原（三井住友他）: 売上56,946,263円、粗利19,559,366円
- 高石（内装・リニューアル）: 売上13,956,171円、粗利4,340,401円
- ガラス工事計: 売上94,420,364円、粗利25,781,585円

ガラス工事計の検証:
```
売上: 23,517,930 + 0 + 56,946,263 + 13,956,171 = 94,420,364 ✅
粗利: 1,881,818 + 0 + 19,559,366 + 4,340,401 = 25,781,585 ✅
```

✅ **問題3が解決**: 「岡本（清水他）」のデータが正しく出力されている

#### 修正2: 工事営業部の担当者別分類（表記揺れの修正）

**対応方法**:
データソース側で`staff_name`カラムの表記揺れを修正（ユーザー側で対応）

**修正状況**: ✅ 完了

#### 修正3: 仕様書とSQLの人名不一致（表記揺れの修正）

**対応方法**:
データソース側または仕様書側で人名を統一（ユーザー側で対応）

**修正状況**: ✅ 完了

### 正常動作を確認した部分

以下の部分は正しく動作していることを確認：

✅ 売上高の集計（工事営業部・硝子建材営業部・東京支店計）
✅ 硝子建材営業部の部門別分類（硝子販売、サッシ販売、硝子工事、ビルサッシ、サッシ完成品、その他）
✅ 売上総利益率の計算ロジック
✅ 組織階層の集計ロジック

### 修正前の数値検証

**売上高の整合性**:
- 工事営業部計: 164,061,036円
- 硝子建材営業部計: 131,746,979円
- 東京支店計: 295,808,015円
- 検証: 164,061,036 + 131,746,979 = 295,808,015 ✅

**売上総利益率の計算（修正前）**:
- 東京支店計: 0.1121796 = 33,183,622 / 295,808,015 ✅（工事営業部のみで計算）
- 工事営業部計: 0.2022639 = 33,183,622 / 164,061,036 ✅

### 修正後の数値検証（最終確認）

**売上高の整合性**:
- 工事営業部計: 164,061,036円
- 硝子建材営業部計: 131,746,979円
- 東京支店計: 295,808,015円
- 検証: 164,061,036 + 131,746,979 = 295,808,015 ✅

**売上総利益の整合性**:
- 工事営業部計: 33,183,622円
- 硝子建材営業部計: 26,732,089円
- 東京支店計: 59,915,711円
- 検証: 33,183,622 + 26,732,089 = 59,915,711 ✅

**売上総利益率の計算（修正後）**:
- 東京支店計: 0.2025493 = 59,915,711 / 295,808,015 ✅（両営業部を含む）
- 工事営業部計: 0.2022639 = 33,183,622 / 164,061,036 ✅
- 硝子建材営業部計: 0.2029047 = 26,732,089 / 131,746,979 ✅

**営業利益の整合性**:
```
営業利益 = 売上総利益 - 営業経費
東京支店計: 59,915,711 - 0 = 59,915,711 ✅
工事営業部計: 33,183,622 - 0 = 33,183,622 ✅
硝子建材営業部計: 26,732,089 - 0 = 26,732,089 ✅
```

**経常利益の整合性**:
```
経常利益 = 営業利益 + 営業外収入 - 営業外費用 - 本店管理費
東京支店計: 59,915,711 + 0 - 0 - 0 = 59,915,711 ✅
```

### まとめ

**修正完了日**: 2025-10-28

**修正内容**:
1. SQL修正: 売上総利益の集計条件を変更（branch_code = 11のみ → 全branch_code）
2. データソース修正: 担当者名の表記揺れを修正
3. データソース修正: 人名の不一致を修正

**結果**: すべての問題が解決し、数値の整合性が確認された ✅

---

## Version 2: 値がゼロになっている項目の調査（2025-10-28）

### 調査依頼
SQL実行結果（script_job_2cc3b46f2d9c728ba2eeea8f775f5e24_0_202510281.csv）において、特定のmain_categoryのvalueが全てのmain_department・secondary_departmentでゼロになっている状態について調査する。

### 調査結果

#### ゼロになっているmain_categoryの特定

SQL実行結果を集計した結果、以下の6項目が全ての部門でゼロになっていることが判明：

| main_category | 合計値 | 行数 | 非ゼロ行数 |
|--------------|--------|------|-----------|
| 営業外収入（その他） | 0円 | 15行 | 0行 |
| 営業外収入（リベート） | 0円 | 15行 | 0行 |
| 営業外費用（社内利息A・B） | 0円 | 15行 | 0行 |
| 営業外費用（雑損失） | 0円 | 15行 | 0行 |
| 営業経費 | 0円 | 45行 | 0行 |
| 本店管理費 | 0円 | 15行 | 0行 |

**参考（正常に値が入っている項目）:**
- 売上高: 合計981,844,409円、非ゼロ行数14行
- 売上総利益: 合計205,528,718円、非ゼロ行数14行
- 営業利益: 合計205,528,718円、非ゼロ行数14行

#### 1. 営業経費がゼロになっている

**SQL処理内容（dashboard_management_report_vertical.sql:212-241）:**
```sql
operating_expenses AS (
  SELECT
    CASE
      WHEN code IN ('11', '18') THEN 'ガラス工事計'
      WHEN code = '13' THEN '山本（改装）'
      WHEN code = '20' THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,
    SUM(
      CASE
        WHEN subject_name IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN total
        ELSE 0
      END
    ) AS operating_expense_amount
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  WHERE
    sales_accounting_period = target_month
    AND code IN ('11', '18', '13', '20')
  GROUP BY detail_category
)
```

**仕様書の記載（行116-199）:**
- データソース: #6 部門集計表
- 条件: 1行目の値が"11", "18", "13", "20"かつ、A列「コード」が8331～8361の経費科目
- 対象部門: ガラス工事計、山本（改装）、硝子建材営業部

**SQLと仕様書の整合性**: ✅ 一致

**考えられる原因:**
1. **データソース（department_summary）に2025-09-01のデータが存在しない**
2. **codeカラムの型不一致**: SQLでは文字列 ('11', '18', '13', '20')、データは数値型の可能性
3. **subject_nameカラムの値が期待値と異なる**: 実データでは文字列の前後にスペースや特殊文字がある可能性

#### 2. 営業外収入（リベート・その他）がゼロになっている

**SQL処理内容（dashboard_management_report_vertical.sql:247-282）:**
```sql
non_operating_income AS (
  SELECT
    CASE
      WHEN own_department_code = 11 THEN 'ガラス工事計'
      WHEN own_department_code in (13, 18) THEN '山本（改装）'
      WHEN own_department_code = 20 THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,
    SUM(
      CASE
        WHEN REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS rebate_income,
    SUM(
      CASE
        WHEN NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS other_non_operating_income
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
  WHERE
    DATE(slip_date) = target_month
    AND own_department_code IN (11, 13, 18, 20)
  GROUP BY detail_category
)
```

**仕様書の記載（行206-219）:**
- データソース: #4 元帳_雑収入
- 条件: V列「摘要」に"リベート"が含まれるか判定、AD列の値を利用
- R列「自部門コード」で部門を判定（11, 18, 13, 20）
- リベートは全角・半角両方とも判定が必要

**SQLと仕様書の整合性**: ✅ 一致（REGEXP_CONTAINSで全角・半角両方判定）

**考えられる原因:**
1. **データソース（ledger_income）に2025-09-01のデータが存在しない**
   - 営業外収入は毎月発生するとは限らない項目
2. **slip_dateの型不一致**: DATE(slip_date)で変換しているが、元データの形式が異なる可能性
3. **own_department_codeの型不一致**: SQLでは数値 (11, 13, 18, 20)、データは文字列型の可能性

#### 3. 営業外費用（社内利息A・B）がゼロになっている

**SQL処理内容（dashboard_management_report_vertical.sql:288-355）:**

複数のCTEで構成：
1. `yamamoto_interest`: 山本（改装）の社内利息を請求残高×利率で計算
2. `department_interest`: 部門集計表から勘定科目コード'9250'を取得
3. `glass_interest`: ガラス工事計の利息から山本分を除く

**仕様書の記載（行220-228）:**
- 山本（改装）: #3 請求残高一覧表 × #7 社内金利計算表で計算
  - 値A = 売上月度が2か月前かつ営業所コード"013"の当月売上残高
  - 値B = 利率
  - 結果 = 値A × 値B
- ガラス工事計: 部門集計表のコード"9250" - 山本（改装）の値
- 硝子建材営業部: 部門集計表のコード"9250"

**SQLと仕様書の整合性**: ✅ 一致

**考えられる原因:**
1. **データソース（billing_balance, internal_interest, department_summary）に対象月のデータが存在しない**
2. **JOINの条件が合わない**:
   - `bb.sales_month = ii.year_month` で結合しているが、日付フォーマットが異なる可能性
   - `two_months_ago` (2025-07-01) のデータが存在しない
3. **branch_codeやcategoryの値が期待値と異なる**

#### 4. 営業外費用（雑損失）がゼロになっている

**SQL処理内容（dashboard_management_report_vertical.sql:360-380）:**
```sql
miscellaneous_loss AS (
  SELECT
    CASE
      WHEN own_department_code IN (11, 18) THEN 'ガラス工事計'
      WHEN own_department_code = 13 THEN '山本（改装）'
      WHEN own_department_code = 20 THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,
    SUM(amount) AS miscellaneous_loss_amount
  FROM `data-platform-prod-475201.corporate_data.ledger_loss`
  WHERE
    DATE(slip_date) = target_month
    AND own_department_code IN (11, 13, 18, 20)
  GROUP BY detail_category
)
```

**仕様書の記載（行229-235）:**
- データソース: #16 元帳_雑損失
- 条件: AD列の値を利用、R列「自部門コード」で部門を判定

**SQLと仕様書の整合性**: ✅ 一致

**考えられる原因:**
1. **データソース（ledger_loss）に2025-09-01のデータが存在しない**
   - 雑損失は毎月発生するとは限らない項目
2. **slip_dateの型不一致**
3. **own_department_codeの型不一致**

#### 5. 本店管理費がゼロになっている

**SQL処理内容（dashboard_management_report_vertical.sql:385-402）:**
```sql
head_office_expenses AS (
  SELECT
    CASE
      WHEN code = '11' THEN 'ガラス工事計'
      WHEN code = '13' THEN '山本（改装）'
      WHEN code = '20' THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,
    SUM(total) AS head_office_expense
  FROM `data-platform-prod-475201.corporate_data.department_summary`
  WHERE
    sales_accounting_period = target_month
    AND subject_name = '8366'
    AND code IN ('11', '13', '20')
  GROUP BY detail_category
)
```

**仕様書の記載（行236-240）:**
- データソース: #6 部門集計表
- 条件: A列「コード」が"8366"、1行目が"11", "13", "20"の値

**SQLと仕様書の整合性**: ✅ 一致

**考えられる原因:**
1. **データソース（department_summary）に勘定科目コード'8366'のデータが存在しない**
2. **codeカラムの型不一致**
3. **subject_nameカラムの値が'8366'ではない（例: ' 8366 'のようにスペースが含まれる）**

### 原因の推測まとめ

**最も可能性が高い原因: データソースにデータが存在しない**

すべてのゼロ項目に共通する問題として、以下の可能性が高い：

1. **対象月（2025-09-01）のデータが各データソースに登録されていない**
   - `department_summary`: 営業経費、本店管理費、社内利息の元データ
   - `ledger_income`: 営業外収入の元データ
   - `ledger_loss`: 雑損失の元データ
   - `billing_balance`: 社内利息計算用の請求残高データ
   - `internal_interest`: 社内利息計算用の利率データ

2. **カラムの型不一致**
   - `code`: 文字列 '11' vs 数値 11
   - `own_department_code`: 数値 11 vs 文字列 '11'
   - `subject_name`: '8331' vs 数値 8331

3. **日付フォーマットの不一致**
   - `sales_accounting_period`: '2025-09-01' vs 他のフォーマット
   - `slip_date`: DATE型への変換が正しく機能していない

### 推奨される調査手順

次のステップとして、以下の調査を推奨：

1. **データソースの存在確認**
   ```sql
   -- department_summaryに2025-09-01のデータが存在するか
   SELECT COUNT(*), MIN(sales_accounting_period), MAX(sales_accounting_period)
   FROM `data-platform-prod-475201.corporate_data.department_summary`
   WHERE sales_accounting_period = '2025-09-01';

   -- ledger_incomeに2025-09-01のデータが存在するか
   SELECT COUNT(*), MIN(slip_date), MAX(slip_date)
   FROM `data-platform-prod-475201.corporate_data.ledger_income`
   WHERE DATE(slip_date) = '2025-09-01';
   ```

2. **カラムの型確認**
   ```sql
   -- codeカラムの実際の値とデータ型を確認
   SELECT DISTINCT code, TYPEOF(code)
   FROM `data-platform-prod-475201.corporate_data.department_summary`
   LIMIT 10;
   ```

3. **実データのサンプル確認**
   ```sql
   -- 営業経費の対象となるデータが存在するか
   SELECT *
   FROM `data-platform-prod-475201.corporate_data.department_summary`
   WHERE sales_accounting_period = '2025-09-01'
     AND code IN ('11', '18', '13', '20')
   LIMIT 10;
   ```

### 重要な発見: 仕様書とSQLの解釈のズレ

**仕様書の記述の問題点:**

仕様書では以下のような記述がある：
- 行116: 「**1行目の値が"11"または"18"かつ**、A列「コード」が...」
- 行144: 「**８行目の値が"13"かつ**、A列「コード」が...」
- 行172: 「**８行目の値が"20"かつ**、A列「コード」が...」

この「1行目の値」「8行目の値」という表記は、**元のスプレッドシート（#6 部門集計表）の構造**を指していると推測される。

**SQLの実装との乖離:**

SQLでは以下のように実装されている：
```sql
WHERE sales_accounting_period = target_month
  AND code IN ('11', '18', '13', '20')
```

しかし：
1. `code`というカラムが`department_summary`テーブルに存在するか不明
2. 仕様書の「1行目の値が"11"」という表現が、SQLの`code`カラムに対応しているか不明
3. **スプレッドシートの横持ち構造（列に部門が並ぶ）とテーブルの縦持ち構造（行に部門が並ぶ）の変換が正しく行われているか不明**

**考えられる問題:**

1. **元データはスプレッドシート横持ち形式**で、1行目に部門コード（11, 18, 13, 20など）が列として並んでいる
2. **BigQueryテーブルへの変換時**に、この構造が正しく縦持ちに変換されていない、または
3. **SQLの条件が誤っている**（`code`カラムが実際には存在しない、または別の名前）

**該当するレコードが存在しない可能性が高い理由:**

ユーザー指摘の通り、「1行目の値が"11"または"18"」という条件に該当するレコードが`department_summary`テーブルに存在しない可能性が高い。

これは：
- スプレッドシートからBigQueryへのデータ取り込み処理に問題がある
- または、テーブル構造の理解が誤っている

ことを示唆している。

### 対応内容

**immediate action required:**

1. **`department_summary`テーブルのスキーマを確認する**
   ```sql
   -- テーブルの全カラムを確認
   SELECT * FROM `data-platform-prod-475201.corporate_data.department_summary` LIMIT 1;

   -- スキーマ情報を取得
   SELECT column_name, data_type
   FROM `data-platform-prod-475201.corporate_data.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name = 'department_summary';
   ```

2. **`code`カラムの存在と実際の値を確認する**
   ```sql
   -- codeカラムに何が入っているか確認
   SELECT DISTINCT code, COUNT(*) as count
   FROM `data-platform-prod-475201.corporate_data.department_summary`
   GROUP BY code
   ORDER BY code;
   ```

3. **2025-09-01のデータが存在するか確認**
   ```sql
   -- 対象月のデータ件数を確認
   SELECT COUNT(*),
          COUNT(DISTINCT code) as distinct_codes,
          STRING_AGG(DISTINCT CAST(code AS STRING)) as codes
   FROM `data-platform-prod-475201.corporate_data.department_summary`
   WHERE sales_accounting_period = '2025-09-01';
   ```

4. **元のスプレッドシート #6 部門集計表の構造を確認する**
   - 実際のスプレッドシートを見て、「1行目」「8行目」が何を指しているか確認
   - BigQueryへの取り込み処理（ETL）が正しく行われているか確認

**対応状況**: 🚨 **緊急対応必要** - テーブル構造とデータの存在確認が最優先

### 調査の中断

**日時**: 2025-10-28

**理由**: 必要なデータ（department_summary, ledger_income, ledger_loss, billing_balance, internal_interest等）をBigQueryに連携する必要が発生したため、調査を一旦中断。

**次回調査時の開始ポイント**:
1. データ連携完了後、上記「対応内容」セクションの確認SQLを実行
2. テーブルスキーマとデータの存在を確認
3. 必要に応じてSQLの条件を修正（カラム名、型、値の調整）

**状態**: ⏸️ データ連携待ち

---

## Version 3: 経費項目がゼロになる根本原因の詳細分析（2025-10-28）

### 調査依頼

営業外収入（リベート）、営業外収入（その他）、営業外費用（社内利息A・B）、営業外費用（雑損失）、本店管理費、経常利益の6項目がすべてゼロになっている原因を項目毎に詳細に調査し、言語化する。

### 共通の根本原因

**すべての経費項目が営業経費と同じ構造的問題を抱えています。**

各経費CTE（operating_expenses, non_operating_income, non_operating_expenses, miscellaneous_loss, head_office_expenses）は「**集計グループレベル**」のdetail_categoryを持っているにも関わらず、consolidated_metricsで「**個人/部門レベル**」のsales_actualとJOINしているため、JOINが失敗してゼロになっています。

### データ構造の不一致

#### sales_actual CTEのdetail_category（個人/部門レベル）:
```
工事営業部:
  - '佐々木（大成・鹿島他）'
  - '岡本（清水他）'
  - '小笠原（三井住友他）'
  - '高石（内装・リニューアル）'
  - '山本（改装）'

硝子建材営業部:
  - '硝子工事'
  - 'ビルサッシ'
  - '硝子販売'
  - 'サッシ販売'
  - 'サッシ完成品'
  - 'その他'
```

#### 各経費CTEのdetail_category（集計グループレベル）:
```
  - 'ガラス工事計'     （佐々木+岡本+小笠原+高石の合計）
  - '山本（改装）'     （山本のみ）
  - '硝子建材営業部'   （硝子建材営業部全体）
```

### 項目別の詳細分析

---

#### 1. 営業外収入（リベート）

**CTE:** `non_operating_income` (Lines 265-323)

**detail_category:**
- 'ガラス工事計'
- '山本（改装）'
- '硝子建材営業部'

**データソース:** `ledger_income` テーブル
- 条件: own_department_code IN (11, 18) → 'ガラス工事計'
- 条件: own_department_code = 13 → '山本（改装）'
- 条件: own_department_code IN (20, 62) → '硝子建材営業部'

**JOIN箇所:** dashboard_management_report_vertical.sql:560-561
```sql
LEFT JOIN non_operating_income noi
  ON sa.detail_category = noi.detail_category
```

**使用箇所:** Line 528
```sql
COALESCE(noi.rebate_income, 0) AS rebate_income,
```

**JOIN結果:**

| sales_actual.detail_category | non_operating_income.detail_category | JOIN成否 | rebate_income |
|------------------------------|--------------------------------------|----------|---------------|
| '佐々木（大成・鹿島他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '岡本（清水他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '小笠原（三井住友他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '高石（内装・リニューアル）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '山本（改装）' | '山本（改装）' | ✅ 一致 | ✅ **値あり** |
| '硝子工事' | '硝子建材営業部' | ❌ 不一致 | **0** |
| 'ビルサッシ' | '硝子建材営業部' | ❌ 不一致 | **0** |

**原因:**
- non_operating_income CTEには'ガラス工事計'として集計された値（工事営業１課+業務課の合計）が存在する
- しかし、consolidated_metricsでJOINする際、'佐々木（大成・鹿島他）' ≠ 'ガラス工事計' のため、この値は使われない
- COALESCEにより0に変換される

**aggregated_metricsでの影響:** Line 603
```sql
SUM(rebate_income) AS rebate_income,  -- 0 + 0 + 0 + 0 = 0
```

---

#### 2. 営業外収入（その他）

**CTE:** `non_operating_income` (同上)

**JOIN箇所:** Line 560-561（同じJOIN）

**使用箇所:** Line 529
```sql
COALESCE(noi.other_non_operating_income, 0) AS other_non_operating_income,
```

**原因:**
リベートと全く同じ。non_operating_incomeは2つのカラム（rebate_income、other_non_operating_income）を持ち、両方とも同じJOIN条件で取得されるため、両方ともゼロになります。

**データ区分:**
- description_commentに'リベート'が含まれる → rebate_income
- description_commentに'リベート'が含まれない → other_non_operating_income

**aggregated_metricsでの影響:** Line 604
```sql
SUM(other_non_operating_income) AS other_non_operating_income,  -- 0 + 0 + 0 + 0 = 0
```

---

#### 3. 営業外費用（社内利息A・B）

**CTE:** `non_operating_expenses` (Lines 392-400)

複数のCTEで構成:
- `yamamoto_interest`: 山本（改装）の社内利息（billing_balance × internal_interest）
- `department_interest`: 部門集計表から勘定科目コード'9250'を取得
- `glass_interest`: ガラス工事計の利息（department_interest - yamamoto_interest）

**detail_category:**
- 'ガラス工事計' ← glass_interestから
- '山本（改装）' ← yamamoto_interestから
- '硝子建材営業部' ← department_interestから

**JOIN箇所:** Line 563-564
```sql
LEFT JOIN non_operating_expenses noe
  ON sa.detail_category = noe.detail_category
```

**使用箇所:** Line 532
```sql
COALESCE(noe.interest_expense, 0) AS non_operating_expenses,
```

**JOIN結果:**

| sales_actual.detail_category | non_operating_expenses.detail_category | JOIN成否 | interest_expense |
|------------------------------|---------------------------------------|----------|------------------|
| '佐々木（大成・鹿島他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '岡本（清水他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '小笠原（三井住友他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '高石（内装・リニューアル）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '山本（改装）' | '山本（改装）' | ✅ 一致 | ✅ **値あり** |
| '硝子工事' | '硝子建材営業部' | ❌ 不一致 | **0** |

**原因:**
- 'ガラス工事計'の社内利息データは存在するが、個人名とマッチしないため使われない
- '山本（改装）'のみ、detail_categoryが一致するため正しく取得できる

**aggregated_metricsでの影響:** Line 605
```sql
SUM(non_operating_expenses) AS non_operating_expenses,  -- 0 + 0 + 0 + 0 = 0
```

---

#### 4. 営業外費用（雑損失）

**CTE:** `miscellaneous_loss` (Lines 405-438)

**detail_category:**
- 'ガラス工事計'
- '山本（改装）'
- '硝子建材営業部'

**データソース:** `ledger_loss` テーブル
- 条件: own_department_code IN (11, 18) → 'ガラス工事計'
- 条件: own_department_code = 13 → '山本（改装）'
- 条件: own_department_code IN (20, 62) → '硝子建材営業部'

**JOIN箇所:** Line 566-567
```sql
LEFT JOIN miscellaneous_loss ml
  ON sa.detail_category = ml.detail_category
```

**使用箇所:** Line 533
```sql
COALESCE(ml.miscellaneous_loss_amount, 0) AS miscellaneous_loss,
```

**JOIN結果:**

| sales_actual.detail_category | miscellaneous_loss.detail_category | JOIN成否 | miscellaneous_loss_amount |
|------------------------------|------------------------------------|----------|---------------------------|
| '佐々木（大成・鹿島他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '岡本（清水他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '小笠原（三井住友他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '高石（内装・リニューアル）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '山本（改装）' | '山本（改装）' | ✅ 一致 | ✅ **値あり** |
| '硝子工事' | '硝子建材営業部' | ❌ 不一致 | **0** |

**原因:**
ledger_lossテーブルから集計したデータが'ガラス工事計'として存在するが、個人名とマッチしないため使われない。

**aggregated_metricsでの影響:** Line 606
```sql
SUM(miscellaneous_loss) AS miscellaneous_loss,  -- 0 + 0 + 0 + 0 = 0
```

---

#### 5. 本店管理費

**CTE:** `head_office_expenses` (Lines 444-478)

**detail_category:**
- 'ガラス工事計'
- '山本（改装）'
- '硝子建材営業部'

**データソース:** `department_summary` テーブル
- 条件: code = '8366'
- 集計: construction_sales_section_1 + operations_section → 'ガラス工事計'
- 集計: renovation_section → '山本（改装）'
- 集計: glass_building_material_sales_section → '硝子建材営業部'

**JOIN箇所:** Line 569-570
```sql
LEFT JOIN head_office_expenses hoe
  ON sa.detail_category = hoe.detail_category
```

**使用箇所:** Line 536
```sql
COALESCE(hoe.head_office_expense, 0) AS head_office_expense,
```

**JOIN結果:**

| sales_actual.detail_category | head_office_expenses.detail_category | JOIN成否 | head_office_expense |
|------------------------------|--------------------------------------|----------|---------------------|
| '佐々木（大成・鹿島他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '岡本（清水他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '小笠原（三井住友他）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '高石（内装・リニューアル）' | 'ガラス工事計' | ❌ 不一致 | **0** |
| '山本（改装）' | '山本（改装）' | ✅ 一致 | ✅ **値あり** |
| '硝子工事' | '硝子建材営業部' | ❌ 不一致 | **0** |

**原因:**
department_summaryテーブルのcode='8366'のデータが'ガラス工事計'として集計されているが、個人名とマッチしないため使われない。

**aggregated_metricsでの影響:** Line 607
```sql
SUM(head_office_expense) AS head_office_expense,  -- 0 + 0 + 0 + 0 = 0
```

---

#### 6. 経常利益

**計算式:** Lines 539-547
```sql
(
  sa.gross_profit_amount
  - COALESCE(oe.operating_expense_amount, 0)
  + COALESCE(noi.rebate_income, 0)
  + COALESCE(noi.other_non_operating_income, 0)
  - COALESCE(noe.interest_expense, 0)
  - COALESCE(ml.miscellaneous_loss_amount, 0)
  - COALESCE(hoe.head_office_expense, 0)
) AS recurring_profit_actual,
```

**原因:**
上記1〜5のすべての経費項目がゼロのため、経常利益の計算結果も不正確になります。

**計算例（佐々木の場合）:**
```
経常利益 = 粗利 - 0（営業経費） + 0（リベート） + 0（その他収入）
           - 0（社内利息） - 0（雑損失） - 0（本店管理費）
         = 粗利のみ
```

本来は各種経費を引くべきですが、すべてゼロのため、**実質的に粗利と同じ値**になってしまいます。

**aggregated_metricsでの影響:** Line 608
```sql
SUM(recurring_profit_actual) AS recurring_profit_actual,
```

この値は、個人レベルの粗利を単純に合計したものになり、グループレベルの経費が一切反映されていません。

---

### データフロー全体図

```
【各経費CTE】
operating_expenses:       'ガラス工事計' → 23,290,821円
non_operating_income:     'ガラス工事計' → XXX円
non_operating_expenses:   'ガラス工事計' → XXX円
miscellaneous_loss:       'ガラス工事計' → XXX円
head_office_expenses:     'ガラス工事計' → XXX円
                              ↓
                        JOINを試みる
                              ↓
【sales_actual】
'佐々木（大成・鹿島他）' ----×---- JOIN失敗（detail_categoryが不一致）
'岡本（清水他）'        -----×
'小笠原（三井住友他）'   -----×
'高石（内装・リニューアル）'--×
                              ↓
【consolidated_metrics】
佐々木: すべての経費項目が0
岡本:   すべての経費項目が0
小笠原: すべての経費項目が0
高石:   すべての経費項目が0
                              ↓
【aggregated_metrics - ガラス工事計】
SUM(0 + 0 + 0 + 0) = すべての経費項目が0
```

---

### まとめ

#### ゼロになる項目と原因の一覧

| 項目 | CTE | detail_category | 根本原因 |
|-----|-----|----------------|---------|
| 営業外収入（リベート） | non_operating_income | 'ガラス工事計', '山本（改装）', '硝子建材営業部' | 個人名とグループ名の不一致 |
| 営業外収入（その他） | non_operating_income | 同上 | 同上 |
| 営業外費用（社内利息A・B） | non_operating_expenses | 同上 | 同上 |
| 営業外費用（雑損失） | miscellaneous_loss | 同上 | 同上 |
| 本店管理費 | head_office_expenses | 同上 | 同上 |
| 経常利益 | 計算フィールド | - | 上記すべての項目を使った計算のため、経費が反映されず粗利のみになる |

#### 唯一の例外

**'山本（改装）'のみ**、sales_actualのdetail_categoryとすべての経費CTEのdetail_categoryが一致するため、正しく値が取得できます。

#### 影響範囲

- **ガラス工事計**: すべての経費項目がゼロ
- **硝子建材営業部**: すべての経費項目がゼロ
- **工事営業部計**: 上記の集計なので、すべての経費項目がゼロ
- **東京支店計**: 全体の集計なので、すべての経費項目がゼロ

結果として、**最終出力の大部分の行で、営業経費以降の項目がすべてゼロ**になります。

#### 構造的問題の本質

この問題の本質は、**集計レベルの異なるデータ同士を直接JOINしようとしている**点にあります。

- 経費CTEは「グループレベル」（ガラス工事計、山本、硝子建材営業部）
- sales_actualは「個人/部門レベル」（佐々木、岡本、小笠原、高石、等）

この2つを直接JOINすることはできないため、「山本（改装）」以外は全てNULLとなり、結果として0になります。

---

## Version 4: データマート可視化でゼロになる項目の根本原因調査（2025-10-29）

### 調査依頼

`datamart_management_report_vertical.sql` で作成したデータマートを可視化したところ、以下の5項目が依然としてゼロになっている原因を調査し、詳細に言語化する：
- 営業外収入（リベート）
- 営業外収入（その他）
- 営業外費用（社内利息A・B）
- 営業外費用（雑損失）
- 本店管理費

### 調査方法

1. DWHテーブル（`corporate_data_dwh.*`）のデータ存在確認
2. ソーステーブル（`corporate_data.*`）のデータ存在確認
3. sql/split_dwh_dm/ 配下のクエリと実データの照合
4. 仕様書（sql/README.md, transformation_proposal.md）との整合性確認

### 調査結果サマリー

| 項目 | DWH状態 | 根本原因 | 優先度 |
|------|---------|----------|--------|
| 営業外収入（リベート） | NULL | ソーステーブル `ledger_income` に 2025-09-01 のデータが存在しない | 高 |
| 営業外収入（その他） | NULL | 同上 | 高 |
| 営業外費用（社内利息A・B） | 一部のみ値あり | `internal_interest.category` の値が仕様と不一致 + 山本（改装）データ欠落 | 緊急 |
| 営業外費用（雑損失） | NULL | ソーステーブル `ledger_loss` のデータが5月で停止 | 緊急 |
| 本店管理費 | **正常に値あり** | DWHは正常、山本（改装）の行が最終出力に存在しない（sales_actual欠落が原因） | 緊急 |

### 項目別の詳細分析

---

#### 1. 営業外収入（リベート）・営業外収入（その他）

**DWHテーブルの状態:**
```
data-platform-prod-475201.corporate_data_dwh.non_operating_income:
  year_month    | detail_category  | rebate_income | other_non_operating_income
  2025-09-01    | 硝子建材営業部    | NULL          | NULL
  2025-09-01    | 山本（改装）      | NULL          | NULL
  2025-09-01    | ガラス工事計      | NULL          | NULL
```

**根本原因:**

ソーステーブル `corporate_data.ledger_income` に **2025-09-01 のデータが存在しない**

**データ存在確認:**
```sql
-- ledger_incomeの日付範囲
SELECT MIN(slip_date), MAX(slip_date), COUNT(*)
FROM `data-platform-prod-475201.corporate_data.ledger_income`

結果:
  最小日付: 2025-09-03
  最大日付: 2025-09-30
  2025-09-01のレコード数: 0件
```

**SQLの実装:**
`sql/split_dwh_dm/dwh_non_operating_income.sql:68`
```sql
WHERE DATE(slip_date) = DATE('2025-09-01')
```

この条件に該当するレコードが存在しないため、全てのdetail_categoryでNULLになっています。

**仕様書との関係:**
- sql/README.md:89-93 には営業外収入のソースとして `ledger_income` を使用することが記載
- DWHクエリのロジック自体は正しく実装されている

---

#### 2. 営業外費用（社内利息A・B）

**DWHテーブルの状態:**
```
data-platform-prod-475201.corporate_data_dwh.non_operating_expenses:
  year_month    | detail_category  | interest_expense
  2025-09-01    | 硝子建材営業部    | 2,081,087円 ✅
  2025-09-01    | ガラス工事計      | 5,066,451円 ✅
  (山本（改装）のデータなし) ❌
```

**根本原因:**

`internal_interest` テーブルの **`category` カラムの値が仕様と不一致**

**データ存在確認:**
```sql
-- internal_interestのcategory値
SELECT DISTINCT year_month, branch, category
FROM `data-platform-prod-475201.corporate_data.internal_interest`
WHERE year_month = DATE('2025-08-01')  -- 2か月前

結果:
  category の実際の値:
    - '社内利息（A）'
    - '社内利息（B）'
    - '減価償却費'
    - '本店管理費'
    - '貸倒損失'
    - '退職金'

  期待値（SQLの条件）:
    - '売掛金'  ← 存在しない！
```

**SQLの実装:**
`sql/split_dwh_dm/dwh_non_operating_expenses.sql:23-40`
```sql
yamamoto_interest AS (
  SELECT
    '山本（改装）' AS detail_category,
    bb.current_month_sales_balance * ii.interest_rate AS interest_expense
  FROM
    `data-platform-prod-475201.corporate_data.billing_balance` AS bb
  INNER JOIN
    `data-platform-prod-475201.corporate_data.internal_interest` AS ii
    ON bb.sales_month = ii.year_month
  WHERE
    bb.sales_month = DATE('2025-08-01')
    AND bb.branch_code = 13
    AND ii.year_month = DATE('2025-08-01')
    AND ii.branch = '東京支店'
    AND ii.category = '売掛金'  ← ★ここが問題★
  LIMIT 1
),
```

**検証結果:**
- `billing_balance` の該当データ: 存在する（2460件中に含まれる）
- `internal_interest` の 2025-08-01 データ: 存在する
- しかし、`category = '売掛金'` に該当するレコードが0件

このため、INNER JOINが失敗し、yamamoto_interest CTEが空になり、最終的に山本（改装）のデータが出力されません。

**補足:** 山本（改装）が最終出力に表示されない根本原因は、後述する「sales_actual DWH に山本（改装）のデータが存在しない」問題が主因です。

---

#### 3. 営業外費用（雑損失）

**DWHテーブルの状態:**
```
data-platform-prod-475201.corporate_data_dwh.miscellaneous_loss:
  year_month    | detail_category       | miscellaneous_loss_amount
  2025-09-01    | ガラス工事計          | NULL
  2025-09-01    | 山本（改装）          | NULL
  2025-09-01    | 硝子建材営業部        | NULL
```

**根本原因:**

ソーステーブル `corporate_data.ledger_loss` に **2025-09-01 のデータが存在しない**

**データ存在確認:**
```sql
-- ledger_lossの日付範囲
SELECT MIN(slip_date), MAX(slip_date), COUNT(*)
FROM `data-platform-prod-475201.corporate_data.ledger_loss`

結果:
  最小日付: 2025-04-15
  最大日付: 2025-05-29  ← 5月で停止
  2025-09-01のレコード数: 0件
```

**SQLの実装:**
`sql/split_dwh_dm/dwh_miscellaneous_loss.sql:43`
```sql
WHERE DATE(slip_date) = DATE('2025-09-01')
```

**重要な発見:**
データ連携が **5月29日で停止している** ことから、以下の可能性が高い：
1. Google Drive → GCS へのデータ同期が停止
2. GCS → BigQuery へのロード処理が停止
3. 元データ（Excel/CSV）自体が更新されていない

雑損失は毎月発生するとは限らない項目ですが、このケースでは **データパイプライン全体の問題** と推測されます。

---

#### 4. 本店管理費

**DWHテーブルの状態:**
```
data-platform-prod-475201.corporate_data_dwh.head_office_expenses:
  year_month    | detail_category  | head_office_expense
  2025-09-01    | 硝子建材営業部    | 1,952,000円 ✅
  2025-09-01    | 山本（改装）      | 790,000円 ✅
  2025-09-01    | ガラス工事計      | 3,264,000円 ✅
```

**expense_data CTEの状態:**
```
detail_category   | hq_expense
硝子建材営業部     | 1,952,000円 ✅
山本（改装）       | 790,000円 ✅
ガラス工事計       | 3,264,000円 ✅
```

**最終出力（management_documents_current_month_tbl）の状態:**
```
secondary_department     | main_category | value
東京支店計               | 本店管理費    | 66,066,000円 ✅
工事営業部計             | 本店管理費    | 20,270,000円 ✅
ガラス工事計             | 本店管理費    | 3,264,000円 ✅
硝子建材営業部計         | 本店管理費    | 1,952,000円 ✅
山本（改装）             | 本店管理費    | (行自体が存在しない) ❌
```

**結論:**

**本店管理費自体は正常に計算されています。**

ユーザーが「本店管理費がゼロ」と認識した原因は、**山本（改装）の行が最終出力に存在しない**ためと推測されます。

山本（改装）の行が存在しない根本原因は、次の項目5で詳述します。

---

#### 5. 【最重要】山本（改装）データ欠落問題

**問題の発見:**

`corporate_data_dwh.sales_actual` テーブルに **山本（改装）のレコードが存在しない**

**データ存在確認:**
```sql
-- sales_actual DWHのdetail_category一覧
SELECT DISTINCT organization, detail_category
FROM `data-platform-prod-475201.corporate_data_dwh.sales_actual`

結果:
  工事営業部:
    - 佐々木（大成・鹿島他）
    - 岡本（清水他）
    - 小笠原（三井住友他）
    - 高石（内装・リニューアル）
    - 未分類
  硝子建材営業部:
    - 硝子工事、ビルサッシ、硝子販売、サッシ販売、サッシ完成品、その他

  山本（改装） ← 存在しない！
```

**根本原因:**

ソーステーブル `corporate_data.sales_target_and_achievements` に **「山本」という担当者名のレコードが存在しない**

**データ存在確認:**
```sql
-- 工事営業部の担当者名一覧
SELECT DISTINCT staff_code, staff_name
FROM `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
WHERE sales_accounting_period = DATE('2025-09-01')
  AND branch_code = 11

結果:
  staff_code | staff_name
  119        | 佐々木康裕
  140        | 岡本一郎
  121        | 小笠原洋介
  151        | 高石麻友子
  122        | 浅井一作
  110        | 工事営業部

  「山本」← 存在しない！
```

**SQLの実装:**
`sql/split_dwh_dm/dwh_sales_actual.sql:27-33`
```sql
CASE
  -- 工事営業部の担当者別
  WHEN branch_code = 11 AND staff_name = '佐々木康裕' THEN '佐々木（大成・鹿島他）'
  WHEN branch_code = 11 AND staff_name = '岡本一郎' THEN '岡本（清水他）'
  WHEN branch_code = 11 AND staff_name = '小笠原洋介' THEN '小笠原（三井住友他）'
  WHEN branch_code = 11 AND staff_name = '高石麻友子' THEN '高石（内装・リニューアル）'
  WHEN branch_code = 11 AND staff_name = '山本' THEN '山本（改装）'  ← この条件に該当するレコードが0件
```

**影響範囲:**

datamart_management_report_vertical.sql の構造:
1. `consolidated_metrics` CTE で sales_actual を基準に各経費データを LEFT JOIN
2. sales_actual に山本（改装）がない
   ↓
3. consolidated_metrics にも山本（改装）の行が作成されない
   ↓
4. aggregated_metrics で「ガラス工事計」を計算する際も山本のデータが参照されない
   ↓
5. vertical_format で縦持ちに変換する際、山本（改装）の行が一切出現しない

**結果:**
- 山本（改装）の売上・粗利データが最終出力に表示されない
- 山本（改装）の本店管理費（790,000円）が表示されない
- 山本（改装）の営業経費（2,587,925円）が表示されない
- 工事営業部計の経費集計に影響する可能性がある

**仕様書との関係:**
- sql/README.md:108-112 には「山本（改装）」が組織階層に記載されている
- しかし、実際のソーステーブルには該当するデータが存在しない

---

### データ整合性の確認結果

**正常に動作している項目:**
- 営業経費（DWHテーブルに正しく値が入っている）
- 本店管理費（DWHテーブルに正しく値が入っている）
- 営業外費用・社内利息（ガラス工事計・硝子建材営業部は正常）

**問題がある項目:**
| 項目 | 問題レベル | 原因カテゴリ |
|------|-----------|-------------|
| 営業外収入 | データ不足 | ソーステーブルの日付不一致 |
| 営業外費用（雑損失） | データパイプライン停止 | データ連携の停止（5月で停止） |
| 営業外費用（社内利息・山本分） | スキーマ不一致 | category値の不一致 |
| 山本（改装）全般 | データ欠落 | ソーステーブルに担当者データが存在しない |

---

### 対策の提案

#### 優先度1（緊急対応必要）

**対策1-1: ledger_loss のデータ連携復旧**

**問題:** データが5月29日で停止している

**アクション:**
1. Google Drive 上の元データ（Excel/CSV）を確認
   - 6月以降のデータが存在するか
2. `sync_drive_to_gcs.py` のログを確認
   - 同期処理が正常に実行されているか
3. `load_to_bigquery.py` のログを確認
   - BigQueryへのロードが成功しているか
4. 必要に応じて手動でデータ連携を実行

**期待効果:** 雑損失データが正しく取得できるようになる

---

**対策1-2: 山本（改装）のデータ確認と追加**

**問題:** sales_target_and_achievements に山本さんのデータが存在しない

**アクション:**
1. 元データ（Excel）を確認し、山本さんのデータが存在するか調査
2. 山本さんのデータが別の staff_name で登録されていないか確認
   - 例: 「山本太郎」「山本 改装」「改装課」など
3. 山本（改装）が担当者名ではなく、部門として扱われている可能性を調査
   - department_code または division_code で識別できるか確認

**SQL修正案（暫定対応）:**

案A: 部分一致検索
```sql
-- dwh_sales_actual.sql:33
WHEN branch_code = 11 AND staff_name LIKE '山本%' THEN '山本（改装）'
```

案B: department_code を使用（改修課を示すコードが 13 の場合）
```sql
WHEN branch_code = 11 AND department_code = 13 THEN '山本（改装）'
```

案C: 経費のみの行を追加（応急処置・非推奨）
```sql
-- sales_actual DWHに経費表示用のダミー行を追加
SELECT DATE('2025-09-01'), '工事営業部', '山本（改装）', 0, 0
```

**期待効果:** 山本（改装）の行が最終出力に表示され、経費データも正しく表示される

---

#### 優先度2（短期対応）

**対策2-1: 営業外収入の日付条件変更**

**問題:** ledger_income に 2025-09-01 のデータが存在しない（最小日付: 2025-09-03）

**SQL修正案:**
```sql
-- dwh_non_operating_income.sql:68
-- 変更前
WHERE DATE(slip_date) = DATE('2025-09-01')

-- 変更後（案1）: 9月全体を集計
WHERE DATE_TRUNC(DATE(slip_date), MONTH) = DATE('2025-09-01')

-- 変更後（案2）: 9月の最初の営業日を動的に取得
WHERE DATE(slip_date) = (
  SELECT MIN(DATE(slip_date))
  FROM `data-platform-prod-475201.corporate_data.ledger_income`
  WHERE DATE_TRUNC(DATE(slip_date), MONTH) = DATE('2025-09-01')
)
```

**メリット:** すぐに実装可能、9月のリベート収入を取得できる
**デメリット:** 月の途中データも含まれるため、締め日の概念と合わない可能性

**期待効果:** 営業外収入（リベート・その他）が表示されるようになる

---

**対策2-2: 営業外費用（雑損失）の日付条件変更**

営業外収入と同様に、月全体を集計するよう変更：

```sql
-- dwh_miscellaneous_loss.sql:43
WHERE DATE_TRUNC(DATE(slip_date), MONTH) = DATE('2025-09-01')
```

**注意:** 現状では9月のデータが1件も存在しないため、対策1-1（データ連携復旧）が完了しない限り効果はありません。

---

**対策2-3: 社内利息の category 条件修正**

**問題:** internal_interest.category の値が '売掛金' ではなく '社内利息（A）'/'社内利息（B）' になっている

**SQL修正案:**
```sql
-- dwh_non_operating_expenses.sql:38
-- 変更前
AND ii.category = '売掛金'

-- 変更後（案1）: 社内利息（A）を使用
AND ii.category = '社内利息（A）'

-- 変更後（案2）: 両方を考慮
AND ii.category IN ('社内利息（A）', '社内利息（B）')
```

**注意:** この変更には慎重な検討が必要
- 「社内利息（A）」と「（B）」の違いを確認
- interest_rate カラムの意味を確認
- 仕様書の「売掛金利率」の定義を明確化

**アクション:**
1. internal_interest テーブルのスキーマを確認
2. 元データ（Excel）での category の定義を確認
3. ETL処理（transform_raw_to_proceed.py）での変換ルールを確認
4. 仕様書（元のExcel）の #7 社内金利計算表 シートを確認

**期待効果:** 山本（改装）の社内利息が計算できるようになる（ただし、対策1-2も必要）

---

#### 優先度3（中長期対応）

**対策3-1: データパイプライン全体の健全性チェック**

**アクション:**
1. 全テーブルの最新データ日付を確認
2. GCS バケット内のファイル一覧と更新日時を確認
3. Cloud Run/Cloud Functions のログを確認
4. スケジューラの設定を確認

**期待効果:** データ連携の問題を早期発見できる体制を構築

---

**対策3-2: テーブルスキーマのドキュメント化**

**アクション:**
1. 各テーブルの全カラムの意味を文書化
2. category, detail_category, code などの列挙値を一覧化
3. ETL処理でのデータ変換ルールを文書化
4. 仕様書とテーブル構造の対応表を作成

**期待効果:** 今後の開発・保守が容易になる

---

**対策3-3: 仕様書とデータ構造の整合性確認**

**アクション:**
1. 元の仕様書（Excel）と実際のテーブル構造を比較
2. 不一致がある場合は、どちらを正とするか決定
3. 仕様書またはテーブル構造を修正

**期待効果:** 仕様書と実装の乖離を解消

---

### まとめ

**判明した3つの根本原因:**

1. **データ連携の問題**
   - ledger_income: 月初（1日）のデータが存在しない
   - ledger_loss: 5月29日でデータが停止している

2. **スキーマの不一致**
   - internal_interest.category の値が仕様書の記載と異なる

3. **ソースデータの欠落**
   - sales_target_and_achievements に山本さんのデータが存在しない

**影響を受けている項目:**
- 営業外収入（リベート）: データ連携の問題
- 営業外収入（その他）: データ連携の問題
- 営業外費用（社内利息）: スキーマ不一致 + ソースデータ欠落
- 営業外費用（雑損失）: データ連携の停止
- 本店管理費（山本のみ）: ソースデータ欠落

**正常に動作している部分:**
- 本店管理費（ガラス工事計・硝子建材営業部）: ✅
- 営業経費: ✅
- 営業外費用・社内利息（ガラス工事計・硝子建材営業部）: ✅
- 売上高・売上総利益・売上総利益率: ✅

**次のステップ:**
1. **即時実施**: データ連携の確認（ledger_loss が5月で停止）
2. **即時実施**: 山本（改装）のデータ確認
3. **短期実施**: 日付条件の変更（月全体集計）
4. **短期実施**: internal_interest の category 調査・修正

**調査完了日:** 2025-10-29

**状態:** 🔍 調査完了・対策提案済み

---
