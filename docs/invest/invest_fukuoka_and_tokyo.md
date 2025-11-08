# 福岡支店DataMart実装方針

## 🎯 実装方針の提案

### 推奨: **選択肢③ - 基本は①だが、福岡用のDWHが一部必要**

**理由**:
1. 長崎支店の実装と同じパターンで進められる
2. 東京・長崎・福岡の統合DataMartを`management_documents_all_period_all`で一元管理
3. 福岡固有の組織構造(GSセンター、福北センター等)に対応可能
4. 既存の東京・長崎のデータに影響を与えない

### 方針③の詳細

#### A. 福岡専用DWHテーブル (corporate_data_dwh)
福岡の組織構造に合わせた専用DWHテーブルを作成:

1. **売上・粗利系**
   - `dwh_sales_actual_fukuoka` - 本年実績(売上・粗利)
   - `dwh_sales_actual_prev_year_fukuoka` - 前年実績(売上・粗利)
   - `dwh_sales_target_fukuoka` - 本年目標(売上・粗利)

2. **経費系**
   - `operating_expenses_fukuoka` - 営業経費(業務部案分込み)
   - `non_operating_income_fukuoka` - 営業外収入(リベート・その他)
   - `miscellaneous_loss_fukuoka` - 雑損失(業務部案分込み)
   - `head_office_expenses_fukuoka` - 本店管理費

3. **目標系** (既に作成済み)
   - `profit_plan_term_fukuoka` - 損益5期目標(福岡)元テーブル
   - `dwh_recurring_profit_target_fukuoka` - 経常利益目標
   - `operating_expenses_target_fukuoka` - 営業経費目標
   - `operating_income_target_fukuoka` - 営業利益目標

4. **社内利息** (複雑なため後回し)
   - `non_operating_expenses_fukuoka` - 営業外費用(社内利息A・B)

#### B. 福岡専用DataMart (corporate_data_dm)
- `management_documents_all_period_fukuoka` - 福岡支店単独DataMart

#### C. 統合DataMart (corporate_data_dm)
- `management_documents_all_period_all` に福岡データを追加
  - 東京支店 (4,904行)
  - 長崎支店 (3,355行)
  - 福岡支店 (新規追加)

---

## 📋 具体的な実装方針

### 1. データソース別の実装アプローチ

#### ① 売上・粗利実績 (本年実績)
**データソース**: #1 全支店[1.売上管理] 担当者売上目標／実績データ

**実装**:
```sql
-- dwh_sales_actual_fukuoka.sql
CREATE OR REPLACE TABLE corporate_data_dwh.dwh_sales_actual_fukuoka AS
SELECT
  sales_accounting_period AS year_month,
  CASE
    WHEN branch_code = '030' AND division_code IN ('010', '011') THEN '硝子工事'
    WHEN branch_code = '030' AND division_code = '021' THEN 'ビルサッシ'
    WHEN branch_code = '034' THEN '内装工事'
    WHEN branch_code = '031' AND division_code IN ('010', '011') THEN '硝子'
    WHEN branch_code = '031' AND division_code IN ('030', '031') THEN '樹脂'
    WHEN branch_code = '031' AND division_code NOT IN ('010', '011', '030', '031') THEN '建材'
    WHEN branch_code = '037' THEN '福北センター'
    ELSE 'その他'
  END AS detail_category,
  CASE
    WHEN branch_code IN ('030', '034') THEN '工事部'
    WHEN branch_code = '031' THEN '硝子樹脂'
    WHEN branch_code = '037' THEN '福北センター'
    ELSE 'その他'
  END AS organization,
  SUM(sales_actual) AS sales_amount,
  SUM(gross_profit_actual) AS gross_profit_amount
FROM corporate_data.sales_target_and_achievements
WHERE branch_code IN ('030', '031', '034', '037')
GROUP BY year_month, organization, detail_category;
```

**課題**:
- GSセンターの売上・粗利はスプレッドシート手入力 → 別途テーブル作成が必要
- 在庫損益の半年1回計上 → スプレッドシート連携方法を検討

#### ② 売上・粗利 (前年実績)
**データソース**: #11 損益4期実績 (福岡支店シート)

**実装**: `profit_plan_term_fukuoka_prev_year` テーブルを作成 (要検討)

**課題**:
- **福北センターの前年実績データが過去から算出不可** → 別途データ投入方法が必要

#### ③ 売上・粗利 (本年目標)
**データソース**: #12 損益5期目標 (福岡支店シート)

**実装**: 既に`profit_plan_term_fukuoka`テーブル作成済み
- DWH変換SQL作成が必要

#### ④ 営業経費 (本年実績)
**データソース**: #6 部門集計表

**実装**: 長崎と同様、業務部の案分ロジックを実装
```sql
-- operating_expenses_fukuoka.sql
-- 部門コード61=工事部, 62=硝子樹脂, 63=業務部(案分対象), 38=GS, 40-46=福北
```

**課題**:
- 福北センターは8行目の値で複数の識別子を使用 → データ確認が必要
- 福北センターの業務部案分比率は5期は0%

#### ⑤ 営業外収入 (リベート・その他)
**データソース**: #4 元帳_雑収入

**実装**: 長崎と同じロジック、部門コードを福岡用に変更
```sql
-- non_operating_income_fukuoka.sql
-- 部門コード: 31=工事部, 34=樹脂, 38=GS, 40-46=福北
```

**課題**:
- 全角・半角の「リベート」判定
- 税込み→税抜き計算

#### ⑥ 営業外費用 (社内利息A・B)
**データソース**: #3 請求残高、#7 社内金利計算表、#9 在庫、#10 案分比率マスタ

**実装**: 長崎と類似だが、一部ロジックが異なる

**課題**:
- **長崎と若干異なるロジック → 30分程度の打合せで詳細確認が必要**
- 半期に1回の前受け金加算 → スプレッドシート入力方法を検討
- 複雑なため、優先度を下げる可能性あり

#### ⑦ 雑損失
**データソース**: #6 部門集計表

**実装**: コード8730の値を業務部案分で集計

#### ⑧ 本店管理費
**データソース**: #6 部門集計表

**実装**: コード8366の値を集計

---

### 2. 実装の優先順位

#### フェーズ1: 基本データの実装
1. ✅ `profit_plan_term_fukuoka` テーブル作成 (完了)
2. 🔲 `dwh_sales_actual_fukuoka` - 本年実績(売上・粗利)
3. 🔲 `dwh_sales_target_fukuoka` - 本年目標(売上・粗利)
4. 🔲 `operating_expenses_fukuoka` - 営業経費
5. 🔲 `non_operating_income_fukuoka` - 営業外収入
6. 🔲 `miscellaneous_loss_fukuoka` - 雑損失
7. 🔲 `head_office_expenses_fukuoka` - 本店管理費

#### フェーズ2: 目標データの実装
1. 🔲 `dwh_recurring_profit_target_fukuoka` - 経常利益目標
2. 🔲 `operating_expenses_target_fukuoka` - 営業経費目標
3. 🔲 `operating_income_target_fukuoka` - 営業利益目標

#### フェーズ3: DataMart統合
1. 🔲 `management_documents_all_period_fukuoka` - 福岡単独DataMart
2. 🔲 `management_documents_all_period_all` に福岡データを追加

#### フェーズ4: 保留・要相談項目
1. ⏸️ `dwh_sales_actual_prev_year_fukuoka` - 前年実績 (福北センターデータ投入方法要相談)
2. ⏸️ `non_operating_expenses_fukuoka` - 社内利息 (ロジック詳細確認が必要)
3. ⏸️ GSセンター手入力データの連携方法
4. ⏸️ 在庫損益の半年1回計上方法

---

### 3. 組織階層マッピング

#### 福岡支店の組織構造
```
福岡支店
├─ 工事部
│  ├─ 硝子工事 (営業所030, 部門010/011)
│  ├─ ビルサッシ (営業所030, 部門021)
│  └─ 内装工事 (営業所034)
├─ 硝子樹脂
│  ├─ 硝子 (営業所031, 部門010/011)
│  ├─ 建材 (営業所031, 部門010/011/030/031以外)
│  └─ 樹脂 (営業所031, 部門030/031)
├─ GSセンター (手入力)
└─ 福北センター (営業所037)
```

#### DataMartでの階層表現
- `main_department`: 福岡支店
- `organization`: 工事部 / 硝子樹脂 / GSセンター / 福北センター
- `secondary_department`:
  - 福岡支店計
  - 工事部計、硝子工事、ビルサッシ、内装工事
  - 硝子樹脂計、硝子、建材、樹脂
  - GSセンター
  - 福北センター

---

### 4. テーブル設計

#### スキーマ統一方針
東京・長崎と同じスキーマを使用:

**DWHテーブル**:
```
year_month DATE
organization STRING
detail_category STRING
sales_amount FLOAT64
gross_profit_amount FLOAT64
```

**DataMartテーブル**:
```
date DATE
main_category STRING
main_category_sort_order INTEGER
secondary_category STRING
secondary_category_sort_order INTEGER
main_department STRING
secondary_department STRING
secondary_department_sort_order INTEGER
value FLOAT64
display_value FLOAT64
```

---

### 5. 選択肢①/②との比較

#### 選択肢①: 東京のDWH/DMに福岡データを挿入
**メリット**:
- テーブル数が少ない
- 統合DataMartが1つで済む

**デメリット**:
- 福岡固有の組織構造(GSセンター、福北センター)が東京のスキーマに合わない
- 営業所コードの競合リスク
- SQLが複雑になり、メンテナンス性が低下

#### 選択肢②: DWH/DMを完全に分離
**メリット**:
- 支店間の独立性が高い
- 変更の影響範囲が限定的

**デメリット**:
- テーブル数が増加
- 統合レポート作成時にUNION ALLが複雑化
- コード重複が多い

#### 選択肢③: 基本は①だが、福岡用のDWHが一部必要 (推奨)
**メリット**:
- 長崎支店と同じ実装パターン
- 福岡固有の組織構造に柔軟に対応
- 統合DataMartで東京・長崎・福岡を一元管理
- 既存データへの影響なし

**デメリット**:
- DWHテーブルが増える
- 初期実装コストが高い

---

## 🚀 実装手順

### ステップ1: データソース確認
1. #1 sales_target_and_achievements で福岡の営業所コード(030, 031, 034, 037)のデータ確認
2. #6 department_summary で福岡支店の部門コード確認
3. #10 ms_allocation_ratio で福岡の案分比率確認

### ステップ2: DWHテーブル作成
1. `dwh_sales_actual_fukuoka.sql` 作成
2. `dwh_sales_target_fukuoka.sql` 作成
3. `operating_expenses_fukuoka.sql` 作成
4. `non_operating_income_fukuoka.sql` 作成
5. `miscellaneous_loss_fukuoka.sql` 作成
6. `head_office_expenses_fukuoka.sql` 作成

### ステップ3: 目標系DWHテーブル作成
1. `dwh_recurring_profit_target_fukuoka.sql` 作成
2. `operating_expenses_target_fukuoka.sql` 作成
3. `operating_income_target_fukuoka.sql` 作成

### ステップ4: DataMart作成
1. `datamart_management_report_fukuoka.sql` 作成 (長崎版を参考)
2. `datamart_management_report_all.sql` に福岡データを追加

### ステップ5: 検証
1. 福岡単独DataMartのデータ確認
2. 統合DataMartで東京・長崎・福岡のデータが正しく分離されているか確認
3. サンプルクエリでの動作確認

---

## 📝 次のステップ

1. question_fukuoka.md に不明点・相談事項を記載
2. データソースの実データ確認
3. DWH SQL作成開始
4. 手入力データの連携方法検討
