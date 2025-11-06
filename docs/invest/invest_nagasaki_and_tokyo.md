# 長崎支店・東京支店 DWH/DM アーキテクチャ比較調査

## 調査概要

長崎支店向けに経営資料（当月）DataMartを構築するにあたり、東京支店の既存DWH/DMとの統合可能性を調査した。

### 調査対象の選択肢
1. **選択肢①**: 同一のDWH/DMに長崎のデータを挿入してレポートを作成
2. **選択肢②**: DWH/DMを完全に分離（東京用/長崎用で別テーブル）
3. **選択肢③**: 基本は①だが、長崎用のDWHが一部必要

---

## 調査ログ

### 1. 東京支店の現状アーキテクチャ調査

#### 1.1 DWHテーブル構造

**dwh_sales_actual（売上実績テーブル）**:
```sql
Schema:
  year_month              DATE       -- 対象年月
  organization            STRING     -- 組織（工事営業部、硝子建材営業部）
  detail_category         STRING     -- 詳細分類（担当者名・部門名）
  sales_amount            INTEGER    -- 売上高（円）
  gross_profit_amount     INTEGER    -- 粗利額（円）
```

**現在の organization 値**:
- 工事営業部
- 硝子建材営業部

**現在の detail_category 値**:
- 佐々木（大成・鹿島他）、浅井（清水他）、小笠原（三井住友他）、高石（内装・リニューアル）、岡本（清水他）
- 硝子工事、ビルサッシ、硝子販売、サッシ販売、サッシ完成品、その他
- 未分類

**営業所コードのフィルタリング**:
```sql
WHERE branch_code IN (11, 25)  -- 011=工事営業部, 025=硝子建材営業部
```

#### 1.2 その他のDWHテーブル

**operating_expenses（営業経費テーブル）**:
```sql
Schema:
  year_month                  DATE       -- 対象年月
  detail_category             STRING     -- 詳細分類
  operating_expense_amount    INTEGER    -- 営業経費額（円）
```

**現在の detail_category 値**:
- ガラス工事計
- 山本（改装）
- 硝子建材営業部

**特徴**: `organization` カラムが存在せず、`detail_category` のみで分類

**その他のテーブル**:
- `non_operating_income`: 営業外収入（リベート・その他）
- `non_operating_expenses`: 営業外費用（社内利息）
- `miscellaneous_loss`: 雑損失
- `head_office_expenses`: 本店管理費
- `dwh_sales_target`: 売上目標
- `dwh_sales_actual_prev_year`: 前年実績
- `dwh_recurring_profit_target`: 経常利益目標
- `operating_expenses_target`: 営業経費目標
- `operating_income_target`: 営業利益目標

すべてのテーブルで **支店を識別するカラムが存在しない**（暗黙的に東京支店のみを想定）

#### 1.3 DataMartテーブル構造

**management_documents_all_period**:
```sql
Schema:
  date                                DATE       -- 対象年月
  main_category                       STRING     -- 売上高、売上総利益等
  main_category_sort_order            INTEGER    -- ソート順
  secondary_category                  STRING     -- 前年実績(千円)、本年目標(千円)等
  secondary_category_graphname        STRING     -- グラフ表示用名称
  secondary_category_sort_order       INTEGER    -- ソート順
  main_department                     STRING     -- 最上位部門
  secondary_department                STRING     -- 詳細部門
  secondary_department_newline        STRING     -- 改行付き部門名
  secondary_department_sort_order     INTEGER    -- ソート順
  value                               FLOAT      -- 集計値
  display_value                       FLOAT      -- 表示用値
```

**現在の main_department 値**:
- 東京支店（固定）

**現在の secondary_department 値**:
- 東京支店計
- 工事営業部計、ガラス工事計、佐々木（大成・鹿島他）、浅井（清水他）...
- 硝子建材営業部、硝子工事、ビルサッシ、硝子販売...

---

### 2. 長崎支店の要件調査

#### 2.1 組織構造

**長崎支店の階層**:
```
長崎支店計
├── 工事営業部計
│   ├── ガラス工事
│   └── ビルサッシ
└── 硝子建材営業部計
    ├── 硝子工事
    ├── サッシ工事
    ├── 硝子販売
    ├── サッシ販売
    └── 完成品（その他）
```

#### 2.2 営業所コード

**長崎支店の営業所コード**:
- **061**: 工事営業部（ガラス工事、ビルサッシ）
- **065, 066**: 硝子建材営業部（硝子工事、サッシ工事、硝子販売、サッシ販売、完成品）

**東京支店の営業所コード（比較）**:
- **011**: 工事営業部（担当者別）
- **025**: 硝子建材営業部（部門別）

→ **営業所コードが完全に異なる**（重複なし）

#### 2.3 データソースの違い

| データ種別 | 東京支店 | 長崎支店 |
|-----------|---------|---------|
| 損益実績 | 損益4期実績（東京支店シート） | 損益4期実績（長崎支店シート） |
| 損益目標 | 損益5期目標（東京支店シート） | 損益5期目標（長崎支店シート） |
| 売上実績 | #1 担当者売上（営業所011, 025） | #1 担当者売上（営業所061, 065, 066） |
| 営業経費 | #6 部門集計表（部門61, 62, 63） | #6 部門集計表（部門61, 62, 63）※同じファイルの別セクション |
| 業務部案分 | なし（業務部がない） | あり（業務部63を案分） |

#### 2.4 計算ロジックの違い

**東京支店**:
- 工事営業部: 担当者別（佐々木、浅井、小笠原等）の実績を集計
- 営業経費: 直接集計（案分なし）
- 業務部門: 存在しない

**長崎支店**:
- 工事営業部: 部門別（ガラス工事、ビルサッシ）の実績を集計
- 営業経費: 部門別集計 + 業務部（63）の費用を案分して加算
- 業務部門: 案分比率マスタに基づき工事・建材に配分

---

### 3. 統合可能性の検証

#### 3.1 DWHレイヤーでの統合検証

**dwh_sales_actual テーブルに長崎データを追加する場合**:

**現在のスキーマ**:
```sql
year_month, organization, detail_category, sales_amount, gross_profit_amount
```

**追加したい長崎データ**:
```
2025-09-01, 工事営業部, ガラス工事（長崎）, 40000000, 8000000
2025-09-01, 工事営業部, ビルサッシ（長崎）, 20000000, 4000000
```

**問題点**:
1. **organization名の衝突**: 東京の「工事営業部」と長崎の「工事営業部」が同じ名前
2. **detail_categoryの区別が必要**: 東京の「ガラス工事」と長崎の「ガラス工事」は別物だが、現状のスキーマでは区別不可能
3. **支店識別カラムの欠如**: `branch` や `branch_name` カラムが存在しない

**解決策の検討**:

**案A: detail_categoryに支店名を追加**
```sql
detail_category = 'ガラス工事（長崎）', 'ガラス工事（東京）'
```
- メリット: スキーマ変更不要
- デメリット: 命名規則が複雑化、WHERE句でのフィルタリングが面倒

**案B: branchカラムを追加**
```sql
ALTER TABLE dwh_sales_actual ADD COLUMN branch STRING;
-- データ例: branch='東京', branch='長崎'
```
- メリット: 明示的な支店識別、フィルタリングが容易
- デメリット: スキーマ変更が必要、既存の東京データにも `branch='東京'` を追加する必要がある

#### 3.2 営業経費等のテーブルでの検証

**operating_expenses テーブルの現状**:
```sql
year_month, detail_category, operating_expense_amount
```

**現在の detail_category**:
- ガラス工事計
- 山本（改装）
- 硝子建材営業部

**長崎を追加する場合**:
- 工事営業部計（長崎）
- 硝子建材営業部計（長崎）

→ **同じ問題が発生**: 支店識別カラムがないため、東京と長崎を区別できない

#### 3.3 DataMartレイヤーでの統合検証

**management_documents_all_period の現状**:
```sql
main_department = '東京支店'（固定）
secondary_department = '東京支店計', '工事営業部計', ...
```

**長崎を追加する場合**:
```sql
main_department = '長崎支店'
secondary_department = '長崎支店計', '工事営業部計', ...
```

**検証結果**:
- `main_department` で支店を識別できる → **統合可能**
- `secondary_department` は支店ごとに異なる値を持つため衝突しない
- Looker Studioでのフィルタリング: `main_department='長崎支店'` で長崎のみ表示可能

**メリット**:
- DataMartレイヤーでは支店識別が容易
- 1つのテーブルで全支店のデータを管理できる
- ダッシュボードで支店切り替えが可能

**デメリット**:
- DWHからDataMartへの変換時に `main_department` を正しく設定する必要がある

---

### 4. 各選択肢の詳細分析

#### 選択肢①: 同一のDWH/DMに長崎のデータを挿入

**実装イメージ**:

**DWHレイヤー**:
- 全てのDWHテーブルに `branch STRING` カラムを追加
- 既存の東京データに `branch='東京'` を設定
- 長崎データは `branch='長崎'` で挿入
- SQLのWHERE句を修正: `WHERE branch_code IN (11, 25)` → `WHERE branch_code IN (11, 25, 61, 65, 66)`

**DataMartレイヤー**:
- 既存のテーブル `management_documents_all_period` をそのまま使用
- `main_department` に '東京支店' または '長崎支店' を設定
- Looker Studioでは `main_department` でフィルタリング

**メリット**:
- テーブル数が増えない（管理が容易）
- 支店間の比較が容易（1つのクエリで複数支店を集計可能）
- ETLパイプラインが1つで済む（コード重複が少ない）

**デメリット**:
- DWHテーブルのスキーマ変更が必要（`branch` カラム追加）
- 既存の東京データにもカラム追加が必要（UPDATE文でデータ修正）
- SQLロジックが複雑化（支店ごとの条件分岐が増える）

**データ例**:
```sql
-- dwh_sales_actual
year_month   | branch | organization  | detail_category        | sales_amount
-------------|--------|---------------|------------------------|-------------
2025-09-01   | 東京   | 工事営業部    | 佐々木（大成・鹿島他） | 50000000
2025-09-01   | 長崎   | 工事営業部    | ガラス工事             | 40000000

-- management_documents_all_period
date         | main_department | secondary_department | main_category | value
-------------|-----------------|----------------------|---------------|----------
2025-09-01   | 東京支店        | 東京支店計           | 売上高        | 300000000
2025-09-01   | 長崎支店        | 長崎支店計           | 売上高        | 150000000
```

---

#### 選択肢②: DWH/DMを完全に分離

**実装イメージ**:

**DWHレイヤー**:
- 東京用: `dwh_sales_actual_tokyo`, `operating_expenses_tokyo`, ...
- 長崎用: `dwh_sales_actual_nagasaki`, `operating_expenses_nagasaki`, ...

**DataMartレイヤー**:
- 東京用: `management_documents_tokyo`
- 長崎用: `management_documents_nagasaki`

**メリット**:
- スキーマ変更不要（新規テーブルとして作成）
- 東京と長崎のデータが完全に分離されて安全
- 各支店のETLを独立して実行可能（障害の影響範囲が限定的）

**デメリット**:
- テーブル数が2倍になる（管理コスト増）
- ETLコードが重複する（DRY原則に反する）
- 支店間の比較が困難（UNIONクエリが必要）
- 今後支店が増えるたびにテーブルを追加する必要がある

**将来の拡張性**:
- 大阪支店、福岡支店...と増えるたびにテーブルが増加
- テーブル管理が煩雑になる

---

#### 選択肢③: 基本は①だが、長崎用のDWHが一部必要

**実装イメージ**:

**DWHレイヤー（共通化できるテーブル）**:
- `dwh_sales_actual`: `branch` カラムを追加して東京・長崎を統合
- `dwh_sales_target`: 同上
- `dwh_sales_actual_prev_year`: 同上

**DWHレイヤー（分離が必要なテーブル）**:
- `operating_expenses_tokyo`: 東京の営業経費（業務部案分なし）
- `operating_expenses_nagasaki`: 長崎の営業経費（業務部案分あり）
- `non_operating_income_tokyo`: 東京の営業外収入
- `non_operating_income_nagasaki`: 長崎の営業外収入（業務部案分あり）

**DataMartレイヤー**:
- `management_documents_all_period`: 統合テーブル（`main_department` で識別）

**メリット**:
- 計算ロジックが大きく異なる部分のみ分離（適切な責務分離）
- 売上実績など共通ロジックは統合（コード重複を削減）
- DataMartは統合されているため、ダッシュボードで支店比較可能

**デメリット**:
- どのテーブルを統合し、どのテーブルを分離するか判断が必要
- ETLパイプラインが一部重複する（管理コスト増）
- テーブル命名規則が複雑化（`_tokyo`, `_nagasaki` サフィックス）

---

## 結論

### 選択した選択肢: **③ 基本は①だが、長崎用のDWHが一部必要**

---

## 選択の根拠

### 1. DataMartは統合すべき理由

**ビジネス要件**:
- 支店間の業績比較が必要になる可能性が高い
- 全社レポートを作成する際、支店ごとに別テーブルからUNIONするのは非効率
- Looker Studioで1つのダッシュボードから支店を切り替えられる方が使いやすい

**技術的理由**:
- `main_department` カラムで支店を識別できるため、スキーマは既に対応済み
- クエリのパフォーマンス: パーティション+クラスタリングで支店ごとのフィルタリングも高速

### 2. DWHは一部分離すべき理由

**計算ロジックの差異**:
| テーブル | 東京 | 長崎 | 統合可否 |
|---------|------|------|---------|
| dwh_sales_actual | 営業所コード 011, 025 | 営業所コード 061, 065, 066 | ✅ 統合可能（営業所コードが異なるだけ） |
| dwh_sales_target | 東京支店シート | 長崎支店シート | ✅ 統合可能（シート名が異なるだけ） |
| operating_expenses | 直接集計 | 業務部案分あり | ❌ ロジックが異なる → 分離推奨 |
| non_operating_income | 直接集計 | 業務部案分あり | ❌ ロジックが異なる → 分離推奨 |
| non_operating_expenses | 営業所コード 011, 025 | 営業所コード 061, 065, 066 | ⚠️ 要調査（利率マスタが支店ごとに異なる可能性） |

**分離推奨テーブル**:
1. **operating_expenses**: 長崎は業務部（63）の案分が必要だが、東京は不要
2. **operating_expenses_target**: 同上
3. **non_operating_income**: 長崎はリベートも業務部案分が必要
4. **miscellaneous_loss**: 長崎は業務部案分が必要
5. **head_office_expenses**: 長崎は業務部案分なし（直接割当）だが、東京との差異を確認する必要がある

### 3. 具体的な実装方針

#### Phase 1: 売上系テーブルの統合（短期）

**統合するテーブル**:
- `dwh_sales_actual`
- `dwh_sales_actual_prev_year`
- `dwh_sales_target`
- `dwh_recurring_profit_target`
- `operating_income_target`

**実装方法**:
1. `branch STRING` カラムを追加
2. 既存の東京データに `branch='東京'` を追加（UPDATE文）
3. SQLのWHERE句を拡張して長崎の営業所コードを追加

**SQL例**:
```sql
-- Before
WHERE branch_code IN (11, 25)

-- After
WHERE
  (branch_code IN (11, 25) AND branch = '東京')
  OR (branch_code IN (61, 65, 66) AND branch = '長崎')
```

または、branch カラムを追加せず、営業所コードのみで識別:
```sql
WHERE branch_code IN (11, 25, 61, 65, 66)
```

その後、DataMartで `main_department` を設定:
```sql
CASE
  WHEN branch_code IN (11, 25) THEN '東京支店'
  WHEN branch_code IN (61, 65, 66) THEN '長崎支店'
END AS main_department
```

#### Phase 2: 経費系テーブルの分離作成（中期）

**新規作成するテーブル**:
- `operating_expenses_nagasaki`
- `non_operating_income_nagasaki`
- `non_operating_expenses_nagasaki`（要調査後に決定）
- `miscellaneous_loss_nagasaki`

**実装方法**:
1. 長崎支店専用のSQL作成（`sql/split_dwh_dm/dwh_operating_expenses_nagasaki.sql`）
2. 業務部（63）の案分ロジックを実装
3. DataMart生成時に、東京用と長崎用のテーブルを UNION

**DataMart SQL例**:
```sql
-- 営業経費の統合
operating_expenses_all AS (
  SELECT
    year_month,
    '東京' AS branch,
    detail_category,
    operating_expense_amount
  FROM `corporate_data_dwh.operating_expenses`  -- 東京用（既存）

  UNION ALL

  SELECT
    year_month,
    '長崎' AS branch,
    detail_category,
    operating_expense_amount
  FROM `corporate_data_dwh.operating_expenses_nagasaki`  -- 長崎用（新規）
)
```

#### Phase 3: DataMartの統合（長期）

**統合方針**:
- テーブル名: `management_documents_all_period`（既存）を継続使用
- `main_department` に '東京支店' または '長崎支店' を設定
- `secondary_department` には支店ごとの組織階層を設定

**ダッシュボード設計**:
- Looker Studioで `main_department` によるフィルタコントロール追加
- 全支店比較ビューと支店別詳細ビューを用意

---

## 懸念点（追加で必要なこと）

### 1. 既存東京データへの影響

**懸念**: DWHテーブルに `branch` カラムを追加すると、既存のSQLやダッシュボードに影響が出る可能性

**対策**:
- まずは `branch` カラムなしで実装（営業所コードのみで識別）
- DataMartレイヤーで `main_department` を設定する際に支店を識別
- 必要に応じて後からDWHに `branch` カラムを追加

### 2. 業務部案分ロジックの実装

**懸念**: 案分比率マスタ（#10）の構造が不明で、実装方法が確定していない

**対策**:
- 案分比率マスタのスキーマを調査
- 案分ロジックを別関数またはCTEとして実装
- テストデータで計算結果を検証

### 3. 在庫損益・前受け金の手入力対応

**懸念**: スプレッドシートからの手入力をどう実装するか未定

**対策**:
- Google Sheetsとの連携方法を設計
- ETLパイプラインでシートデータを読み込む処理を追加
- 入力検証ロジックを実装（異常値チェック）

### 4. 前年実績の過去データ取得

**懸念**: TKSデータから過去データを取得・計算する方法が未確定

**対策**:
- TKSデータの構造を調査
- データ移行スクリプトを作成
- 過去2年分のデータを一括取得してDWHに登録

### 5. パフォーマンスへの影響

**懸念**: 支店データが増えることでクエリ実行時間が増加する可能性

**対策**:
- パーティショニング: `date` カラムで月次パーティション
- クラスタリング: `main_department`, `main_category` でクラスタリング
- インデックス: 必要に応じてBigQueryのクラスタリングを最適化

### 6. ETLパイプラインの管理

**懸念**: 支店ごとにETLを実行する場合、スケジューリングとエラーハンドリングが複雑化

**対策**:
- Cloud Schedulerで支店ごとにジョブを分離
- エラー通知: Cloud Loggingと連携してSlack/Email通知
- リトライロジック: 一時的なエラーは自動リトライ

### 7. テスト戦略

**懸念**: 長崎データを追加した際、東京データに影響が出ないか検証が必要

**対策**:
- 開発環境（`data-platform-dev`）で先行実装
- 東京支店の既存レポートと新レポートを比較検証
- 差分がないことを確認してから本番環境に展開

---

## 調査していて理解できなかったところ

### 1. 硝子・樹脂部門の統合予定の詳細

**仕様書の記載**:
> 硝子・樹脂部門は統合する予定のため、ロジックも若干変更になる予定です。

**不明点**:
- 統合のタイミングはいつか？
- 統合後の組織コードはどうなるか？（065と066が統合される？）
- 統合後の detail_category はどう変わるか？

**影響範囲**:
- 長崎支店の営業所コード 065, 066 のロジックが変更される可能性
- DataMartの secondary_department の命名規則が変わる可能性

**対応方針**:
- 現時点では現状の組織構造で実装
- 統合時にはSQLロジックを修正
- 過去データとの整合性を保つため、統合前後のデータを明示的に区別

### 2. 東京支店の業務部門の有無

**調査結果**:
- 長崎支店には業務部（63）が存在し、費用を案分する
- 東京支店の既存SQLには業務部の記載がない

**不明点**:
- 東京支店にも業務部門は存在するのか？
- 存在する場合、なぜ既存SQLで案分していないのか？
- 存在しない場合、なぜ長崎だけ業務部があるのか？

**影響範囲**:
- `operating_expenses` テーブルの統合可否に影響
- もし東京にも業務部があるなら、既存ロジックが間違っている可能性

**対応方針**:
- ユーザーに確認が必要
- 東京の部門集計表（#6）を確認して業務部の有無を調査

### 3. 社内利息計算の支店間差異

**長崎の計算要素**:
- 売掛金、未落手形、在庫・未成工事、建物利息、償却資産利息
- それぞれに利率を掛け算

**不明点**:
- 東京支店の社内利息計算も同じロジックか？
- 利率マスタ（#7）は支店ごとに異なる値を持つのか？
- 在庫マスタ（#9）の構造は？

**影響範囲**:
- `non_operating_expenses` テーブルを統合できるかどうか
- 統合する場合、支店ごとに利率を切り替える必要がある

**対応方針**:
- 東京支店の `non_operating_expenses` SQLを確認
- 利率マスタと在庫マスタのスキーマを調査
- 計算ロジックが同じなら統合、異なるなら分離

### 4. 前受け金の計上方法

**仕様書の記載**:
> 半期に一度だけ前受け金を加算する必要があります。これはスプレッドシートに手入力としたく、実装方法は相談させてください。

**不明点**:
- どのスプレッドシートに入力するのか？
- 入力タイミングはいつか？（決算月のみ？）
- 前受け金は売掛金に加算するのか、別の項目か？

**影響範囲**:
- 社内利息計算の売掛金部分に影響
- ETLパイプラインでスプレッドシート読み込みが必要

**対応方針**:
- ユーザーと相談して仕様を確定
- 一旦、前受け金なしで実装
- スプレッドシート連携は別タスクとして切り出し

### 5. 本店管理費の案分ルール

**仕様書の記載**:
> 本店管理費は業務部の案分なし（直接割当）

**不明点**:
- 東京支店の本店管理費はどう計算されているか？
- コード 8366 の部門集計表には支店別のデータが入っているのか？
- 長崎の「直接割当」とは、部門61/62の値をそのまま使うという意味か？

**影響範囲**:
- `head_office_expenses` テーブルの統合可否
- DataMartでの集計方法

**対応方針**:
- 東京支店の `head_office_expenses` SQLを確認
- 部門集計表（#6）のコード 8366 を確認
- 計算ロジックを比較して統合可否を判断

---

## まとめ

### 推奨アーキテクチャ

```
[Raw Data Sources (Drive)]
    ↓
[corporate_data - Rawデータ]
    ↓
[corporate_data_dwh - DWH Layer]
    ├── 【統合テーブル】
    │   ├── dwh_sales_actual (branch不要、営業所コードで識別)
    │   ├── dwh_sales_actual_prev_year
    │   ├── dwh_sales_target
    │   ├── dwh_recurring_profit_target
    │   └── operating_income_target
    │
    └── 【分離テーブル】
        ├── operating_expenses (東京用、既存)
        ├── operating_expenses_nagasaki (長崎用、新規)
        ├── operating_expenses_target (東京用、既存)
        ├── operating_expenses_target_nagasaki (長崎用、新規)
        ├── non_operating_income (東京用、既存)
        ├── non_operating_income_nagasaki (長崎用、新規)
        ├── non_operating_expenses (要調査後に決定)
        ├── miscellaneous_loss (東京用、既存)
        └── miscellaneous_loss_nagasaki (長崎用、新規)
    ↓
[corporate_data_dm - DataMart Layer]
    └── management_documents_all_period (統合)
        ├── main_department='東京支店'
        └── main_department='長崎支店'
```

### 実装ステップ

**Step 1**: 売上系DWHテーブルの拡張（営業所コード追加）
**Step 2**: 長崎専用の経費系DWHテーブルを作成
**Step 3**: DataMart生成SQLを修正（東京・長崎を統合）
**Step 4**: Looker Studioダッシュボードを更新（支店フィルタ追加）
**Step 5**: テストと検証（開発環境で実施）
**Step 6**: 本番展開

### 今後の拡張性

- 大阪支店、福岡支店等が追加される場合も同じパターンで対応可能
- 営業所コードを追加するだけで、DWHテーブルは自動的に対応
- 経費系テーブルは支店ごとの計算ロジックに応じて分離または統合を判断
