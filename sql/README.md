# 経営資料ダッシュボードSQL

## 概要

BigQueryのデータをLooker Studioで可視化するためのSQLクエリです。
月次の損益計算書（P/L）を組織階層別に集計します。

## ファイル構成

```
sql/
├── README.md                          # このファイル
└── dashboard_management_report.sql    # メインクエリ
```

## 使用方法

### 1. パラメータの設定

SQLファイルの冒頭でパラメータを設定します：

```sql
DECLARE target_month DATE DEFAULT '2025-09-01';        -- 対象月（前月）
DECLARE fiscal_year_start DATE DEFAULT '2025-04-01';  -- 期首
DECLARE two_months_ago DATE DEFAULT DATE_SUB(target_month, INTERVAL 2 MONTH);
```

### 2. BigQueryでの実行

```bash
# コマンドラインから実行
bq query --use_legacy_sql=false < sql/dashboard_management_report.sql

# または、BigQuery Console上で直接実行
```

### 3. Looker Studioでの利用

1. Looker Studioで新規データソースを作成
2. BigQueryコネクタを選択
3. 「カスタムクエリ」を選択して、このSQLを貼り付け
4. パラメータをLooker Studioの日付フィルターと連携

## 出力カラム

### メタデータ
| カラム名 | 説明 | 例 |
|---------|------|-----|
| `report_month` | レポート対象月 | 2025-09-01 |
| `organization` | 組織名 | 工事営業部 |
| `detail_category` | 詳細分類 | 佐々木（大成・鹿島他） |

### 売上高（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `sales_prev_year_k` | 前年実績 |
| `sales_target_k` | 本年目標 |
| `sales_actual_k` | 本年実績 |

### 売上総利益（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `gross_profit_prev_year_k` | 前年実績 |
| `gross_profit_target_k` | 本年目標 |
| `gross_profit_actual_k` | 本年実績 |

### 売上総利益率（単位: %）
| カラム名 | 説明 |
|---------|------|
| `gross_profit_margin_prev_year_pct` | 前年実績 |
| `gross_profit_margin_target_pct` | 本年目標 |
| `gross_profit_margin_actual_pct` | 本年実績 |

### 営業経費（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `operating_expense_prev_year_k` | 前年実績 |
| `operating_expense_target_k` | 本年目標 |
| `operating_expense_actual_k` | 本年実績 |

### 営業利益（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `operating_income_prev_year_k` | 前年実績 |
| `operating_income_target_k` | 本年目標 |
| `operating_income_actual_k` | 本年実績 |

### 営業外損益（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `rebate_income_k` | リベート収入 |
| `other_non_operating_income_k` | その他営業外収入 |
| `non_operating_expenses_k` | 営業外費用（社内利息） |

### 本店管理費・経常利益（単位: 千円）
| カラム名 | 説明 |
|---------|------|
| `head_office_expense_k` | 本店管理費 |
| `recurring_profit_target_k` | 経常利益目標 |
| `recurring_profit_actual_k` | 経常利益実績 |

## 組織階層

```
東京支店計
├── 工事営業部計
│   ├── ガラス工事計
│   │   ├── 佐々木（大成・鹿島他）
│   │   ├── 岡本（清水他）
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

## データソーステーブル

| テーブル名 | 用途 |
|-----------|------|
| `sales_target_and_achievements` | 売上・粗利実績 |
| `profit_plan_term` | 目標値、前年実績 |
| `department_summary` | 営業経費、本店管理費、社内利息 |
| `ledger_income` | 営業外収入（リベート等） |
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

### リベート判定
- 全角「リベート」と半角「リベート」の両方を判定
- `ledger_income.description_comment`カラムで判定

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
