# 変更履歴

## 2025-10-27 (Part 2)

### 🚀 Cloud Runへの統一とエンドツーエンドパイプライン完成

#### アーキテクチャ変更
- **Cloud Functionを廃止**、すべてCloud Runに統一
- **理由**: 柔軟性、デバッグの容易さ、実装の一貫性

#### 新規実装: `gcs-to-bq` Cloud Runサービス
- **ディレクトリ**: `gcs_to_bq_service/`
- **エンドポイント**:
  1. `POST /transform` - Excel → CSV変換（raw/ → proceed/）
  2. `POST /load` - CSV → BigQuery ロード
  3. `POST /pubsub` - Pub/Subトリガー（自動実行）
  4. `GET /` - ヘルスチェック

- **機能**:
  - `transform_raw_to_proceed.py`の処理をCloud Run化
  - `load_to_bigquery.py`の処理をCloud Run化
  - テーブル・カラム説明の自動設定（GCS上のマッピングファイルから取得）
  - パーティションの既存データ削除オプション

#### Pub/Sub連携の構築
```
[Google Drive]
     ↓
[Pub/Sub: drive-monthly]
     ↓
[Cloud Run: drive-to-gcs]
     ↓
[GCS: raw/202509/*.xlsx]
     ↓
[Pub/Sub: transform-trigger] ← 新規作成
     ↓
[Cloud Run: gcs-to-bq]
     ├─ /transform: Excel→CSV変換
     └─ /load: CSV→BigQuery
     ↓
[BigQuery: corporate_data.*]
```

#### デプロイ情報
- **サービス名**: `gcs-to-bq`
- **URL**: https://gcs-to-bq-102847004309.asia-northeast1.run.app
- **リージョン**: asia-northeast1
- **メモリ**: 1Gi
- **タイムアウト**: 600秒

#### 削除したリソース
- ❌ Cloud Function `drive_to_gcs` を削除

#### エンドツーエンドテスト結果
```
✅ Drive → GCS: 7ファイル同期成功
✅ Excel → CSV変換: 7ファイル変換成功
✅ CSV → BigQuery: 7テーブルロード成功
✅ テーブル説明: 全テーブル設定完了
✅ カラム説明: 全140カラム設定完了
```

**テスト日時**: 2025-10-27 16:27 JST
**対象データ**: 202509

---

## 2025-10-27 (Part 1)

### 📊 経営資料ダッシュボード用SQL作成
- **ファイル**: `sql/dashboard_management_report.sql`
- **目的**: Looker Studioで月次損益計算書を可視化
- **機能**:
  - 組織階層別の集計（東京支店計 → 工事営業部計/硝子建材営業部 → 担当者別/部門別）
  - 損益計算書の全指標（売上高、粗利、営業利益、経常利益など）
  - 期間比較（前年実績、本年目標、本年実績）
  - 単位変換（DBは円単位、表示は千円単位）

### 🏷️ BigQueryテーブル・カラム説明の自動設定機能
- **ファイル**: `load_to_bigquery.py`
- **実装内容**:
  1. テーブル説明の自動設定
     - `mapping/excel_mapping.csv`から日本語テーブル名を読み込み
     - BigQueryのテーブルdescriptionに設定
  2. カラム説明の自動設定
     - `columns/` 配下のCSVファイルからカラム説明を読み込み
     - BigQueryの各カラムdescriptionに設定
  3. データロード後に自動実行

### 📝 新規追加関数
- `load_table_name_mapping()`: テーブル名マッピング読み込み
- `load_column_descriptions()`: カラム説明読み込み
- `update_table_and_column_descriptions()`: BigQueryのメタデータ更新

### ✅ 実行結果（202509データ）
```
✅ sales_target_and_achievements: 395行
   テーブル説明: 1_全支店[1.売上管理] 担当者売上目標／実績データ
   カラム説明: 19個設定

✅ billing_balance: 1,652行
   テーブル説明: 3_請求残高一覧表（月間）
   カラム説明: 6個設定

✅ ledger_income: 105行
   テーブル説明: 4_元帳_雑収入
   カラム説明: 31個設定

✅ department_summary: 113行
   テーブル説明: 6_部門集計表_202509
   カラム説明: 29個設定

✅ internal_interest: 41行
   テーブル説明: 7_社内金利計算表
   カラム説明: 7個設定

✅ profit_plan_term: 138行
   テーブル説明: 12_損益5期目標
   カラム説明: 17個設定

✅ ledger_loss: 10行
   テーブル説明: 16_元帳_雑損失
   カラム説明: 31個設定
```

### 📚 ドキュメント更新
- `README.md`: BigQueryテーブル一覧を詳細化
- `sql/README.md`: ダッシュボードSQL使用方法を追加

---

## 2025-10-24

### ✅ 初期実装完了
- Google Drive → GCS → BigQuery パイプライン構築
- 7テーブルのデータ連携成功
- 詳細は `README.md` 参照
