# raw/ → proceed/ 変換処理実装提案

## 現状分析

### 1. ファイル構成
- **raw/**: Excelファイル(.xlsx)形式でGoogle Driveからダウンロード済み
- **columns/**: 各テーブルのカラムマッピング定義（日本語→英語、データ型）
- **proceed/**: CSVファイル形式でBigQuery連携用データを配置（未実装）

### 2. カラムマッピング設定
各テーブルごとにcolumns/配下にCSVファイルで定義:
- jp_name: 元のExcelファイルの日本語カラム名
- en_name: BigQueryテーブルの英語カラム名
- type: BigQueryのデータ型（DATE, INT64, STRING, NUMERIC, DATETIME）
- description: カラムの説明

### 3. 確認したデータ構造例（sales_target_and_achievements）
- Excelファイル: 19カラム、395行
- 日付形式: '2025/09'形式（DATE型への変換が必要）
- 数値データ: 整数値（INT64）
- 文字列データ: 日本語テキスト

## 実装要件

### 必須要件
1. **Excel → CSV変換**: .xlsxファイルを.csv形式に変換
2. **カラム名マッピング**: 日本語カラム名を英語カラム名に変換
3. **データ型変換**: BigQueryの型に合わせた変換
   - DATE型: '2025/09' → '2025-09-01'形式
   - DATETIME型: 適切なフォーマットに変換
   - NUMERIC型: 小数点を含む数値として処理
   - INT64型: 整数値として処理
   - STRING型: 文字列として保持

### 追加考慮事項
1. **文字コード**: UTF-8で統一
2. **NULL値処理**: 空文字や0の適切な処理
3. **エラーハンドリング**: カラム不一致時の警告
4. **バッチ処理**: 複数ファイルの一括処理

## 実装方針

### アーキテクチャ
```
raw/{yyyymm}/{table}.xlsx
    ↓
[変換処理スクリプト]
  1. Excelファイル読み込み
  2. カラムマッピング適用
  3. データ型変換
  4. CSV出力
    ↓
proceed/{yyyymm}/{table}.csv
```

### 処理フロー
1. **設定読み込み**
   - columns/から各テーブルのマッピング定義を読み込み
   - マッピング辞書を作成

2. **Excel読み込み**
   - pandas.read_excel()でデータフレーム化
   - シート名指定が必要な場合は対応

3. **カラム変換**
   - 日本語カラム名を英語に変換
   - 存在しないカラムは警告出力

4. **データ型変換**
   - DATE: pd.to_datetime() → strftime('%Y-%m-%d')
   - DATETIME: pd.to_datetime() → strftime('%Y-%m-%d %H:%M:%S')
   - NUMERIC: pd.to_numeric()
   - INT64: astype('Int64') ※NULL対応
   - STRING: astype('str')、'nan'は空文字に

5. **CSV出力**
   - pandas.to_csv()でUTF-8エンコーディング
   - index=Falseで出力

### 実装コンポーネント

#### 1. transform_raw_to_proceed.py
メインの変換処理スクリプト
- GCSからraw/データを読み込み
- 変換処理を実行
- proceed/へCSV出力

#### 2. column_mapper.py
カラムマッピング処理モジュール
- columns/のCSVを読み込み
- マッピング辞書生成
- データ型変換関数

#### 3. 設定ファイル更新
- columns/にGCSパスを追加（ローカルとGCS両対応）
- バケット名は固定値: data-platform-landing-prod

## 実装優先順位

1. **Phase 1: 基本機能**
   - ローカルでのExcel→CSV変換
   - カラム名マッピング
   - 基本的なデータ型変換

2. **Phase 2: GCS連携**
   - GCSからの読み込み
   - GCSへの書き込み
   - Cloud Run対応

3. **Phase 3: エラー処理強化**
   - データ検証
   - エラーログ
   - リトライ処理

## テスト計画

1. **単体テスト**
   - 各テーブルの変換処理
   - データ型変換の確認
   - NULL値処理

2. **統合テスト**
   - 全7テーブルの一括処理
   - GCS連携確認

3. **BigQuery連携テスト**
   - CSVファイルのインポート確認
   - データ型の整合性確認