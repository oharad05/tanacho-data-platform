# データ重複問題の再発防止策

## 発生した問題の要約
- ETL処理がAPPENDモードで繰り返し実行され、データが重複蓄積
- profit_plan_term: 1,300,728行（正: 72行、約6,022倍）
- ledger_income: 621,201行（正: 21行、約1,409～8,453倍）
- billing_balance: 2,474,104行（正: 340行、約67～244倍）
- ledger_loss: 76,380行（正: 2行、約38,190倍）

---

## 再発防止策（優先度順）

### 【必須】1. デフォルトをREPLACEモードに変更

#### 対応内容
`load_to_bigquery.py`のデフォルト動作を変更

**修正前**:
```python
def process_all_tables(yyyymm: str, replace_existing: bool = False):
```

**修正後**:
```python
def process_all_tables(yyyymm: str, replace_existing: bool = True):
```

**影響範囲**:
- 全テーブルのロード処理
- 既存データは自動削除されてから追加される

**メリット**:
- デフォルトで重複が発生しない
- 明示的に`--no-replace`を指定しない限り安全

---

### 【必須】2. パーティション削除の必須化

#### 対応内容
`delete_partition_data()`を常に実行するように変更

**修正前**:
```python
if replace_existing:
    delete_partition_data(client, table_name, yyyymm)
```

**修正後**:
```python
# 常にパーティション削除を実行（冪等性の確保）
delete_partition_data(client, table_name, yyyymm)
```

**メリット**:
- ETL処理が何度実行されても同じ結果になる（冪等性）
- `replace_existing`フラグの設定ミスによる影響を防ぐ

---

### 【必須】3. シート指定の明示化（既に対応済み）

#### 対応内容
複数シートを持つExcelファイルは、取得対象シートを明示

**gcs_to_bq_service/main.py（318-320行目）**:
```python
# profit_plan_termの場合は「東京支店目標103期」シートのみを読み込む
if table_name == "profit_plan_term":
    df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='東京支店目標103期')
    print(f"   シート指定: 東京支店目標103期")
else:
    df = pd.read_excel(io.BytesIO(excel_bytes))
```

**拡張提案**:
設定ファイルでシート名を管理

```python
# config/sheet_names.yaml
profit_plan_term:
  target_sheet: "東京支店目標103期"
  description: "東京支店のみを対象"

# もしくは config/sheet_names.json
{
  "profit_plan_term": {
    "target_sheet": "東京支店目標103期",
    "description": "東京支店のみを対象"
  }
}
```

---

### 【推奨】4. データ件数の検証機能追加

#### 対応内容
ロード前後でデータ件数を検証し、異常値を検知

**新規ファイル: validate_data_counts.py**:
```python
#!/usr/bin/env python3
"""
データ件数検証スクリプト
"""
from google.cloud import bigquery, storage
import io
import pandas as pd

# 期待値の定義（テーブルごとの正常範囲）
EXPECTED_COUNTS = {
    "profit_plan_term": {"min": 50, "max": 100},
    "ledger_income": {"min": 10, "max": 100},
    "billing_balance": {"min": 200, "max": 500},
    "ledger_loss": {"min": 1, "max": 50},
    "sales_target_and_achievements": {"min": 100, "max": 10000},
    "department_summary": {"min": 50, "max": 1000},
    "internal_interest": {"min": 10, "max": 100},
}

def validate_load(table_name, yyyymm, loaded_rows):
    """ロードされた行数を検証"""
    expected = EXPECTED_COUNTS.get(table_name, {})
    min_rows = expected.get("min", 0)
    max_rows = expected.get("max", float('inf'))

    if loaded_rows < min_rows:
        print(f"⚠️  WARNING: {table_name} のロード行数が少なすぎます: {loaded_rows}行 (期待: {min_rows}～{max_rows}行)")
        return False
    elif loaded_rows > max_rows:
        print(f"❌ ERROR: {table_name} のロード行数が多すぎます: {loaded_rows}行 (期待: {min_rows}～{max_rows}行)")
        return False
    else:
        print(f"✅ {table_name}: {loaded_rows}行 (正常範囲)")
        return True

def validate_total_rows(client, table_name, yyyymm):
    """BigQueryテーブルの総行数を検証"""
    table_id = f"data-platform-prod-475201.corporate_data.{table_name}"

    try:
        table = client.get_table(table_id)
        total_rows = table.num_rows

        # 月次データの場合、期待値を調整
        expected = EXPECTED_COUNTS.get(table_name, {})
        max_total = expected.get("max", 1000) * 50  # 最大50ヶ月分を許容

        if total_rows > max_total:
            print(f"❌ ERROR: {table_name} の総行数が異常に多いです: {total_rows:,}行")
            return False
        else:
            print(f"✅ {table_name} 総行数: {total_rows:,}行")
            return True
    except Exception as e:
        print(f"⚠️  {table_name} の検証に失敗: {e}")
        return True  # 検証失敗でもロード処理は続行
```

**load_to_bigquery.pyに統合**:
```python
# ロード後に検証
if load_csv_to_bigquery(client, table_name, gcs_uri, yyyymm):
    # データ件数検証
    if not validate_load(table_name, yyyymm, loaded_rows):
        print(f"⚠️  {table_name} のデータ件数が異常です。確認してください。")

    if not validate_total_rows(client, table_name, yyyymm):
        print(f"❌ {table_name} の総行数が異常です。処理を中断します。")
        sys.exit(1)
```

---

### 【推奨】5. 定期監視とアラート設定

#### 対応内容
BigQueryのモニタリングクエリをスケジュール実行

**監視クエリ例**:
```sql
-- テーブル行数監視（毎日実行）
SELECT
  table_name,
  row_count,
  CASE
    WHEN table_name = 'profit_plan_term' AND row_count > 500 THEN 'ALERT'
    WHEN table_name = 'ledger_income' AND row_count > 10000 THEN 'ALERT'
    WHEN table_name = 'billing_balance' AND row_count > 50000 THEN 'ALERT'
    WHEN table_name = 'ledger_loss' AND row_count > 5000 THEN 'ALERT'
    ELSE 'OK'
  END as status
FROM `data-platform-prod-475201.corporate_data.__TABLES__`
WHERE table_id IN ('profit_plan_term', 'ledger_income', 'billing_balance', 'ledger_loss')
```

**BigQuery Scheduled Queriesで設定**:
```bash
bq mk \
  --transfer_config \
  --project_id=data-platform-prod-475201 \
  --data_source=scheduled_query \
  --schedule='every day 09:00' \
  --display_name='Data Quality Monitor' \
  --target_dataset=monitoring \
  --params='{"query":"SELECT ...","destination_table_name_template":"data_quality_check"}'
```

**アラート設定（Cloud Monitoring）**:
- テーブル行数が閾値を超えた場合にSlack/メール通知
- ETL実行失敗時の通知
- パーティション数の異常増加検知

---

### 【推奨】6. ユニークキー制約の追加（該当テーブルのみ）

#### 対応内容
重複を許さないテーブルには、アプリケーションレベルでユニーク制約を実装

**例: billing_balanceテーブル**:
```python
def deduplicate_before_load(df, unique_keys):
    """ロード前に重複除去"""
    before_count = len(df)
    df = df.drop_duplicates(subset=unique_keys, keep='last')
    after_count = len(df)

    if before_count > after_count:
        print(f"⚠️  重複除去: {before_count - after_count}行を削除")

    return df

# billing_balanceの場合
unique_keys = ['sales_month', 'branch_code', 'customer_code']
df = deduplicate_before_load(df, unique_keys)
```

---

### 【推奨】7. ETL実行ログの記録

#### 対応内容
ETL実行履歴をBigQueryに記録

**新規テーブル: etl_execution_log**:
```sql
CREATE TABLE `data-platform-prod-475201.monitoring.etl_execution_log`
(
  execution_id STRING,
  table_name STRING,
  yyyymm STRING,
  execution_time TIMESTAMP,
  status STRING,  -- 'SUCCESS', 'FAILED', 'PARTIAL'
  rows_loaded INT64,
  rows_deleted INT64,
  total_rows_after INT64,
  error_message STRING,
  execution_duration_seconds FLOAT64
)
PARTITION BY DATE(execution_time)
CLUSTER BY table_name, yyyymm;
```

**load_to_bigquery.pyに統合**:
```python
import uuid
import time

def log_etl_execution(client, table_name, yyyymm, status, rows_loaded, error=None):
    """ETL実行ログを記録"""
    log_table = "data-platform-prod-475201.monitoring.etl_execution_log"

    row = {
        "execution_id": str(uuid.uuid4()),
        "table_name": table_name,
        "yyyymm": yyyymm,
        "execution_time": time.time(),
        "status": status,
        "rows_loaded": rows_loaded,
        "error_message": str(error) if error else None,
    }

    errors = client.insert_rows_json(log_table, [row])
    if errors:
        print(f"⚠️  ログ記録失敗: {errors}")
```

---

### 【参考】8. MERGE（UPSERT）パターンへの移行（長期的対応）

#### 対応内容
TRUNCATE & LOADパターンからMERGEパターンへ移行

**現在のパターン（TRUNCATE & LOAD）**:
```python
# 既存データを削除
DELETE FROM table WHERE partition = '2025-09-01';
# 新データを追加
INSERT INTO table SELECT * FROM new_data;
```

**推奨パターン（MERGE）**:
```sql
MERGE `data-platform-prod-475201.corporate_data.billing_balance` AS target
USING (
  SELECT * FROM `temp_table`
) AS source
ON target.sales_month = source.sales_month
   AND target.branch_code = source.branch_code
   AND target.customer_code = source.customer_code
WHEN MATCHED THEN
  UPDATE SET
    target.amount = source.amount,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (sales_month, branch_code, customer_code, amount, created_at)
  VALUES (source.sales_month, source.branch_code, source.customer_code, source.amount, CURRENT_TIMESTAMP())
```

**メリット**:
- 真の冪等性を実現
- 重複が発生しない
- データの更新履歴を管理可能

---

## 実装優先順位と工数見積もり

| 優先度 | 対策 | 工数 | 期限目安 |
|--------|------|------|----------|
| 🔴 必須 | 1. デフォルトをREPLACEモードに変更 | 0.5h | 即時 |
| 🔴 必須 | 2. パーティション削除の必須化 | 0.5h | 即時 |
| 🟢 完了 | 3. シート指定の明示化 | - | 完了済み |
| 🟡 推奨 | 4. データ件数の検証機能追加 | 2h | 1週間以内 |
| 🟡 推奨 | 5. 定期監視とアラート設定 | 3h | 2週間以内 |
| 🟡 推奨 | 6. ユニークキー制約の追加 | 2h | 2週間以内 |
| 🟡 推奨 | 7. ETL実行ログの記録 | 3h | 1ヶ月以内 |
| ⚪ 参考 | 8. MERGEパターンへの移行 | 10h | 3ヶ月以内 |

---

## まとめ

### 即座に実施すべき対策（今週中）
1. ✅ **デフォルトをREPLACEモードに変更**
2. ✅ **パーティション削除の必須化**

### 短期的に実施すべき対策（1～2週間）
3. データ件数の検証機能追加
4. 定期監視とアラート設定

### 中長期的に実施すべき対策（1～3ヶ月）
5. ETL実行ログの記録
6. MERGEパターンへの移行

これらの対策により、今後同様のデータ重複問題が発生するリスクを大幅に低減できます。
