#!/usr/bin/env python3
"""
profit_plan_term専用のExcel→CSV変換スクリプト
"""
import os, io
import pandas as pd
import numpy as np
from google.cloud import storage, bigquery

PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"
LANDING_BUCKET = "data-platform-landing-prod"
COLUMNS_PATH = "config/columns"

def load_column_mapping(storage_client, table_name):
    """カラムマッピング定義を読み込み"""
    bucket = storage_client.bucket(LANDING_BUCKET)
    mapping_blob = bucket.blob(f"{COLUMNS_PATH}/{table_name}.csv")

    if not mapping_blob.exists():
        print(f"❌ マッピングファイルが見つかりません: {table_name}.csv")
        return {}

    csv_data = mapping_blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(csv_data))

    mapping = {}
    for _, row in df.iterrows():
        mapping[row['jp_name']] = {
            'en_name': row['en_name'],
            'type': row['type']
        }
    return mapping

def convert_date_format(value, date_type, column_name=''):
    """日付フォーマットの変換"""
    if pd.isna(value) or value == '' or value is None:
        return ''

    # pandas Timestamp型の場合
    if isinstance(value, pd.Timestamp):
        if date_type == 'DATETIME':
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return value.strftime('%Y-%m-%d')

    # 数値の場合の処理
    if isinstance(value, (int, float, np.integer, np.floating)):
        # ナノ秒タイムスタンプ（1e15以上）
        if value >= 1e15:
            try:
                dt = pd.to_datetime(value, unit='ns')
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass

        # Excelのシリアル日付
        elif value > 0 and value < 100000:
            try:
                excel_base = pd.Timestamp('1899-12-30')
                dt = excel_base + pd.Timedelta(days=int(value))
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass

    # 文字列に変換
    value_str = str(value)

    # 「年月」特殊処理（例: "2025年9月" → "2025-09-01"）
    if '年' in value_str and '月' in value_str:
        import re
        try:
            match = re.match(r'(\d{4})年(\d{1,2})月', value_str)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                return f"{year}-{month}-01"
        except:
            pass

    # DATE型の処理
    if date_type == 'DATE':
        # YYYY/MM形式の場合、1日を追加
        import re
        if re.match(r'^\d{4}/\d{1,2}$', value_str):
            try:
                dt = pd.to_datetime(value_str + '/01', format='%Y/%m/%d')
                return dt.strftime('%Y-%m-%d')
            except:
                pass

        try:
            dt = pd.to_datetime(value_str)
            return dt.strftime('%Y-%m-%d')
        except:
            print(f"⚠️  日付変換エラー: {value_str}")
            return value_str

    # DATETIME型の処理
    elif date_type == 'DATETIME':
        try:
            dt = pd.to_datetime(value_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            print(f"⚠️  日時変換エラー: {value_str}")
            return value_str

    return value_str

def apply_data_type_conversion(df, column_mapping):
    """データ型変換を適用"""
    df = df.copy()

    for col in df.columns:
        if col not in column_mapping:
            continue

        data_type = column_mapping[col]['type']

        # DATE/DATETIME型
        if data_type in ['DATE', 'DATETIME']:
            # すべてのケースで文字列に変換
            df[col] = df[col].apply(lambda x: convert_date_format(x, data_type, col) if pd.notna(x) and x != '' else '')

        # INT64型
        elif data_type == 'INT64':
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype('Int64')

        # NUMERIC型
        elif data_type == 'NUMERIC':
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # STRING型
        elif data_type == 'STRING':
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', '')

    return df

def rename_columns(df, column_mapping):
    """カラム名を日本語から英語に変換"""
    rename_dict = {}

    for jp_col in df.columns:
        if jp_col in column_mapping:
            en_col = column_mapping[jp_col]['en_name']
            rename_dict[jp_col] = en_col
        else:
            print(f"⚠️  マッピング未定義のカラム: {jp_col}")
            rename_dict[jp_col] = jp_col

    return df.rename(columns=rename_dict)

def transform_excel_to_csv(storage_client, table_name, yyyymm):
    """Excelファイルを読み込んでCSVに変換"""
    try:
        print(f"\n📄 処理中: {table_name}")

        bucket = storage_client.bucket(LANDING_BUCKET)

        # raw/ から読み込み
        raw_path = f"raw/{yyyymm}/{table_name}.xlsx"
        raw_blob = bucket.blob(raw_path)

        if not raw_blob.exists():
            print(f"⚠️  ファイルが存在しません: gs://{LANDING_BUCKET}/{raw_path}")
            return False

        # カラムマッピング読み込み
        column_mapping = load_column_mapping(storage_client, table_name)
        if not column_mapping:
            print(f"❌ カラムマッピングが見つかりません: {table_name}")
            return False

        # Excelファイル読み込み
        excel_bytes = raw_blob.download_as_bytes()

        # profit_plan_termの場合は「東京支店目標103期」シートのみを読み込む
        if table_name == "profit_plan_term":
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='東京支店目標103期')
            print(f"   シート指定: 東京支店目標103期")
        else:
            df = pd.read_excel(io.BytesIO(excel_bytes))

        # カラム名の改行を除去
        df.columns = [col.replace('\n', '') if isinstance(col, str) else col for col in df.columns]

        print(f"   データ: {len(df)}行 × {len(df.columns)}列")

        # 日本語カラム名を英語に変換（型変換前）
        jp_column_mapping = {jp: info for jp, info in column_mapping.items()}

        # 日付列の事前処理
        for jp_col, info in jp_column_mapping.items():
            if jp_col in df.columns and info['type'] in ['DATE', 'DATETIME']:
                if pd.api.types.is_datetime64_any_dtype(df[jp_col]):
                    if info['type'] == 'DATE':
                        df[jp_col] = df[jp_col].dt.strftime('%Y-%m-%d')
                    else:
                        df[jp_col] = df[jp_col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # データ型変換
        df = apply_data_type_conversion(df, jp_column_mapping)

        # カラム名変換
        df = rename_columns(df, jp_column_mapping)

        # CSV出力前の最終確認：DATE/DATETIME列を文字列に変換
        for jp_col, info in column_mapping.items():
            en_col = info['en_name']
            if en_col in df.columns:
                data_type = info['type']
                if data_type == 'DATE':
                    # タイムスタンプまたは数値形式を文字列に変換
                    df[en_col] = df[en_col].apply(lambda x: convert_date_format(x, 'DATE') if pd.notna(x) and x != '' else '')
                elif data_type == 'DATETIME':
                    df[en_col] = df[en_col].apply(lambda x: convert_date_format(x, 'DATETIME') if pd.notna(x) and x != '' else '')

        # CSV出力
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        # proceed/ に保存
        proceed_path = f"proceed/{yyyymm}/{table_name}.csv"
        proceed_blob = bucket.blob(proceed_path)
        proceed_blob.upload_from_file(csv_buffer, content_type='text/csv')

        print(f"   出力: gs://{LANDING_BUCKET}/{proceed_path}")
        print(f"✅ 変換完了: {table_name}")

        return True

    except Exception as e:
        print(f"❌ 変換エラー ({table_name}): {e}")
        import traceback
        traceback.print_exc()
        return False

def load_csv_to_bigquery(bq_client, table_name, yyyymm):
    """CSVファイルをBigQueryにロード"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    gcs_uri = f"gs://{LANDING_BUCKET}/proceed/{yyyymm}/{table_name}.csv"

    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=False,
            max_bad_records=0,
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        print(f"   ⏳ ロード開始: {table_name} (Job ID: {load_job.job_id})")

        load_job.result(timeout=300)

        destination_table = bq_client.get_table(table_id)
        print(f"   ✅ ロード完了: {load_job.output_rows} 行を追加")
        print(f"      総レコード数: {destination_table.num_rows:,} 行")

        return True

    except Exception as e:
        print(f"   ❌ ロードエラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    yyyymm = "202509"
    table_name = "profit_plan_term"

    print("=" * 60)
    print(f"profit_plan_term ETL処理開始")
    print(f"対象年月: {yyyymm}")
    print("=" * 60)

    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID)

    # 1. Excel → CSV変換
    print("\n[1/2] Excel → CSV 変換中...")
    if not transform_excel_to_csv(storage_client, table_name, yyyymm):
        print("❌ 変換失敗")
        return False

    # 2. BigQueryロード
    print("\n[2/2] CSV → BigQuery ロード中...")
    if not load_csv_to_bigquery(bq_client, table_name, yyyymm):
        print("❌ ロード失敗")
        return False

    print("\n" + "=" * 60)
    print("✅ profit_plan_term ETL処理完了")
    print("=" * 60)

    return True

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
