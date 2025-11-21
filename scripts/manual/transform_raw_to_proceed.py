#!/usr/bin/env python3
"""
raw/ → proceed/ 変換処理スクリプト
Excel(.xlsx)ファイルをCSVに変換し、カラム名をマッピングして
BigQuery連携用のデータに整形する
"""

import os
import io
import re
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, Any
from google.cloud import storage
from pathlib import Path

# 固定値設定
PROJECT_ID = "data-platform-prod-475201"
LANDING_BUCKET = "data-platform-landing-prod"
COLUMNS_PATH = "config/columns"  # ローカルのカラム定義ファイルパス
MONETARY_SCALE_FILE = "config/mapping/monetary_scale_conversion.csv"  # 金額変換設定ファイル
FILE_NAME_MAPPING_FILE = "config/mapping/mapping_files.csv"  # ファイル名マッピングファイル

def load_column_mapping(table_name: str) -> Dict[str, Dict[str, str]]:
    """
    カラムマッピング定義を読み込み

    Args:
        table_name: テーブル名（例: sales_target_and_achievements）

    Returns:
        {日本語カラム名: {"en_name": 英語名, "type": データ型}}
    """
    mapping_file = f"{COLUMNS_PATH}/{table_name}.csv"
    if not os.path.exists(mapping_file):
        print(f"⚠️  マッピングファイルが見つかりません: {mapping_file}")
        return {}

    df = pd.read_csv(mapping_file)
    mapping = {}
    for _, row in df.iterrows():
        mapping[row['jp_name']] = {
            'en_name': row['en_name'],
            'type': row['type']
        }
    return mapping

def load_file_name_mapping() -> Dict[str, tuple]:
    """
    ファイル名マッピング定義を読み込み

    Returns:
        {英語テーブル名: (日本語ファイル名, シート名)}
    """
    if not os.path.exists(FILE_NAME_MAPPING_FILE):
        print(f"⚠️  ファイル名マッピングファイルが見つかりません: {FILE_NAME_MAPPING_FILE}")
        return {}

    df = pd.read_csv(FILE_NAME_MAPPING_FILE)
    mapping = {}
    for _, row in df.iterrows():
        en_name = row['en_name']
        jp_name = row['jp_name']
        sheet_name = row['sheet_name'] if pd.notna(row['sheet_name']) else None
        mapping[en_name] = (jp_name, sheet_name)
    return mapping

def convert_date_format(value: Any, date_type: str, column_name: str = '') -> str:
    """
    日付フォーマットの変換
    
    Args:
        value: 変換対象の値
        date_type: DATE or DATETIME
        column_name: カラム名（特殊処理用）
    
    Returns:
        変換後の日付文字列
    """
    if pd.isna(value) or value == '' or value is None:
        return ''
    
    # 数値の場合の処理
    if isinstance(value, (int, float)):
        # Excelのシリアル日付の場合（1900年1月1日からの日数）
        if value > 0 and value < 100000:
            try:
                # Excel日付の起点は1899-12-30
                excel_base = pd.Timestamp('1899-12-30')
                dt = excel_base + pd.Timedelta(days=int(value))
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass
        
        # Unixタイムスタンプ（ナノ秒）の場合
        elif value > 1e15:
            try:
                # ナノ秒をDatetimeに変換
                dt = pd.to_datetime(value, unit='ns')
                if date_type == 'DATETIME':
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except:
                pass
    
    # 文字列に変換
    value_str = str(value)
    
    # internal_interestの年月カラム特殊処理（例: "2025年9月" → "2025-09-01"）
    if column_name == '年月' and '年' in value_str and '月' in value_str:
        try:
            # "2025年9月" のような形式から年月を抽出
            match = re.match(r'(\d{4})年(\d{1,2})月', value_str)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                return f"{year}-{month}-01"
        except:
            pass
    
    # profit_plan_termの期間カラム特殊処理（同様）
    if column_name == '期間' and '年' in value_str and '月' in value_str:
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
        if re.match(r'^\d{4}/\d{1,2}$', value_str):
            try:
                dt = pd.to_datetime(value_str + '/01', format='%Y/%m/%d')
                return dt.strftime('%Y-%m-%d')
            except:
                pass
        
        # その他の日付形式
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

def apply_data_type_conversion(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """
    データ型変換を適用
    
    Args:
        df: 変換対象のDataFrame
        column_mapping: カラムマッピング定義
    
    Returns:
        型変換後のDataFrame
    """
    df = df.copy()
    
    for col in df.columns:
        if col not in column_mapping:
            continue
        
        data_type = column_mapping[col]['type']
        
        # DATE/DATETIME型
        if data_type in ['DATE', 'DATETIME']:
            # datetime64型の場合は直接変換
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if data_type == 'DATE':
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                else:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df[col] = df[col].apply(lambda x: convert_date_format(x, data_type, col))
        
        # INT64型
        elif data_type == 'INT64':
            # 空文字やNaNを扱えるようにnullable integerを使用
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 浮動小数点数を丸めてから整数に変換
            df[col] = df[col].round().astype('Int64')
        
        # NUMERIC型
        elif data_type == 'NUMERIC':
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # STRING型
        elif data_type == 'STRING':
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            # 'nan'文字列を空文字に置換
            df[col] = df[col].replace('nan', '')
    
    return df

def rename_columns(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """
    カラム名を日本語から英語に変換

    Args:
        df: 変換対象のDataFrame
        column_mapping: カラムマッピング定義

    Returns:
        カラム名変換後のDataFrame
    """
    rename_dict = {}

    for jp_col in df.columns:
        if jp_col in column_mapping:
            en_col = column_mapping[jp_col]['en_name']
            rename_dict[jp_col] = en_col
        else:
            print(f"⚠️  マッピング未定義のカラム: {jp_col}")
            # マッピングがない場合は元の名前を保持
            rename_dict[jp_col] = jp_col

    return df.rename(columns=rename_dict)

def load_monetary_scale_config() -> pd.DataFrame:
    """金額単位変換設定を読み込み"""
    if not os.path.exists(MONETARY_SCALE_FILE):
        print(f"⚠️  金額変換設定ファイルが見つかりません: {MONETARY_SCALE_FILE}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(MONETARY_SCALE_FILE)
        return df
    except Exception as e:
        print(f"⚠️  金額変換設定の読み込みエラー: {e}")
        return pd.DataFrame()

def apply_monetary_scale_conversion(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    金額単位変換を適用

    Args:
        df: 変換対象のDataFrame（英語カラム名に変換済み）
        table_name: テーブル名

    Returns:
        変換後のDataFrame
    """
    try:
        # 金額変換設定を読み込み
        config_df = load_monetary_scale_config()

        if config_df.empty:
            return df

        # 対象テーブルの設定を取得
        target_config = config_df[config_df['file_name'] == table_name]

        if target_config.empty:
            print(f"   金額変換設定なし: {table_name}")
            return df

        df = df.copy()

        for _, config in target_config.iterrows():
            condition_col = config['condition_column_name']
            condition_values = eval(config['condition_column_value'])  # リスト文字列を評価
            object_columns = eval(config['object_column_name'])  # リスト文字列を評価
            convert_value = float(config['convert_value'])

            # 条件に一致する行をフィルタ
            if condition_col not in df.columns:
                print(f"⚠️  条件カラムが存在しません: {condition_col}")
                continue

            mask = df[condition_col].isin(condition_values)

            # 対象カラムを変換
            for col in object_columns:
                if col in df.columns:
                    # 条件に一致する行のみ変換
                    df.loc[mask, col] = pd.to_numeric(df.loc[mask, col], errors='coerce') * convert_value
                    print(f"   💰 {col} を{convert_value}倍に変換（条件: {condition_col} in {condition_values}）")
                else:
                    print(f"⚠️  変換対象カラムが存在しません: {col}")

        return df

    except Exception as e:
        print(f"⚠️  金額変換エラー: {e}")
        import traceback
        traceback.print_exc()
        return df

def transform_excel_to_csv(
    input_path: str,
    output_path: str,
    table_name: str,
    sheet_name: Optional[str] = None
) -> bool:
    """
    Excelファイルを読み込んでCSVに変換
    
    Args:
        input_path: 入力Excelファイルパス
        output_path: 出力CSVファイルパス
        table_name: テーブル名
        sheet_name: シート名（省略時は最初のシート）
    
    Returns:
        成功時True
    """
    try:
        print(f"\n📄 処理中: {table_name}")
        print(f"   入力: {input_path}")
        
        # カラムマッピング読み込み
        column_mapping = load_column_mapping(table_name)
        if not column_mapping:
            print(f"❌ カラムマッピングが見つかりません: {table_name}")
            return False
        
        # Excelファイル読み込み
        if sheet_name:
            df = pd.read_excel(input_path, sheet_name=sheet_name)
        else:
            # sheet_nameを指定しない場合、最初のシートを読み込む
            df = pd.read_excel(input_path)
        
        # DataFrameが辞書として返される場合の処理
        if isinstance(df, dict):
            # 最初のシートを取得
            df = list(df.values())[0]
        
        # カラム名の改行を除去
        df.columns = [col.replace('\n', '') if isinstance(col, str) else col for col in df.columns]
        
        print(f"   データ: {len(df)}行 × {len(df.columns)}列")
        
        # 日本語カラム名を英語に変換（型変換前に実施）
        jp_column_mapping = {jp: info for jp, info in column_mapping.items()}
        
        # 日付列の事前処理（datetime64型の処理）
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

        # 金額単位変換（カラム名変換後に実行）
        df = apply_monetary_scale_conversion(df, table_name)

        # CSV出力
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"   出力: {output_path}")
        print(f"✅ 変換完了: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ 変換エラー ({table_name}): {e}")
        import traceback
        traceback.print_exc()
        return False

def process_gcs_files(yyyymm: str):
    """
    GCS上のraw/ファイルを変換してproceed/に保存

    Args:
        yyyymm: 対象年月（例: 202509）
    """
    print("=" * 60)
    print(f"raw/ → proceed/ 変換処理")
    print(f"対象年月: {yyyymm}")
    print(f"バケット: {LANDING_BUCKET}")
    print("=" * 60)

    # GCSクライアント初期化
    client = storage.Client()
    bucket = client.bucket(LANDING_BUCKET)

    # ファイル名マッピング読み込み
    file_name_mapping = load_file_name_mapping()

    # テーブルリスト（マッピングファイルから取得）
    tables = [
        "sales_target_and_achievements",
        "billing_balance",
        "ledger_income",
        "department_summary",
        "internal_interest",
        "profit_plan_term",
        "profit_plan_term_nagasaki",
        "profit_plan_term_fukuoka",
        "ledger_loss",
        "stocks",
        "ms_allocation_ratio",
        "customer_sales_target_and_achievements",
        "construction_progress_days_amount",
        "construction_progress_days_final_date",
    ]

    success_count = 0
    error_count = 0

    for table_name in tables:
        try:
            # ファイル名マッピングからシート名を取得
            sheet_name = None
            if table_name in file_name_mapping:
                _, sheet_name = file_name_mapping[table_name]

            # GCSパス（英語ファイル名を使用 - sync_drive_to_gcs.pyが英語スラグで保存するため）
            raw_path = f"raw/{yyyymm}/{table_name}.xlsx"
            proceed_path = f"proceed/{yyyymm}/{table_name}.csv"

            # rawファイルをダウンロード
            raw_blob = bucket.blob(raw_path)
            if not raw_blob.exists():
                print(f"⚠️  ファイルが存在しません: gs://{LANDING_BUCKET}/{raw_path}")
                error_count += 1
                continue

            # 一時ファイルにダウンロード
            temp_excel = f"/tmp/{table_name}.xlsx"
            temp_csv = f"/tmp/{table_name}.csv"

            raw_blob.download_to_filename(temp_excel)

            # 変換処理
            if transform_excel_to_csv(temp_excel, temp_csv, table_name, sheet_name):
                # proceedにアップロード
                proceed_blob = bucket.blob(proceed_path)
                proceed_blob.upload_from_filename(temp_csv)
                print(f"   → gs://{LANDING_BUCKET}/{proceed_path}")
                success_count += 1

                # 一時ファイル削除
                os.remove(temp_excel)
                os.remove(temp_csv)
            else:
                error_count += 1

        except Exception as e:
            print(f"❌ 処理エラー ({table_name}): {e}")
            error_count += 1

    print("=" * 60)
    print(f"処理完了: 成功 {success_count} / エラー {error_count}")
    print("=" * 60)

def generate_month_range(start_yyyymm: str, end_yyyymm: str):
    """
    開始月から終了月までの年月リストを生成

    Args:
        start_yyyymm: 開始年月 (例: '202409')
        end_yyyymm: 終了年月 (例: '202509')

    Returns:
        年月リスト ['202409', '202410', ..., '202509']
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_yyyymm, '%Y%m')
    end = datetime.strptime(end_yyyymm, '%Y%m')

    months = []
    current = start
    while current <= end:
        months.append(current.strftime('%Y%m'))
        current += relativedelta(months=1)

    return months

def process_multiple_months(start_yyyymm: str, end_yyyymm: str):
    """
    複数月のraw → proceed変換を一括実行

    Args:
        start_yyyymm: 開始年月
        end_yyyymm: 終了年月

    Returns:
        処理結果 {"success": 成功数, "error": エラー数}
    """
    months = generate_month_range(start_yyyymm, end_yyyymm)

    print("=" * 80)
    print(f"複数月一括変換処理")
    print(f"対象期間: {start_yyyymm} ～ {end_yyyymm} ({len(months)}ヶ月)")
    print("=" * 80)

    total_success = 0
    total_error = 0

    for yyyymm in months:
        print(f"\n{'='*80}")
        print(f"📅 処理月: {yyyymm}")
        print(f"{'='*80}")

        try:
            process_gcs_files(yyyymm)
            total_success += 1
        except Exception as e:
            print(f"❌ {yyyymm} の処理中にエラー: {e}")
            import traceback
            traceback.print_exc()
            total_error += 1

    print("\n" + "=" * 80)
    print(f"全体結果: 成功 {total_success}ヶ月 / エラー {total_error}ヶ月")
    print("=" * 80)

    return {"success": total_success, "error": total_error}

def process_local_files(yyyymm: str):
    """
    ローカルテスト用: ローカルファイルを変換

    Args:
        yyyymm: 対象年月
    """
    print("=" * 60)
    print(f"ローカル変換テスト")
    print(f"対象年月: {yyyymm}")
    print("=" * 60)

    # テスト用にサンプルファイルを処理
    if os.path.exists("/tmp/sample.xlsx"):
        output_path = "/tmp/sample_proceed.csv"
        if transform_excel_to_csv(
            "/tmp/sample.xlsx",
            output_path,
            "sales_target_and_achievements"
        ):
            # 結果確認
            df = pd.read_csv(output_path)
            print("\n変換後のデータ確認:")
            print(f"カラム: {list(df.columns)[:5]}...")
            print(f"データ型: {df.dtypes.head()}")
            print(f"最初の行: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3 and sys.argv[1] != "--local":
        # 複数月モード: python transform_raw_to_proceed.py 202409 202509
        start_yyyymm = sys.argv[1]
        end_yyyymm = sys.argv[2]
        process_multiple_months(start_yyyymm, end_yyyymm)
    elif len(sys.argv) >= 2 and sys.argv[1] != "--local":
        # 単月モード: python transform_raw_to_proceed.py 202509
        yyyymm = sys.argv[1]
        process_gcs_files(yyyymm)
    elif len(sys.argv) > 2 and sys.argv[2] == "--local":
        # ローカルテストモード
        yyyymm = sys.argv[1] if len(sys.argv) > 1 else "202509"
        process_local_files(yyyymm)
    else:
        print("使用方法:")
        print("  単月処理: python transform_raw_to_proceed.py 202509")
        print("  複数月処理: python transform_raw_to_proceed.py 202409 202509")
        print("  ローカルテスト: python transform_raw_to_proceed.py 202509 --local")