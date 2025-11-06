#!/usr/bin/env python3
"""
BigQuery接続テストスクリプト
"""

from google.cloud import bigquery

PROJECT_ID = "data-platform-prod-475201"
DATASET_ID = "corporate_data"

def test_connection():
    """BigQuery接続テスト"""
    print("=" * 60)
    print("BigQuery 接続テスト")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print("=" * 60)
    
    try:
        # クライアント作成
        client = bigquery.Client(project=PROJECT_ID)
        print("✅ BigQueryクライアント作成成功")
        
        # データセットの存在確認
        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
        dataset = client.get_dataset(dataset_id)
        print(f"✅ データセット確認: {dataset.dataset_id}")
        
        # テーブル一覧取得
        tables = list(client.list_tables(dataset))
        print(f"\n📊 テーブル一覧 ({len(tables)} テーブル):")
        for table in tables:
            full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}"
            table_obj = client.get_table(full_table_id)
            print(f"   - {table.table_id}: {table_obj.num_rows:,} 行")
        
        print("\n✅ 接続テスト成功")
        return True
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        print("\n対処法:")
        print("1. gcloud auth application-default login を実行")
        print("2. プロジェクトへのアクセス権限を確認")
        print("3. BigQuery APIが有効になっているか確認")
        return False

if __name__ == "__main__":
    test_connection()