#!/usr/bin/env python3
"""
Google DriveからGCS raw/にファイルをコピー
"""

import os
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage
import io

# 定数
DRIVE_FOLDER_ID = "1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6"  # 過去データ取り込みフォルダ
GCS_BUCKET = "data-platform-landing-prod"
MONTHS = ["202409", "202410", "202411", "202412", "202501", "202502", "202503", "202504", "202505", "202506", "202507", "202508", "202509", "202510", "202511"]

def get_drive_service():
    """Google Drive APIクライアントを取得"""
    # デフォルトの認証情報を使用
    from google.auth import default
    credentials, project = default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
    return build('drive', 'v3', credentials=credentials)

def get_gcs_client():
    """GCSクライアントを取得"""
    return storage.Client()

def list_folders_in_drive(service, parent_folder_id):
    """Driveフォルダ内のサブフォルダ一覧を取得（共有ドライブ対応）"""
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=100,
        supportsAllDrives=True,  # 共有ドライブ対応
        includeItemsFromAllDrives=True,  # 共有ドライブのアイテムを含める
        corpora='allDrives'  # すべてのドライブを検索対象にする
    ).execute()

    folders = results.get('files', [])
    return {folder['name']: folder['id'] for folder in folders}

def list_files_in_folder(service, folder_id):
    """フォルダ内のファイル一覧を取得（共有ドライブ対応）"""
    query = f"'{folder_id}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)",
        pageSize=100,
        supportsAllDrives=True,  # 共有ドライブ対応
        includeItemsFromAllDrives=True,  # 共有ドライブのアイテムを含める
        corpora='allDrives'  # すべてのドライブを検索対象にする
    ).execute()

    return results.get('files', [])

def download_file_from_drive(service, file_id, file_name):
    """DriveからファイルをダウンロードしてGCSにアップロード（共有ドライブ対応）"""
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    # メモリ上にダウンロード
    file_data = io.BytesIO()
    downloader = MediaIoBaseDownload(file_data, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"   ダウンロード進捗: {int(status.progress() * 100)}%")

    file_data.seek(0)
    return file_data

def upload_to_gcs(gcs_client, bucket_name, blob_name, file_data):
    """GCSにファイルをアップロード"""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    file_data.seek(0)
    blob.upload_from_file(file_data)

    print(f"   ✅ アップロード完了: gs://{bucket_name}/{blob_name}")

def list_all_items_in_folder(service, folder_id):
    """フォルダ内のすべてのアイテム（フォルダ含む）を取得（共有ドライブ対応）"""
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)",
        pageSize=100,
        supportsAllDrives=True,  # 共有ドライブ対応
        includeItemsFromAllDrives=True,  # 共有ドライブのアイテムを含める
        corpora='allDrives'  # すべてのドライブを検索対象にする
    ).execute()
    return results.get('files', [])

def main():
    """メイン処理"""
    print("=" * 80)
    print("Google Drive → GCS コピー処理")
    print(f"Drive Folder ID: {DRIVE_FOLDER_ID}")
    print(f"GCS Bucket: {GCS_BUCKET}")
    print("=" * 80)

    # クライアント初期化
    print("\n初期化中...")
    drive_service = get_drive_service()
    gcs_client = get_gcs_client()

    # まず、指定フォルダ内のすべてのアイテムを表示（デバッグ用）
    print(f"\n指定フォルダ内のアイテム一覧:")
    all_items = list_all_items_in_folder(drive_service, DRIVE_FOLDER_ID)
    for item in all_items:
        item_type = "📁 フォルダ" if item['mimeType'] == 'application/vnd.google-apps.folder' else "📄 ファイル"
        print(f"  {item_type}: {item['name']}")
    print("")

    # Driveフォルダ一覧を取得
    print(f"Driveフォルダ一覧を取得中...")
    folders = list_folders_in_drive(drive_service, DRIVE_FOLDER_ID)

    print(f"見つかったフォルダ: {list(folders.keys())}")

    total_files = 0
    success_files = 0
    error_files = 0

    # 各月のフォルダを処理
    for yyyymm in MONTHS:
        print(f"\n{'='*80}")
        print(f"処理月: {yyyymm}")
        print(f"{'='*80}")

        if yyyymm not in folders:
            print(f"⚠️  Driveにフォルダが見つかりません: {yyyymm}")
            continue

        folder_id = folders[yyyymm]

        # フォルダ内のファイル一覧を取得
        files = list_files_in_folder(drive_service, folder_id)

        if not files:
            print(f"⚠️  フォルダ内にファイルがありません: {yyyymm}")
            continue

        print(f"ファイル数: {len(files)}")

        for file_info in files:
            file_id = file_info['id']
            file_name = file_info['name']

            print(f"\n📄 {file_name}")
            total_files += 1

            try:
                # Driveからダウンロード
                print(f"   Driveからダウンロード中...")
                file_data = download_file_from_drive(drive_service, file_id, file_name)

                # GCSにアップロード
                blob_name = f"google-drive/raw/{yyyymm}/{file_name}"
                print(f"   GCSにアップロード中...")
                upload_to_gcs(gcs_client, GCS_BUCKET, blob_name, file_data)

                success_files += 1

            except Exception as e:
                print(f"   ❌ エラー: {e}")
                error_files += 1
                import traceback
                traceback.print_exc()

    # 結果サマリー
    print("\n" + "=" * 80)
    print("コピー処理完了")
    print("=" * 80)
    print(f"総ファイル数: {total_files}")
    print(f"成功: {success_files}")
    print(f"失敗: {error_files}")
    print("=" * 80)

if __name__ == "__main__":
    main()
