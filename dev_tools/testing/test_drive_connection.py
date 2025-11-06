#!/usr/bin/env python3
"""
Google Drive共有フォルダの接続テストスクリプト
サービスアカウント: 102847004309-compute@developer.gserviceaccount.com
"""

import os
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth import default as google_auth_default

# ========== 設定 ==========
# 1. サービスアカウントの認証情報
SERVICE_ACCOUNT_EMAIL = "102847004309-compute@developer.gserviceaccount.com"
PROJECT_ID = "data-platform-prod-475201"

# 2. 固定値：共有ドライブのフォルダID
DRIVE_FOLDER_ID = "1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6"

# 3. 固定値：GCSバケット
LANDING_BUCKET = "data-platform-landing-prod"

# 4. テスト対象の年月
TARGET_YYYYMM = "202509"

# 5. スコープ
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def test_service_account_auth():
    """サービスアカウント認証のテスト"""
    print("=" * 60)
    print("1. サービスアカウント認証テスト")
    print("=" * 60)
    
    try:
        # ADC (Application Default Credentials) を使用
        creds, project = google_auth_default(scopes=SCOPES)
        print(f"✅ 認証成功")
        print(f"   プロジェクトID: {project}")
        
        # サービスアカウントのメールを確認
        if hasattr(creds, 'service_account_email'):
            print(f"   サービスアカウント: {creds.service_account_email}")
        
        return creds
    except Exception as e:
        print(f"❌ 認証失敗: {e}")
        print("\n対処法:")
        print("1. gcloud auth application-default login を実行")
        print("2. または GOOGLE_APPLICATION_CREDENTIALS 環境変数にキーファイルパスを設定")
        return None

def test_drive_api(creds):
    """Google Drive APIのテスト"""
    print("\n" + "=" * 60)
    print("2. Google Drive API接続テスト")
    print("=" * 60)
    
    try:
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        
        # About APIでアクセス確認
        about = drive.about().get(fields="user").execute()
        user = about.get("user", {})
        print(f"✅ Drive API接続成功")
        print(f"   ユーザー: {user.get('displayName', 'N/A')} ({user.get('emailAddress', 'N/A')})")
        
        return drive
    except HttpError as e:
        print(f"❌ Drive API接続失敗: {e}")
        print("\n対処法:")
        print("1. Google Cloud ConsoleでDrive APIが有効か確認")
        print("2. サービスアカウントに必要な権限があるか確認")
        return None

def test_folder_access(drive, folder_id):
    """フォルダアクセステスト"""
    print("\n" + "=" * 60)
    print(f"3. フォルダアクセステスト (ID: {folder_id})")
    print("=" * 60)
    
    
    try:
        # フォルダのメタデータ取得
        folder = drive.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,driveId,parents,capabilities",
            supportsAllDrives=True
        ).execute()
        
        print(f"✅ フォルダアクセス成功")
        print(f"   フォルダ名: {folder.get('name', 'N/A')}")
        print(f"   タイプ: {folder.get('mimeType', 'N/A')}")
        
        if folder.get('driveId'):
            print(f"   共有ドライブID: {folder.get('driveId')}")
        
        # 権限確認
        capabilities = folder.get('capabilities', {})
        can_read = capabilities.get('canReadDrive', capabilities.get('canRead', False))
        print(f"   読み取り権限: {'✅ あり' if can_read else '❌ なし'}")
        
        return folder
    except HttpError as e:
        print(f"❌ フォルダアクセス失敗: {e}")
        print("\n対処法:")
        print(f"1. Google Driveで共有フォルダに {SERVICE_ACCOUNT_EMAIL} を追加")
        print("2. 「閲覧者」以上の権限を付与")
        print("3. 共有ドライブの場合は、共有ドライブのメンバーとして追加")
        return None

def find_month_folder(drive, parent_id, yyyymm):
    """月フォルダの検索"""
    print("\n" + "=" * 60)
    print(f"4. 月フォルダ検索 ({yyyymm})")
    print("=" * 60)
    
    # まず親フォルダ直下を検索
    try:
        q = (f"'{parent_id}' in parents and "
             f"mimeType='application/vnd.google-apps.folder' and "
             f"name='{yyyymm}' and trashed=false")
        
        res = drive.files().list(
            q=q,
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        files = res.get("files", [])
        if files:
            print(f"✅ 月フォルダ発見: {files[0]['name']} (ID: {files[0]['id']})")
            return files[0]['id']
        
        # 見つからない場合、親フォルダ内の全フォルダを表示
        print(f"⚠️  '{yyyymm}' フォルダが見つかりません")
        print("\n親フォルダ内のフォルダ一覧:")
        
        q2 = (f"'{parent_id}' in parents and "
              f"mimeType='application/vnd.google-apps.folder' and "
              f"trashed=false")
        
        res2 = drive.files().list(
            q=q2,
            fields="files(id,name)",
            pageSize=20,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        for f in res2.get("files", []):
            print(f"   - {f['name']} (ID: {f['id']})")
        
        return None
        
    except HttpError as e:
        print(f"❌ フォルダ検索失敗: {e}")
        return None

def list_xlsx_files(drive, folder_id):
    """XLSXファイルの一覧表示"""
    print("\n" + "=" * 60)
    print("5. XLSXファイル一覧")
    print("=" * 60)
    
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        
        res = drive.files().list(
            q=q,
            fields="files(id,name,mimeType,size)",
            pageSize=100,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        xlsx_files = []
        for f in res.get("files", []):
            name = f.get("name", "")
            if name.lower().endswith(".xlsx") or f.get("mimeType") == "application/vnd.google-apps.spreadsheet":
                xlsx_files.append(f)
                size = f.get("size", "N/A")
                if size != "N/A":
                    size = f"{int(size) / 1024:.1f} KB"
                print(f"   📄 {name}")
                print(f"      ID: {f['id']}")
                print(f"      Type: {f.get('mimeType', 'N/A')}")
                if size != "N/A":
                    print(f"      Size: {size}")
        
        print(f"\n✅ 合計 {len(xlsx_files)} 個のExcelファイルを発見")
        return xlsx_files
        
    except HttpError as e:
        print(f"❌ ファイル一覧取得失敗: {e}")
        return []

def main():
    print("Google Drive接続テスト")
    print(f"サービスアカウント: {SERVICE_ACCOUNT_EMAIL}")
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"対象フォルダID: {DRIVE_FOLDER_ID}")
    print(f"GCSバケット: {LANDING_BUCKET}")
    print(f"対象年月: {TARGET_YYYYMM}")
    
    # 1. 認証
    creds = test_service_account_auth()
    if not creds:
        return 1
    
    # 2. Drive API接続
    drive = test_drive_api(creds)
    if not drive:
        return 1
    
    # 3. フォルダアクセス
    folder = test_folder_access(drive, DRIVE_FOLDER_ID)
    if not folder:
        return 1
    
    # 4. 月フォルダ検索
    month_folder_id = find_month_folder(drive, DRIVE_FOLDER_ID, TARGET_YYYYMM)
    if not month_folder_id:
        print("\n月フォルダが見つからないため、親フォルダ内のファイルを表示します")
        list_xlsx_files(drive, DRIVE_FOLDER_ID)
    else:
        # 5. XLSXファイル一覧
        list_xlsx_files(drive, month_folder_id)
    
    print("\n" + "=" * 60)
    print("テスト完了")
    print("=" * 60)
    
    print("\n📝 必要な設定:")
    print("1. Google Cloud Console で Drive API を有効化")
    print(f"2. Google Drive で {SERVICE_ACCOUNT_EMAIL} に共有フォルダへのアクセス権を付与")
    print("3. 環境変数 DRIVE_FOLDER_ID に正しいフォルダIDを設定")
    print("4. Cloud Run デプロイ時に同じ環境変数を設定")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())