#!/usr/bin/env python3
"""
Google Drive → GCS 連携実行スクリプト
固定値:
- DRIVE_FOLDER_ID: 1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6
- LANDING_BUCKET: data-platform-landing-prod
"""

import os, io, json, base64, datetime as dt, pandas as pd, re, traceback
from typing import Optional, Tuple
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.cloud import storage
from google.auth import default as google_auth_default

# ===== 固定値設定 =====
PROJECT_ID = "data-platform-prod-475201"
DRIVE_FOLDER_ID = "1bHmrsqE1jdUgWbiPFsiPTymR5IRND4t6"  # 02_データソース
LANDING_BUCKET = "data-platform-landing-prod"
MAPPING_GCS_PATH = "config/mapping_files.csv"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ===== Drive API =====
def _build_drive_service():
    creds, _ = google_auth_default(scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _find_month_subfolder(drive, parent_id: str, yyyymm: str) -> Optional[str]:
    """月フォルダを検索"""
    q = (f"'{parent_id}' in parents and "
         "mimeType='application/vnd.google-apps.folder' and "
         f"name='{yyyymm}' and trashed=false")
    try:
        res = drive.files().list(
            q=q, fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True, pageSize=50
        ).execute()
        files = res.get("files", [])
        if files:
            print(f"✅ 月フォルダ発見: {files[0]['name']} (ID: {files[0]['id']})")
            return files[0]["id"]
    except HttpError as e:
        print(f"❌ フォルダ検索失敗: {e}")
    
    print(f"⚠️  月フォルダ {yyyymm} が見つかりません")
    return None

def _iter_files(drive, folder_id):
    """フォルダ内のファイルを取得"""
    q = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    while True:
        res = drive.files().list(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,size)",
            pageToken=page_token,
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        for f in res.get("files", []):
            yield f
        page_token = res.get("nextPageToken")
        if not page_token:
            break

def _download_xlsx(drive, file_id, name_hint=None):
    """Drive上の .xlsx をダウンロード"""
    meta = drive.files().get(fileId=file_id, fields="name", supportsAllDrives=True).execute()
    name = meta.get("name", name_hint or "file")
    out_name = name if name.lower().endswith(".xlsx") else re.sub(r"\.[^.]+$", "", name) + ".xlsx"
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO(req.execute())
    buf.seek(0)
    return out_name, buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# ===== マッピング =====
def _load_mapping_csv():
    """GCSからマッピングCSVを読み込み"""
    client = storage.Client()
    blob = client.bucket(LANDING_BUCKET).blob(MAPPING_GCS_PATH)
    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
    cols = {c.strip().lower(): c for c in df.columns}
    jp_col = cols.get("jp_name") or cols.get("original_name")
    en_col = cols.get("en_name") or cols.get("english_slug")
    if not (jp_col and en_col):
        raise ValueError("mapping CSVに 'jp_name' と 'en_name' が必要です。")
    out = df[[jp_col, en_col]].copy()
    out.columns = ["jp_name", "en_name"]
    return out

def _slug_from_mapping(df_map: pd.DataFrame, original_name: str):
    """ファイル名からスラグを取得"""
    row = df_map.loc[df_map["jp_name"] == original_name]
    if row.empty:
        base = os.path.splitext(original_name)[0]
        safe = re.sub(r"[^0-9A-Za-z_]+", "_", base)
        safe = re.sub(r"_+", "_", safe).strip("_")
        print(f"  ⚠️  マッピング未定義: {original_name} → {safe.lower()}")
        return safe.lower()
    slug = str(row.iloc[0]["en_name"]).strip()
    return slug

# ===== GCSアップロード =====
def _gcs_upload_raw(bucket, bytes_io: io.BytesIO, yyyymm: str, slug: str, content_type: str) -> str:
    """GCSにファイルをアップロード"""
    path = f"raw/{yyyymm}/{slug}.xlsx"
    blob = bucket.blob(path)
    blob.content_type = content_type
    blob.upload_from_file(bytes_io)
    return f"gs://{LANDING_BUCKET}/{path}"

# ===== メイン処理 =====
def sync_drive_to_gcs(yyyymm: str):
    """
    Google DriveからGCSへファイルを同期
    """
    print("=" * 60)
    print(f"Google Drive → GCS 連携開始")
    print(f"対象年月: {yyyymm}")
    print(f"フォルダID: {DRIVE_FOLDER_ID}")
    print(f"GCSバケット: {LANDING_BUCKET}")
    print("=" * 60)
    
    # Drive API接続
    drive = _build_drive_service()
    
    # 月フォルダ検索
    month_id = _find_month_subfolder(drive, DRIVE_FOLDER_ID, yyyymm)
    if not month_id:
        return
    
    # マッピング読込
    try:
        df_map = _load_mapping_csv()
        print(f"✅ マッピングファイル読み込み成功 ({len(df_map)}件)")
    except Exception as e:
        print(f"❌ マッピングファイル読み込み失敗: {e}")
        df_map = pd.DataFrame(columns=["jp_name", "en_name"])
    
    # GCS準備
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)
    
    processed = 0
    skipped = 0
    
    print("\n📁 ファイル処理中...")
    print("-" * 40)
    
    for f in _iter_files(drive, month_id):
        name = f.get("name", "")
        lname = name.lower()
        
        if not lname.endswith(".xlsx"):
            skipped += 1
            continue
        
        try:
            # ダウンロード
            out_name, filebytes, ctype = _download_xlsx(drive, f["id"], name_hint=name)
            
            # マッピング解決
            slug = _slug_from_mapping(df_map, name)
            
            # GCSアップロード
            gcs_uri = _gcs_upload_raw(bucket, filebytes, yyyymm, slug, ctype)
            
            print(f"✅ {name}")
            print(f"   → {gcs_uri}")
            processed += 1
            
        except Exception as e:
            print(f"❌ {name}: {e}")
    
    print("-" * 40)
    print(f"\n📊 処理結果:")
    print(f"  処理済み: {processed} ファイル")
    print(f"  スキップ: {skipped} ファイル")
    print("=" * 60)
    
    return processed

if __name__ == "__main__":
    import sys
    yyyymm = sys.argv[1] if len(sys.argv) > 1 else "202509"
    sync_drive_to_gcs(yyyymm)