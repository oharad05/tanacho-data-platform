import os, io, json, base64, datetime as dt, pandas as pd, requests, re
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage

# === 環境変数 ===
PROJECT_ID         = os.environ.get("GCP_PROJECT")
DRIVE_FOLDER_ID    = os.environ["DRIVE_FOLDER_ID"]     # Drive親フォルダID
LANDING_BUCKET     = os.environ["LANDING_BUCKET"]      # GCSバケット名
MAPPING_GCS_PATH   = os.environ.get("MAPPING_GCS_PATH", "config/mapping_files.csv")
CLOUD_RUN_ENDPOINT = os.environ["CLOUD_RUN_ENDPOINT"]  # 例: https://<run-url>/ingest
SERVICE_JSON       = os.environ.get("SERVICE_JSON_PATH")  # 任意：サービスアカウント鍵パス
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ============== Drive API ==============
def _build_drive_service():
    creds = None
    if SERVICE_JSON:
        creds = service_account.Credentials.from_service_account_file(SERVICE_JSON, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _find_month_subfolder(drive, parent_id, yyyymm):
    q = (
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and "
        f"name='{yyyymm}' and trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def _iter_files(drive, folder_id):
    """月フォルダ配下の全ファイル（ページング対応）"""
    q = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    while True:
        res = drive.files().list(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,size)",
            pageToken=page_token,
        ).execute()
        for f in res.get("files", []):
            yield f
        page_token = res.get("nextPageToken")
        if not page_token:
            break

def _download_excel_as_xlsx(drive, file_id):
    """Google スプレッドシートは .xlsx でエクスポート、通常のExcelはそのままダウンロード"""
    meta = drive.files().get(fileId=file_id, fields="mimeType,name").execute()
    mime, name = meta["mimeType"], meta["name"]
    if mime == "application/vnd.google-apps.spreadsheet":
        req = drive.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        req = drive.files().get_media(fileId=file_id)
    b = io.BytesIO(req.execute())
    b.seek(0)
    # 末尾拡張子を .xlsx に正規化（.xlsでもOKですが後段を揃えるため.xlsxに寄せます）
    if not name.lower().endswith((".xlsx", ".xls")):
        name = re.sub(r"\.[^.]+$", "", name)  # 既存拡張子っぽいものを外す
        name = f"{name}.xlsx"
    return name, b

# ============== マッピング ==============
def _load_mapping_csv():
    """
    mapping CSV（GCS上）を読み込みます。
    想定カラム（ヘッダーは多少ゆるく対応）:
      - jp_name / original_name : Drive上のファイル名（完全一致を推奨）
      - en_name / english_slug  : BigQueryテーブル用の英語スラグ
      - sheet_name（任意）      : 取り込むシート名が決まっていれば指定
    """
    client = storage.Client()
    blob = client.bucket(LANDING_BUCKET).blob(MAPPING_GCS_PATH)
    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
    cols = {c.strip().lower(): c for c in df.columns}
    # 別名対応
    jp_col = cols.get("jp_name") or cols.get("original_name")
    en_col = cols.get("en_name") or cols.get("english_slug")
    sheet_col = cols.get("sheet_name") if "sheet_name" in cols else None
    if not (jp_col and en_col):
        raise ValueError("mapping CSVに 'jp_name(or original_name)' と 'en_name(or english_slug)' が必要です。")
    # 正規化したインデックスを作成
    out = df[[jp_col, en_col] + ([sheet_col] if sheet_col else [])].copy()
    out.columns = ["jp_name", "en_name"] + (["sheet_name"] if sheet_col else [])
    return out

def _slug_from_mapping(df_map: pd.DataFrame, original_name: str) -> tuple[str, str | None]:
    """マッピングにあればスラグとシート名を返す。なければ安全なスラグを自動生成。"""
    row = df_map.loc[df_map["jp_name"] == original_name]
    if row.empty:
        base = os.path.splitext(original_name)[0]
        safe = re.sub(r"[^0-9A-Za-z_]+", "_", base)
        safe = re.sub(r"_+", "_", safe).strip("_")
        return safe.lower(), None
    slug = str(row.iloc[0]["en_name"]).strip()
    sheet = str(row.iloc[0]["sheet_name"]).strip() if "sheet_name" in row.columns and pd.notna(row.iloc[0]["sheet_name"]) else None
    return slug, sheet

# ============== ユーティリティ ==============
def _yyyymm_now_utc():
    return dt.datetime.utcnow().strftime("%Y%m")

def _gcs_upload_xlsx(bucket, bytes_io: io.BytesIO, yyyymm: str, slug: str) -> str:
    path = f"raw/{yyyymm}/{slug}.xlsx"
    bucket.blob(path).upload_from_file(
        bytes_io,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return f"gs://{LANDING_BUCKET}/{path}"

def _post_to_run(payload: dict):
    r = requests.post(CLOUD_RUN_ENDPOINT, json=payload, timeout=60)
    return r.status_code, r.text[:500]

# ============== Pub/Sub エントリポイント ==============
def entrypoint_pubsub(event, context):
    """
    受信payload例: {"yyyymm":"202510"}
    - 指定がなければ当月(UTC)を使用
    - 対象: 月フォルダ直下の全Excel（Googleスプレッドシート含め.xlsxとして取得）
    - マッピングに従って slug, sheet_name を決定（未定義は自動スラグ）
    - GCSに raw/{yyyymm}/{slug}.xlsx で保存 → Cloud Run /ingest にPOST
    """
    payload = {}
    if "data" in event and event["data"]:
        payload = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    yyyymm = payload.get("yyyymm") or _yyyymm_now_utc()

    drive = _build_drive_service()
    month_id = _find_month_subfolder(drive, DRIVE_FOLDER_ID, yyyymm)
    if not month_id:
        print(f"[WARN] No subfolder {yyyymm} under {DRIVE_FOLDER_ID}")
        return

    # マッピング読込
    try:
        df_map = _load_mapping_csv()
    except Exception as e:
        print(f"[ERROR] mapping CSV load failed: {e}")
        df_map = pd.DataFrame(columns=["jp_name", "en_name"])  # 空で続行（自動スラグ）

    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    processed = 0
    skipped = 0

    for f in _iter_files(drive, month_id):
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        # Excel or Googleスプレッドシートのみ対象
        if not (
            name.lower().endswith((".xlsx", ".xls"))
            or mime in ("application/vnd.google-apps.spreadsheet",)
        ):
            skipped += 1
            continue

        try:
            # ダウンロード & マッピング解決
            dl_name, filebytes = _download_excel_as_xlsx(drive, f["id"])
            slug, sheet_name = _slug_from_mapping(df_map, name)

            # GCS へ保存
            gcs_uri = _gcs_upload_xlsx(bucket, filebytes, yyyymm, slug)

            # Cloud Run へ通知（/ingest）
            body = {
                "yyyymm": yyyymm,
                "slug": slug,
                "gcs_uri": gcs_uri,
                "original": name,
            }
            # 必要ならシート名を渡す（run側で対応している場合）
            if sheet_name:
                body["sheet_name"] = sheet_name

            status, text = _post_to_run(body)
            print(f"[INFO] POST Run {status} slug={slug} uri={gcs_uri} resp={text[:200]}")
            processed += 1

        except Exception as e:
            print(f"[ERROR] file '{name}' failed: {e}")

    print(f"[DONE] yyyymm={yyyymm}, processed={processed}, skipped={skipped}")


