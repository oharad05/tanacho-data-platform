import os, io, json, base64, datetime as dt, pandas as pd, requests, re, traceback
from flask import Flask, request, jsonify  # ← jsonify を追加
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.cloud import storage
from google.auth import default as google_auth_default

# === 環境変数 ===
PROJECT_ID         = os.environ.get("GCP_PROJECT")
DRIVE_FOLDER_ID    = os.environ["DRIVE_FOLDER_ID"]          # 親フォルダ fileId もしくは 共有ドライブ driveId
LANDING_BUCKET     = os.environ["LANDING_BUCKET"]           # GCSバケット名
MAPPING_GCS_PATH   = os.environ.get("MAPPING_GCS_PATH", "config/mapping_files.csv")
CLOUD_RUN_ENDPOINT = os.environ.get("CLOUD_RUN_ENDPOINT")   # 任意: 下流通知
SERVICE_JSON       = os.environ.get("SERVICE_JSON_PATH")    # 任意: 鍵ファイル
IMPERSONATE_USER   = os.environ.get("IMPERSONATE_USER")     # ドメイン全体の委任: なりすますユーザー

# スコープ: 管理コンソールで登録したものと一致させる
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

# ============== Drive API ==============
def _build_drive_service():
    """
    Drive APIサービスを構築

    ドメイン全体の委任（Domain-wide Delegation）を使用する場合:
    - SERVICE_JSON_PATH: サービスアカウントのJSONキーファイルパス
    - IMPERSONATE_USER: なりすますユーザーのメールアドレス（例: fiby2@tanacho.com）

    これにより、サービスアカウントが指定ユーザーとしてDriveにアクセスできる
    """
    if SERVICE_JSON:
        creds = service_account.Credentials.from_service_account_file(SERVICE_JSON, scopes=SCOPES)
        # ドメイン全体の委任: ユーザーになりすまし
        if IMPERSONATE_USER:
            creds = creds.with_subject(IMPERSONATE_USER)
            print(f"[INFO] Domain-wide delegation enabled: impersonating {IMPERSONATE_USER}")
    else:
        creds, _ = google_auth_default(scopes=SCOPES)
        print("[INFO] Using default credentials (no impersonation)")
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _get_drive_id_of(drive, file_id: str) -> str | None:
    """file_id がフォルダIDなら、その所属共有ドライブIDを返す。共有ドライブIDが渡された場合は None の可能性。"""
    try:
        meta = drive.files().get(fileId=file_id, fields="driveId", supportsAllDrives=True).execute()
        return meta.get("driveId")
    except HttpError:
        return None

# --- 3-1) 子フォルダ一覧（デバッグ用） ---
def _list_children_folders(drive, parent_id):
    res = drive.files().list(
        q=f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id,name)", pageSize=200,
        includeItemsFromAllDrives=True, supportsAllDrives=True
    ).execute()
    return res.get("files", [])

def _find_month_subfolder(drive, parent_or_drive_id: str, yyyymm: str) -> str | None:
    """
    1) 親フォルダ直下で name='YYYYMM' を検索
    2) 見つからなければ、親の driveId を逆引き or 親を driveId とみなして drive-wide 検索
    """
    # --- 1) 親直下検索 ---
    q = (f"'{parent_or_drive_id}' in parents and "
         "mimeType='application/vnd.google-apps.folder' and "
         f"name='{yyyymm}' and trashed=false")
    try:
        res = drive.files().list(
            q=q, fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True, pageSize=50
        ).execute()
        files = res.get("files", [])
        if files:
            return files[0]["id"]
        # --- 3-2) 見つからなければ親直下の子フォルダ名をデバッグ出力 ---
        kids = _list_children_folders(drive, parent_or_drive_id)
        print(f"[DEBUG] children under {parent_or_drive_id}: {[k.get('name') for k in kids]}")
    except HttpError as e:
        print(f"[DEBUG] parent search failed (assumed folderId={parent_or_drive_id}): {e}")

    # --- 2) drive-wide 検索 ---
    drive_id_for_wide = _get_drive_id_of(drive, parent_or_drive_id) or parent_or_drive_id
    try:
        res2 = drive.files().list(
            corpora="drive", driveId=drive_id_for_wide,
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            q="mimeType='application/vnd.google-apps.folder' and "
              f"name='{yyyymm}' and trashed=false",
            fields="files(id,name,parents)", pageSize=200
        ).execute()
        files2 = res2.get("files", [])
        if files2:
            if len(files2) > 1:
                print(f"[DEBUG] multiple '{yyyymm}' folders in driveId={drive_id_for_wide}: "
                      f"{[(f.get('name'), f.get('id')) for f in files2]}")
            return files2[0]["id"]
        else:
            # drive-wide でも見つからない場合、候補一覧をデバッグ出力
            res3 = drive.files().list(
                corpora="drive", driveId=drive_id_for_wide,
                includeItemsFromAllDrives=True, supportsAllDrives=True,
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id,name)", pageSize=200
            ).execute()
            print(f"[DEBUG] wide candidates in driveId={drive_id_for_wide}: {[f.get('name') for f in res3.get('files', [])]}")
    except HttpError as e:
        print(f"[DEBUG] drive-wide search failed (driveId={drive_id_for_wide}): {e}")

    print(f"[WARN] No subfolder {yyyymm} under/within {parent_or_drive_id}")
    return None

def _iter_files(drive, folder_id):
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
    """Drive上の .xlsx をそのままダウンロード"""
    meta = drive.files().get(fileId=file_id, fields="name").execute()
    name = meta.get("name", name_hint or "file")
    out_name = name if name.lower().endswith(".xlsx") else re.sub(r"\.[^.]+$", "", name) + ".xlsx"
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO(req.execute()); buf.seek(0)
    return out_name, buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# ============== マッピング ==============
def _load_mapping_csv():
    client = storage.Client()
    blob = client.bucket(LANDING_BUCKET).blob(MAPPING_GCS_PATH)
    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
    cols = {c.strip().lower(): c for c in df.columns}
    jp_col = cols.get("jp_name") or cols.get("original_name")
    en_col = cols.get("en_name") or cols.get("english_slug")
    sheet_col = cols.get("sheet_name") if "sheet_name" in cols else None
    if not (jp_col and en_col):
        raise ValueError("mapping CSVに 'jp_name(or original_name)' と 'en_name(or english_slug)' が必要です。")
    out = df[[jp_col, en_col] + ([sheet_col] if sheet_col else [])].copy()
    out.columns = ["jp_name", "en_name"] + (["sheet_name"] if sheet_col else [])
    return out

def _slug_from_mapping(df_map: pd.DataFrame, original_name: str):
    row = df_map.loc[df_map["jp_name"] == original_name]
    if row.empty:
        base = os.path.splitext(original_name)[0]
        safe = re.sub(r"[^0-9A-Za-z_]+", "_", base)
        safe = re.sub(r"_+", "_", safe).strip("_")
        return safe.lower(), None
    slug = str(row.iloc[0]["en_name"]).strip()
    sheet = str(row.iloc[0]["sheet_name"]).strip() if ("sheet_name" in row.columns and pd.notna(row.iloc[0]["sheet_name"])) else None
    return slug, sheet

# ============== ユーティリティ ==============
def _yyyymm_now_utc():
    return dt.datetime.utcnow().strftime("%Y%m")

def _gcs_upload_raw(bucket, bytes_io: io.BytesIO, yyyymm: str, slug: str, out_name: str, content_type: str) -> str:
    path = f"raw/{yyyymm}/{slug}.xlsx"
    blob = bucket.blob(path)
    blob.content_type = content_type
    blob.upload_from_file(bytes_io)
    return f"gs://{LANDING_BUCKET}/{path}"

def _post_to_run(payload: dict):
    if not CLOUD_RUN_ENDPOINT:
        print("[INFO] CLOUD_RUN_ENDPOINT not set. Skip downstream POST.")
        return 0, "skipped"
    try:
        r = requests.post(CLOUD_RUN_ENDPOINT, json=payload, timeout=60)
        return r.status_code, r.text[:500]
    except Exception as e:
        print(f"[WARN] POST to {CLOUD_RUN_ENDPOINT} failed: {e}")
        return -1, str(e)[:500]

# ============== Pub/Sub 本体処理 ==============
def entrypoint_pubsub(event, context):
    """
    入力: {"yyyymm":"202510"} を base64で包んだ Pub/Sub message.data
    対象: 親フォルダ直下 or 共有ドライブ全体から『name=YYYYMM』のフォルダを見つけ、
          その配下の **.xlsx** を raw/{yyyymm}/{slug}.xlsx として保存
    """
    payload = {}
    if "data" in event and event["data"]:
        payload = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    yyyymm = payload.get("yyyymm") or _yyyymm_now_utc()
    print(f"[INFO] Start ingest yyyymm={yyyymm}")

    drive = _build_drive_service()
    month_id = _find_month_subfolder(drive, DRIVE_FOLDER_ID, yyyymm)
    if not month_id:
        return

    try:
        df_map = _load_mapping_csv()
    except Exception as e:
        print(f"[ERROR] mapping CSV load failed: {e}")
        df_map = pd.DataFrame(columns=["jp_name", "en_name"])

    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    processed = 0
    skipped = 0

    for f in _iter_files(drive, month_id):
        name = f.get("name", "")
        lname = name.lower()

        if not lname.endswith(".xlsx"):
            skipped += 1
            print(f"[SKIP] not xlsx: name={name}")
            continue

        try:
            out_name, filebytes, ctype = _download_xlsx(drive, f["id"], name_hint=name)
            slug, sheet_name = _slug_from_mapping(df_map, name)

            gcs_uri = _gcs_upload_raw(bucket, filebytes, yyyymm, slug, out_name, ctype)

            body = {"yyyymm": yyyymm, "slug": slug, "gcs_uri": gcs_uri, "original": name}
            if sheet_name:
                body["sheet_name"] = sheet_name
            status, _ = _post_to_run(body)

            print(f"[OK] saved {gcs_uri} from {name} post_status={status}")
            processed += 1
        except Exception as e:
            print(f"[ERROR] file '{name}' failed: {e}\n{traceback.format_exc()}")

    print(f"[DONE] yyyymm={yyyymm}, processed={processed}, skipped={skipped}")

# ============== Cloud Run HTTP 受け口 ==============
app = Flask(__name__)

@app.route("/pubsub", methods=["POST"])
def pubsub_http():
    try:
        envelope = request.get_json(force=True, silent=True) or {}
        if not isinstance(envelope, dict):
            return ("Bad Request: not JSON", 400)
        msg = envelope.get("message", {})
        data = msg.get("data")
        event = {"data": data}
        entrypoint_pubsub(event, None)
        return ("", 204)
    except Exception as e:
        traceback.print_exc()
        return (f"Error: {e}", 500)

@app.route("/", methods=["POST"])
def eventarc_pubsub():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        msg = payload.get("message", {})
        data_b64 = msg.get("data") or (payload.get("data") if isinstance(payload.get("data"), str) else None)
        if not data_b64:
            print(f"[WARN] Eventarc payload has no message.data. payload={payload}")
            return ("Bad Request: no message.data", 400)
        event = {"data": data_b64}
        entrypoint_pubsub(event, None)
        return ("", 204)
    except Exception as e:
        traceback.print_exc()
        return (f"Error: {e}", 500)

# --- かんたん診断ルート（追加分） ---
@app.route("/debug/folder", methods=["GET"])
def debug_folder():
    try:
        folder_id = request.args.get("id")
        if not folder_id:
            return ("Missing ?id=<folderId>", 400)
        drive = _build_drive_service()
        # meta
        meta = drive.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,driveId,parents,shortcutDetails",
            supportsAllDrives=True,
        ).execute()
        # children（フォルダの場合のみ）
        children = []
        if meta.get("mimeType") == "application/vnd.google-apps.folder":
            res = drive.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id,name,mimeType)",
                pageSize=200,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ).execute()
            children = res.get("files", [])
        return jsonify({"meta": meta, "children": children})
    except Exception as e:
        traceback.print_exc()
        return (f"Error: {e}", 500)

@app.route("/", methods=["GET"])
def health():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

