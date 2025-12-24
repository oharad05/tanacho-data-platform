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
MAPPING_GCS_PATH   = os.environ.get("MAPPING_GCS_PATH", "google-drive/config/mapping_files.csv")
CLOUD_RUN_ENDPOINT = os.environ.get("CLOUD_RUN_ENDPOINT")   # 任意: 下流通知
SERVICE_JSON       = os.environ.get("SERVICE_JSON_PATH")    # 任意: 鍵ファイルパス（ローカル）
SERVICE_JSON_GCS   = os.environ.get("SERVICE_JSON_GCS_PATH") # 任意: 鍵ファイルパス（GCS）
IMPERSONATE_USER   = os.environ.get("IMPERSONATE_USER")     # ドメイン全体の委任: なりすますユーザー
DEFAULT_MODE       = os.environ.get("DEFAULT_MODE", "replace")  # デフォルトモード: replace / append

# === GCSからJSONキーをダウンロード ===
def _download_service_json_from_gcs():
    """GCSからサービスアカウントJSONキーをダウンロードして一時ファイルに保存"""
    if not SERVICE_JSON_GCS:
        return None
    try:
        # gs://bucket/path/to/file.json 形式をパース
        gcs_path = SERVICE_JSON_GCS.replace("gs://", "")
        bucket_name = gcs_path.split("/")[0]
        blob_path = "/".join(gcs_path.split("/")[1:])

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # 一時ファイルに保存
        temp_path = "/tmp/sa-key.json"
        blob.download_to_filename(temp_path)
        print(f"[INFO] Downloaded service account key from GCS: {SERVICE_JSON_GCS}")
        return temp_path
    except Exception as e:
        print(f"[ERROR] Failed to download service account key from GCS: {e}")
        return None

# 起動時にGCSからキーをダウンロード
_SERVICE_JSON_PATH = SERVICE_JSON or _download_service_json_from_gcs()

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
    - SERVICE_JSON_PATH または SERVICE_JSON_GCS_PATH: サービスアカウントのJSONキーファイル
    - IMPERSONATE_USER: なりすますユーザーのメールアドレス（例: fiby2@tanacho.com）

    これにより、サービスアカウントが指定ユーザーとしてDriveにアクセスできる
    """
    if _SERVICE_JSON_PATH:
        creds = service_account.Credentials.from_service_account_file(_SERVICE_JSON_PATH, scopes=SCOPES)
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
    meta = drive.files().get(fileId=file_id, fields="name", supportsAllDrives=True).execute()
    name = meta.get("name", name_hint or "file")
    out_name = name if name.lower().endswith(".xlsx") else re.sub(r"\.[^.]+$", "", name) + ".xlsx"
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
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

def _list_all_month_folders(drive, parent_or_drive_id: str) -> list:
    """
    親フォルダ配下のYYYYMM形式のフォルダを全て取得

    Returns:
        [{"id": "xxx", "name": "202509"}, ...] のリスト
    """
    month_folders = []

    # 親直下のフォルダを取得
    q = (f"'{parent_or_drive_id}' in parents and "
         "mimeType='application/vnd.google-apps.folder' and "
         "trashed=false")
    try:
        res = drive.files().list(
            q=q, fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True, pageSize=200
        ).execute()

        for folder in res.get("files", []):
            name = folder.get("name", "")
            # YYYYMM形式かチェック（6桁の数字）
            if re.match(r"^\d{6}$", name):
                month_folders.append(folder)
    except HttpError as e:
        print(f"[ERROR] Failed to list month folders: {e}")

    # 名前でソート（昇順）
    month_folders.sort(key=lambda x: x.get("name", ""))
    print(f"[INFO] Found {len(month_folders)} month folders: {[f.get('name') for f in month_folders]}")
    return month_folders

def _delete_gcs_folder(bucket, prefix: str) -> int:
    """
    GCSの指定プレフィックス配下のファイルを全て削除

    Args:
        bucket: GCSバケットオブジェクト
        prefix: 削除対象のプレフィックス（例: "google-drive/raw/202509/"）

    Returns:
        削除したファイル数
    """
    blobs = list(bucket.list_blobs(prefix=prefix))
    deleted_count = 0
    for blob in blobs:
        try:
            blob.delete()
            deleted_count += 1
        except Exception as e:
            print(f"[WARN] Failed to delete {blob.name}: {e}")

    if deleted_count > 0:
        print(f"[INFO] Deleted {deleted_count} files from gs://{bucket.name}/{prefix}")
    return deleted_count

def _gcs_upload_raw(bucket, bytes_io: io.BytesIO, yyyymm: str, slug: str, out_name: str, content_type: str) -> str:
    path = f"google-drive/raw/{yyyymm}/{slug}.xlsx"
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

# ============== 同期処理 ==============
def _process_month_folder(drive, bucket, df_map, month_folder: dict) -> dict:
    """
    1つの月フォルダを処理してGCSにアップロード

    Returns:
        {"yyyymm": str, "processed": int, "skipped": int, "failed": list, "success": list}
    """
    yyyymm = month_folder.get("name")
    month_id = month_folder.get("id")

    result = {
        "yyyymm": yyyymm,
        "processed": 0,
        "skipped": 0,
        "failed": [],
        "success": []
    }

    for f in _iter_files(drive, month_id):
        name = f.get("name", "")
        lname = name.lower()

        if not lname.endswith(".xlsx"):
            result["skipped"] += 1
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
            result["processed"] += 1
            result["success"].append({"file": name, "gcs_uri": gcs_uri})
        except Exception as e:
            print(f"[ERROR] file '{name}' failed: {e}\n{traceback.format_exc()}")
            result["failed"].append({"file": name, "error": str(e)})

    return result


def sync_drive_to_gcs(mode: str = "replace", target_month: str = None) -> dict:
    """
    Google Drive から GCS へ同期

    Args:
        mode: "replace"(全データ洗い替え) / "append"(指定月のみ追加)
        target_month: appendモード時の対象月（YYYYMM形式）

    Returns:
        同期結果の辞書
    """
    print("=" * 60)
    print(f"drive-to-gcs 同期開始")
    print(f"  モード: {mode}")
    print(f"  対象月: {target_month if target_month else '全月'}")
    print("=" * 60)

    results = {
        "mode": mode,
        "target_month": target_month,
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "months_processed": [],
        "total_processed": 0,
        "total_skipped": 0,
        "total_failed": [],
        "total_success": [],
        "errors": []
    }

    try:
        drive = _build_drive_service()
    except Exception as e:
        results["errors"].append({"type": "DRIVE_SERVICE_ERROR", "message": str(e)})
        print(f"[ERROR] Drive service build failed: {e}")
        return results

    try:
        df_map = _load_mapping_csv()
    except Exception as e:
        print(f"[WARN] mapping CSV load failed: {e}")
        df_map = pd.DataFrame(columns=["jp_name", "en_name"])

    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    # 処理対象の月フォルダを決定
    if mode == "replace":
        # 全月フォルダを取得
        month_folders = _list_all_month_folders(drive, DRIVE_FOLDER_ID)

        # GCSの既存データを削除
        print("[INFO] Deleting existing GCS data (replace mode)...")
        _delete_gcs_folder(bucket, "google-drive/raw/")

    elif mode == "append":
        if not target_month:
            results["errors"].append({
                "type": "INVALID_PARAMETER",
                "message": "appendモードではtarget_monthが必須です"
            })
            return results

        # 指定月のフォルダのみ取得
        month_id = _find_month_subfolder(drive, DRIVE_FOLDER_ID, target_month)
        if not month_id:
            results["errors"].append({
                "type": "FOLDER_NOT_FOUND",
                "message": f"月フォルダが見つかりません: {target_month}"
            })
            return results

        month_folders = [{"id": month_id, "name": target_month}]

        # 指定月のGCSデータのみ削除
        print(f"[INFO] Deleting GCS data for {target_month} (append mode)...")
        _delete_gcs_folder(bucket, f"google-drive/raw/{target_month}/")

    else:
        results["errors"].append({
            "type": "INVALID_MODE",
            "message": f"無効なモード: {mode}（replace / append のみ有効）"
        })
        return results

    # 各月フォルダを処理
    for month_folder in month_folders:
        print(f"\n[INFO] Processing month folder: {month_folder.get('name')}")
        month_result = _process_month_folder(drive, bucket, df_map, month_folder)

        results["months_processed"].append(month_result["yyyymm"])
        results["total_processed"] += month_result["processed"]
        results["total_skipped"] += month_result["skipped"]
        results["total_failed"].extend(month_result["failed"])
        results["total_success"].extend(month_result["success"])

    # 0件アラートのチェック
    if results["total_processed"] == 0 and not results["total_failed"]:
        results["errors"].append({
            "type": "EMPTY_DATA",
            "message": "取り込み件数が0件です"
        })

    # 結果サマリを出力
    print("\n" + "=" * 60)
    print("drive-to-gcs 同期完了")
    print(f"  処理月数: {len(results['months_processed'])}")
    print(f"  成功ファイル数: {results['total_processed']}")
    print(f"  スキップ数: {results['total_skipped']}")
    print(f"  失敗ファイル数: {len(results['total_failed'])}")
    if results["total_failed"]:
        print("  失敗ファイル一覧:")
        for f in results["total_failed"]:
            print(f"    - {f.get('file')}: {f.get('error')}")
    print("=" * 60)

    return results


# ============== Pub/Sub 本体処理（後方互換） ==============
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
    mode = payload.get("mode", DEFAULT_MODE)

    # 新しい同期関数を呼び出し
    if mode == "replace":
        sync_drive_to_gcs(mode="replace")
    else:
        sync_drive_to_gcs(mode="append", target_month=yyyymm)

# ============== Cloud Run HTTP 受け口 ==============
app = Flask(__name__)

@app.route("/sync", methods=["POST"])
def sync_endpoint():
    """
    同期エンドポイント（Cloud Workflows から呼び出し）

    パラメータ（クエリパラメータまたはJSONボディ）:
        mode: "replace"(デフォルト) / "append"
        target_month: appendモード時の対象月（YYYYMM形式）

    例:
        POST /sync?mode=replace
        POST /sync?mode=append&target_month=202511
        POST /sync -d '{"mode": "append", "target_month": "202511"}'
    """
    try:
        # パラメータ取得（クエリパラメータ優先、なければJSONボディ）
        mode = request.args.get("mode")
        target_month = request.args.get("target_month")

        if not mode:
            body = request.get_json(force=True, silent=True) or {}
            mode = body.get("mode", DEFAULT_MODE)
            target_month = target_month or body.get("target_month")

        # 同期実行
        results = sync_drive_to_gcs(mode=mode, target_month=target_month)

        # エラーがあれば適切なステータスコードを返す
        if results.get("errors"):
            # エラーの種類に応じてステータスコードを決定
            error_types = [e.get("type") for e in results["errors"]]
            if "DRIVE_SERVICE_ERROR" in error_types:
                return jsonify(results), 500
            elif "INVALID_PARAMETER" in error_types or "INVALID_MODE" in error_types:
                return jsonify(results), 400
            elif "FOLDER_NOT_FOUND" in error_types:
                return jsonify(results), 404
            elif "EMPTY_DATA" in error_types:
                # 0件は警告扱い（207 Multi-Status）
                return jsonify(results), 207
            else:
                return jsonify(results), 500

        # 失敗ファイルがあれば207
        if results.get("total_failed"):
            return jsonify(results), 207

        return jsonify(results), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "timestamp": dt.datetime.utcnow().isoformat() + "Z"
        }), 500


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

