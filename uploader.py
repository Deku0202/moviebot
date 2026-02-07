#!/usr/bin/env python3
import argparse
import asyncio
import io
import json
import os
import time
import random
import queue
import threading
from collections import deque
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv

# Telegram imports
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged

# Google Drive imports
import requests
from googleapiclient.discovery import build
from google.auth.transport.requests import AuthorizedSession, Request as GoogleRequest
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# FastTelethon
from FastTelethonn import upload_file as fast_upload_file

load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {name}: {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}: {raw!r}") from exc


def _parse_target(value: str):
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


def _parse_allowed_targets(raw: str) -> set:
    targets = set()
    for part in raw.split(","):
        parsed = _parse_target(part)
        if parsed is not None:
            targets.add(parsed)
    return targets


# ---------------- CONFIG ----------------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
GOOGLE_CREDS_FILE = (os.getenv("GOOGLE_CREDS_FILE") or "credentials.json").strip()
GOOGLE_OAUTH_TOKEN = (os.getenv("GOOGLE_OAUTH_TOKEN") or "token.json").strip()

# Optional target allowlist: TG_ALLOWED_TARGETS=@channel,-100123...
ALLOWED_TARGETS = _parse_allowed_targets(os.getenv("TG_ALLOWED_TARGETS") or "")
_default_target = _parse_target(os.getenv("TG_TARGET") or "")
if _default_target is not None:
    ALLOWED_TARGETS.add(_default_target)

# SAFETY pacing (tune)
MIN_SECONDS_BETWEEN_SENDS = _env_float("MIN_SECONDS_BETWEEN_SENDS", 2.0)
JITTER_SECONDS = (
    _env_float("SEND_JITTER_MIN_SECONDS", 0.0),
    _env_float("SEND_JITTER_MAX_SECONDS", 2.0),
)
if JITTER_SECONDS[0] > JITTER_SECONDS[1]:
    JITTER_SECONDS = (JITTER_SECONDS[1], JITTER_SECONDS[0])
MAX_SEND_RETRIES = _env_int("MAX_SEND_RETRIES", 8)

# Optional: daily cap (0 disables)
DAILY_SEND_LIMIT = _env_int("DAILY_SEND_LIMIT", 0)
DAILY_STATE_FILE = (os.getenv("DAILY_STATE_FILE") or "daily_send_state.json").strip()

# Transfer tuning
TG_UPLOAD_PART_SIZE_KB = _env_int("TG_UPLOAD_PART_SIZE_KB", 512)
TG_UPLOAD_CONNECTIONS = _env_int("TG_UPLOAD_CONNECTIONS", 12)
TG_UPLOAD_READ_CHUNK_KB = _env_int("TG_UPLOAD_READ_CHUNK_KB", 512)
DRIVE_STREAM_CHUNK_MB = _env_int("DRIVE_STREAM_CHUNK_MB", 64)
DRIVE_STREAM_PREFETCH_CHUNKS = _env_int("DRIVE_STREAM_PREFETCH_CHUNKS", 16)
DRIVE_REQUEST_TIMEOUT_SEC = _env_int("DRIVE_REQUEST_TIMEOUT_SEC", 300)
# ----------------------------------------


# ---------- Simple daily cap ----------
def _load_daily_state() -> Dict[str, Any]:
    if not os.path.exists(DAILY_STATE_FILE):
        return {}
    try:
        with open(DAILY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_daily_state(state: Dict[str, Any]) -> None:
    with open(DAILY_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

def _today_key() -> str:
    return time.strftime("%Y-%m-%d")

def check_daily_limit_or_raise():
    if DAILY_SEND_LIMIT <= 0:
        return
    state = _load_daily_state()
    key = _today_key()
    sent = int(state.get(key, 0))
    if sent >= DAILY_SEND_LIMIT:
        raise RuntimeError(f"Daily send limit reached ({sent}/{DAILY_SEND_LIMIT}).")
    state[key] = sent + 1
    _save_daily_state(state)


# ---------- Telegram safety helpers ----------
_last_send_ts = 0.0

def assert_allowed_target(target_chat):
    if ALLOWED_TARGETS and target_chat not in ALLOWED_TARGETS:
        raise ValueError(
            f"Target {target_chat!r} is not in TG_ALLOWED_TARGETS. Refusing to send."
        )
        
        
async def resolve_target_entity(client, target_chat):
    try:
        return await client.get_input_entity(target_chat)
    except Exception:
        await client.get_dialogs(limit=200)
        return await client.get_input_entity(target_chat)

async def safe_sleep_between_sends():
    global _last_send_ts
    now = asyncio.get_event_loop().time()
    elapsed = now - _last_send_ts
    wait = max(0.0, MIN_SECONDS_BETWEEN_SENDS - elapsed)
    wait += random.uniform(*JITTER_SECONDS)
    if wait > 0:
        print(f"[Safety] Waiting {wait:.1f}s before sending...")
        await asyncio.sleep(wait)
    _last_send_ts = asyncio.get_event_loop().time()

async def send_file_safe(client, target_chat, uploaded_file, caption: str = "", as_video: bool = False):
    check_daily_limit_or_raise()
    

    # Resolve entity once (avoids PeerChannel errors)
    entity = await resolve_target_entity(client, target_chat)
    await safe_sleep_between_sends()

    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            return await client.send_file(
                entity,
                uploaded_file,
                caption=caption,
                supports_streaming=as_video,
                force_document=not as_video,
            )

        except FloodWaitError as e:
            wait_s = int(getattr(e, "seconds", 60))
            extra = random.uniform(1, 5)
            print(f"[FloodWait] Sleeping {wait_s + extra:.1f}s...")
            await asyncio.sleep(wait_s + extra)

        except RPCError as e:
            backoff = min(60, 2 ** attempt)
            extra = random.uniform(1, 5)
            print(f"[RPCError] {e.__class__.__name__}: {e}. Backing off {backoff + extra:.1f}s...")
            await asyncio.sleep(backoff + extra)

    raise RuntimeError("Failed to send after retries.")

# ---------- Google Drive Auth ----------
def build_drive_service_and_creds(creds_path: str = GOOGLE_CREDS_FILE):
    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"Missing {creds_path} in current directory.")

    with open(creds_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Service Account
    if data.get("type") == "service_account":
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return service, creds

    # OAuth client secrets
    if "installed" not in data and "web" not in data:
        raise ValueError(
            "credentials.json must be either:\n"
            "- OAuth client secrets (Desktop app) JSON with 'installed' or 'web', OR\n"
            "- service account JSON with 'type=service_account'."
        )

    creds = None
    if os.path.exists(GOOGLE_OAUTH_TOKEN):
        creds = Credentials.from_authorized_user_file(GOOGLE_OAUTH_TOKEN, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_OAUTH_TOKEN, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service, creds


# ---------- Drive listing helpers ----------
def list_shared_drives(service, max_results: int = 100) -> List[Dict[str, Any]]:
    resp = service.drives().list(pageSize=max_results, fields="drives(id,name)").execute()
    return resp.get("drives", [])

def _list_files_with_scope(
    service,
    q: str,
    fields: str,
    scope: str,
    shared_drive_id: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page_token = None
    while True:
        kwargs: Dict[str, Any] = {
            "q": q,
            "fields": f"nextPageToken, files({fields})",
            "pageSize": 1000,
            "pageToken": page_token,
        }

        if scope == "my":
            kwargs["corpora"] = "user"
        elif scope == "shared":
            if not shared_drive_id:
                raise ValueError("shared_drive_id is required when scope='shared'.")
            kwargs["corpora"] = "drive"
            kwargs["driveId"] = shared_drive_id
            kwargs["includeItemsFromAllDrives"] = True
            kwargs["supportsAllDrives"] = True
        else:
            kwargs["corpora"] = "allDrives"
            kwargs["includeItemsFromAllDrives"] = True
            kwargs["supportsAllDrives"] = True

        res = service.files().list(**kwargs).execute()
        out.extend(res.get("files", []))
        if len(out) >= limit:
            return out[:limit]

        page_token = res.get("nextPageToken")
        if not page_token:
            return out


def list_folders(
    service,
    scope: str = "all",
    shared_drive_id: Optional[str] = None,
    name_contains: Optional[str] = None,
    limit: int = 200,
):
    q_parts = ["mimeType='application/vnd.google-apps.folder'", "trashed=false"]
    if name_contains:
        safe = name_contains.replace("'", "\\'")
        q_parts.append(f"name contains '{safe}'")
    q = " and ".join(q_parts)

    out = _list_files_with_scope(
        service=service,
        q=q,
        fields="id,name",
        scope=scope,
        shared_drive_id=shared_drive_id,
        limit=limit,
    )
    out.sort(key=lambda x: (x.get("name") or "").lower())
    return out

def list_files_in_folder(
    service,
    folder_id: str,
    scope: str = "all",
    shared_drive_id: Optional[str] = None,
    name_contains: Optional[str] = None,
    extensions: Optional[tuple] = None,
    limit: int = 200,
):
    q_parts = [f"'{folder_id}' in parents", "trashed=false", "mimeType!='application/vnd.google-apps.folder'"]
    if name_contains:
        safe = name_contains.replace("'", "\\'")
        q_parts.append(f"name contains '{safe}'")
    q = " and ".join(q_parts)

    out = _list_files_with_scope(
        service=service,
        q=q,
        fields="id,name,size,modifiedTime,mimeType",
        scope=scope,
        shared_drive_id=shared_drive_id,
        limit=limit,
    )

    if extensions:
        exts = tuple(e.lower() for e in extensions)
        out = [f for f in out if (f.get("name") or "").lower().endswith(exts)]

    out.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return out

def pick_by_number_or_id(prompt: str, items: List[Dict[str, Any]]) -> str:
    choice = input(prompt).strip()
    if not choice:
        raise ValueError("Selection cannot be empty.")
    if choice.isdigit():
        idx = int(choice)
        if idx < 1 or idx > len(items):
            raise ValueError("Invalid number.")
        return items[idx - 1]["id"]
    return choice


class DrivePrefetchReader(io.RawIOBase):
    def __init__(self, response: requests.Response, name: str, chunk_mb: int = 64, max_queue_chunks: int = 16):
        self._resp = response
        self.name = name
        self._q: "queue.Queue[bytes | None]" = queue.Queue(maxsize=max_queue_chunks)
        self._buf = bytearray()
        self._eof = False

        def worker():
            try:
                for chunk in response.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    if chunk:
                        self._q.put(chunk)
            finally:
                self._q.put(None)

        self._t = threading.Thread(target=worker, daemon=True)
        self._t.start()

    def readable(self):
        return True

    def read(self, n=-1):
        if n == 0:
            return b""

        while not self._eof and (n < 0 or len(self._buf) < n):
            item = self._q.get()
            if item is None:
                self._eof = True
                break
            self._buf.extend(item)

        if n < 0:
            data = bytes(self._buf)
            self._buf.clear()
            return data

        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def close(self):
        try:
            self._resp.close()
        finally:
            super().close()

def make_progress_printer(label: str, window_sec: float = 5.0):
    start = time.time()
    samples = deque()  # (t, sent_bytes)
    last_print = 0.0

    def cb(sent_bytes: int, total_bytes: int):
        nonlocal last_print
        now = time.time()

        # keep samples in a rolling time window
        samples.append((now, sent_bytes))
        while samples and (now - samples[0][0]) > window_sec:
            samples.popleft()

        # print at most once per second
        if now - last_print < 1.0 and sent_bytes != total_bytes:
            return
        last_print = now

        # rolling speed
        if len(samples) >= 2:
            t0, b0 = samples[0]
            t1, b1 = samples[-1]
            dt = max(t1 - t0, 1e-6)
            speed_mb_s = ((b1 - b0) / dt) / (1024 * 1024)
        else:
            speed_mb_s = 0.0

        pct = (sent_bytes / total_bytes) * 100 if total_bytes else 0.0

        # ETA using overall average (stable)
        elapsed = max(now - start, 1e-6)
        avg_bps = sent_bytes / elapsed
        eta = ""
        if total_bytes and avg_bps > 0:
            remaining = total_bytes - sent_bytes
            eta_sec = int(remaining / avg_bps)
            eta = f" | ETA {eta_sec//60:02d}:{eta_sec%60:02d}"

        print(f"{label}: {pct:6.2f}% | {speed_mb_s:7.2f} MB/s{eta}")

    return cb

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a file from Google Drive to Telegram.",
    )
    parser.add_argument("--api-id", help="Telegram API ID. Falls back to TG_API_ID.")
    parser.add_argument("--api-hash", help="Telegram API hash. Falls back to TG_API_HASH.")
    parser.add_argument("--session", help="Telethon session file. Falls back to TG_SESSION.")
    parser.add_argument("--target", help="Telegram target username or -100... ID. Falls back to TG_TARGET.")

    parser.add_argument(
        "--scope",
        choices=["all", "my", "shared"],
        help="Folder search scope. If omitted, interactive selection is used.",
    )
    parser.add_argument("--shared-drive-id", help="Shared Drive ID (used when --scope shared).")
    parser.add_argument("--folder-id", help="Folder ID to browse files from.")
    parser.add_argument("--file-id", help="Google Drive file ID to upload directly.")
    parser.add_argument("--folder-filter", help="Folder name contains filter.")
    parser.add_argument("--file-filter", help="File name contains filter.")
    parser.add_argument("--video-only", action="store_true", help="Only list common video extensions.")
    parser.add_argument("--first-match", action="store_true", help="Auto-pick newest file from filtered list.")
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting. Requires either --file-id, or --folder-id with --first-match.",
    )
    return parser.parse_args()


def _require_text(value: Optional[str], prompt: str, non_interactive: bool, missing_message: str) -> str:
    if value and value.strip():
        return value.strip()
    if non_interactive:
        raise ValueError(missing_message)
    typed = input(prompt).strip()
    if not typed:
        raise ValueError(missing_message)
    return typed


def _resolve_scope_and_drive(service, args: argparse.Namespace):
    scope = (args.scope or "").strip().lower()
    shared_drive_id = (args.shared_drive_id or "").strip() or None

    if not scope:
        if shared_drive_id:
            scope = "shared"
        elif args.non_interactive:
            scope = "all"
        else:
            print("\nFolder search scope:")
            print(" 1. All accessible drives (My Drive + Shared Drives)")
            print(" 2. My Drive only")
            print(" 3. A specific Shared Drive")
            scope_choice = (input("Pick scope [1/2/3, default=1]: ").strip() or "1").lower()
            if scope_choice in {"2", "my", "m"}:
                scope = "my"
            elif scope_choice in {"3", "shared", "s"}:
                scope = "shared"
            elif scope_choice in {"1", "all", "a"}:
                scope = "all"
            else:
                raise ValueError("Invalid scope choice. Use 1, 2, or 3.")

    if scope == "shared" and not shared_drive_id:
        drives = list_shared_drives(service)
        if not drives:
            raise RuntimeError("No Shared Drives visible to these credentials.")
        if args.non_interactive:
            raise ValueError("Missing --shared-drive-id for --scope shared in non-interactive mode.")
        print("\nShared Drives:")
        for i, d in enumerate(drives, 1):
            print(f"{i:>2}. {d['name']}  -->  {d['id']}")
        shared_drive_id = pick_by_number_or_id("\nPick Shared Drive by number or paste ID: ", drives)

    return scope, shared_drive_id


async def main(args: argparse.Namespace):
    api_id_str = _require_text(
        args.api_id or (os.getenv("TG_API_ID") or "").strip(),
        "Telegram api_id: ",
        args.non_interactive,
        "Telegram api_id is required (set TG_API_ID or --api-id).",
    )
    api_hash = _require_text(
        args.api_hash or (os.getenv("TG_API_HASH") or "").strip(),
        "Telegram api_hash: ",
        args.non_interactive,
        "Telegram api_hash is required (set TG_API_HASH or --api-hash).",
    )
    api_id = int(api_id_str)

    session_name = (args.session or os.getenv("TG_SESSION") or "telethon.session").strip()
    target_raw = _require_text(
        args.target or (os.getenv("TG_TARGET") or "").strip(),
        "Send to: ",
        args.non_interactive,
        "Telegram target is required (set TG_TARGET or --target).",
    )
    target_chat = _parse_target(target_raw)

    # Optional safety check when TG_ALLOWED_TARGETS is configured
    assert_allowed_target(target_chat)

    if args.non_interactive and not args.file_id and not (args.folder_id and args.first_match):
        raise ValueError("Non-interactive mode requires --file-id, or --folder-id with --first-match.")

    drive_service, drive_creds = build_drive_service_and_creds(GOOGLE_CREDS_FILE)

    file_id = (args.file_id or "").strip() or None
    scope = "all"
    shared_drive_id = None
    if not file_id:
        scope, shared_drive_id = _resolve_scope_and_drive(drive_service, args)

        folder_id = (args.folder_id or "").strip() or None
        if not folder_id:
            folder_filter = (args.folder_filter or "").strip() or None
            if not args.folder_filter and not args.non_interactive:
                folder_filter = input("\nFolder name filter (press Enter for none): ").strip() or None
            print("[Drive] Loading folders... this can take some time on large drives.")
            folders = list_folders(
                drive_service,
                scope=scope,
                shared_drive_id=shared_drive_id,
                name_contains=folder_filter,
                limit=200,
            )
            if not folders:
                raise RuntimeError("No folders found (or insufficient permission).")

            print("\nFolders (up to 200):")
            for i, f in enumerate(folders, 1):
                print(f"{i:>3}. {f['name']}  -->  {f['id']}")
            folder_id = pick_by_number_or_id("\nPick folder by number or paste folder ID: ", folders)

        only_video = args.video_only
        if not args.video_only and not args.non_interactive:
            only_video = input("\nList only video extensions? (y/N): ").strip().lower() == "y"
        exts = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv") if only_video else None

        file_filter = (args.file_filter or "").strip() or None
        if not args.file_filter and not args.non_interactive:
            file_filter = input("File name contains filter (press Enter for none): ").strip() or None
        print("[Drive] Loading files... please wait.")
        files = list_files_in_folder(
            drive_service,
            folder_id=folder_id,
            scope=scope,
            shared_drive_id=shared_drive_id,
            name_contains=file_filter,
            extensions=exts,
            limit=200,
        )
        if not files:
            raise RuntimeError("No files found in that folder (with your filters).")

        if args.first_match:
            file_id = files[0]["id"]
            print(f"[Select] Using newest match: {files[0]['name']} (id={file_id})")
        else:
            print("\nFiles (newest first, up to 200):")
            for i, f in enumerate(files, 1):
                size_mb = (int(f.get("size", 0)) / (1024 * 1024)) if f.get("size") else 0
                print(f"{i:>3}. {f['name']}  |  {size_mb:.2f} MB  |  id={f['id']}")
            if args.non_interactive:
                raise ValueError("Non-interactive mode needs --first-match (or --file-id) to avoid prompts.")
            file_id = pick_by_number_or_id("\nPick file by number or paste file ID: ", files)

    meta = drive_service.files().get(fileId=file_id, fields="name,size,mimeType", supportsAllDrives=True).execute()
    filename = meta["name"]
    mime_type = meta.get("mimeType") or ""
    if mime_type.startswith("application/vnd.google-apps."):
        raise RuntimeError(
            f"'{filename}' is a native Google file ({mime_type}) and cannot be downloaded with alt=media."
        )
    if "size" not in meta:
        raise RuntimeError(f"Google Drive did not return a downloadable size for '{filename}'.")
    file_size = int(meta["size"])
    print(f"\nSelected: {filename} ({file_size/1024/1024:.2f} MB)")

    sess = AuthorizedSession(drive_creds)
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    resp = sess.get(url, stream=True, timeout=DRIVE_REQUEST_TIMEOUT_SEC)
    resp.raise_for_status()

    stream = DrivePrefetchReader(
        resp,
        name=filename,
        chunk_mb=DRIVE_STREAM_CHUNK_MB,
        max_queue_chunks=DRIVE_STREAM_PREFETCH_CHUNKS,
    )

    client = TelegramClient(
        session_name,
        api_id,
        api_hash,
        connection=ConnectionTcpAbridged,
        connection_retries=5,
        retry_delay=2,
        timeout=20,
        request_retries=5,
        auto_reconnect=True,
    )

    try:
        print("[Telegram] Connecting...")
        try:
            await asyncio.wait_for(client.connect(), timeout=60)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Telegram connection timed out after 60s. Check internet/VPN/proxy and try again.") from exc

        if not await client.is_user_authorized():
            print("[Telegram] Session is not authorized yet. Complete the login prompt.")
            await client.start()

        progress_cb = make_progress_printer("Uploading")
        t0 = time.time()

        uploaded = await fast_upload_file(
            client,
            stream,
            file_size=file_size,
            file_name=filename,
            part_size_kb=TG_UPLOAD_PART_SIZE_KB,
            connection_count=TG_UPLOAD_CONNECTIONS,
            read_chunk_size=TG_UPLOAD_READ_CHUNK_KB * 1024,
            progress_callback=progress_cb,
        )

        t1 = time.time()

        is_video = mime_type.startswith("video/")
        await send_file_safe(
            client,
            target_chat,
            uploaded,
            caption=filename,
            as_video=is_video,
        )

        avg = (file_size / (t1 - t0)) / (1024 * 1024)
        print(f"\nUpload benchmark avg: {avg:.2f} MB/s")
    finally:
        await client.disconnect()
        stream.close()

    print("✅ Done.")


if __name__ == "__main__":
    cli_args = parse_args()
    try:
        asyncio.run(main(cli_args))
    except KeyboardInterrupt:
        print("\nCancelled by user.")
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
