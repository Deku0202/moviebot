# GoogleDrive-to-Telegram

Public CLI tool to upload files from Google Drive to Telegram.

## Features

- Supports **all file types**
  - videos are sent as streamable media
  - everything else is sent as document
- Browse Drive from:
  - `all` (My Drive + Shared Drives)
  - `my`
  - `shared` (specific Shared Drive ID)
- Interactive wizard flow and non-interactive CLI mode
- Fast upload path via Telethon + parallel upload helper

## Requirements

- Python 3.11+
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)
- Google Drive credentials JSON (`credentials.json`)

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Fill `.env` values:

- `TG_API_ID`
- `TG_API_HASH`
- `TG_TARGET` (optional, can also pass `--target`)

Put Google OAuth/service account file at `credentials.json` (or change `GOOGLE_CREDS_FILE` in `.env`).

## Usage

Interactive:

```bash
source .venv/bin/activate
python uploader.py
```

Direct upload by file ID (non-interactive):

```bash
python uploader.py --file-id <GOOGLE_FILE_ID> --target -1001234567890 --non-interactive
```

Pick newest matching file from a folder (non-interactive):

```bash
python uploader.py \
  --scope all \
  --folder-id <GOOGLE_FOLDER_ID> \
  --file-filter "conjuring" \
  --first-match \
  --non-interactive
```

Specific shared drive scope:

```bash
python uploader.py --scope shared --shared-drive-id <SHARED_DRIVE_ID>
```

CLI help:

```bash
python uploader.py --help
```

## Environment Variables

See `.env.example` for full list, including:

- auth/session: `TG_API_ID`, `TG_API_HASH`, `TG_SESSION`, `TG_TARGET`
- safety: `TG_ALLOWED_TARGETS`, `DAILY_SEND_LIMIT`
- tuning: `TG_UPLOAD_PART_SIZE_KB`, `TG_UPLOAD_CONNECTIONS`, `DRIVE_STREAM_CHUNK_MB`

## Public GitHub Safety

- Keep these out of git:
  - `.env`
  - `credentials.json`
  - `token.json`
  - `token.pickle`
  - `telethon.session*`
- Rotate credentials if they were ever exposed.

## Troubleshooting

- If `python` command is missing, use `python3` or `python3.11`.
- If Telegram connect hangs, check VPN/firewall and rerun.
- If your venv points to another project path, recreate it:

```bash
rm -rf .venv
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```
