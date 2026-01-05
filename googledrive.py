import csv
import os
import re
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
OUTPUT_DIR = "movies_csv"

MOVIE_EXTENSIONS = (
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== AUTH (GMAIL OAUTH) =====
creds = None
if os.path.exists("token.pickle"):
    with open("token.pickle", "rb") as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            "credentials.json", SCOPES
        )
        creds = flow.run_local_server(port=0)

    with open("token.pickle", "wb") as token:
        pickle.dump(creds, token)

service = build("drive", "v3", credentials=creds)

# ===== LIST SHARED DRIVES =====
drives = service.drives().list().execute()["drives"]

print("\nAvailable Shared Drives:")
for d in drives:
    print(f"{d['name']}  -->  {d['id']}")

SHARED_DRIVE_ID = input("\nPaste the Shared Drive ID and press Enter: ")

# ===== SAFE FILE NAME =====
def safe_name(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

# ===== GET FOLDERS =====
folders = {}
page_token = None

while True:
    res = service.files().list(
        corpora="drive",
        driveId=SHARED_DRIVE_ID,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        q="mimeType='application/vnd.google-apps.folder'",
        fields="nextPageToken, files(id, name)",
        pageSize=1000,
        pageToken=page_token
    ).execute()

    for f in res.get("files", []):
        folders[f["id"]] = f["name"]

    page_token = res.get("nextPageToken")
    if not page_token:
        break

# ===== EXPORT MOVIES =====
for folder_id, folder_name in folders.items():
    movies = []
    page_token = None

    while True:
        res = service.files().list(
            corpora="drive",
            driveId=SHARED_DRIVE_ID,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            q=f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
            fields="nextPageToken, files(name)",
            pageSize=1000,
            pageToken=page_token
        ).execute()

        for file in res.get("files", []):
            if file["name"].lower().endswith(MOVIE_EXTENSIONS):
                movies.append(file["name"])

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    if movies:
        csv_path = f"{OUTPUT_DIR}/{safe_name(folder_name)}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["MovieFile"])
            for m in sorted(movies):
                writer.writerow([m])

        print(f"✅ Created: {csv_path}")

print("\n🎬 DONE! CSV files created.")
