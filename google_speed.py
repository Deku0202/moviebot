import time
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.credentials import Credentials

SCOPES=["https://www.googleapis.com/auth/drive.readonly"]

file_id = input("file_id: ").strip()
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
sess = AuthorizedSession(creds)

url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
r = sess.get(url, stream=True, timeout=300)
r.raise_for_status()

t0 = time.time()
n = 0
last_print = t0

for chunk in r.iter_content(chunk_size=64*1024*1024):
    if not chunk:
        continue
    n += len(chunk)
    now = time.time()
    if now - last_print >= 1.0:
        dt = now - t0
        speed = (n / dt) / (1024*1024)
        print(f"\r{n/1024/1024:.1f} MB  {speed:.2f} MB/s", end="")
        last_print = now

dt = time.time() - t0
speed = (n / max(dt, 1e-6)) / (1024*1024)
print(f"\r{n/1024/1024:.1f} MB  {speed:.2f} MB/s")
print("Done")
