import os
import json
import threading
import time
import sqlite3
import io
import requests
from pathlib import Path
from contextlib import asynccontextmanager
from functools import lru_cache

# Thư viện FastAPI (Nhanh hơn Flask)
# Cài đặt: pip install fastapi uvicorn requests
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ================= 1. CẤU HÌNH (ĐÃ ĐIỀN THÔNG TIN CỦA BẠN) =================
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GIST_ID_BUNPRO = "1e5f68fe9035be83b832ce0d2d98c865"
GIST_ID_WK = "d505fa51844600a57a846ee1641be2e1"

# ================= 2. FILE PATHS =================
AUDIO_DB = "bunpro_audio.db"
DIR_GEN = Path("bunpro_media_final_v2")
DIR_BULK = Path("bunpro_audio_opus")
DB_FILE = "anki_state.json"
WK_DB_FILE = "wanikani_state.json"

# ================= 3. SQLITE TỐI ƯU (WAL MODE & CONNECTION POOL) =================
def get_db_connection():
    try:
        # check_same_thread=False: Cho phép FastAPI (đa luồng) dùng chung connection
        conn = sqlite3.connect(f"file:{AUDIO_DB}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return None

# Khởi tạo connection toàn cục
db_conn = None
if os.path.exists(AUDIO_DB):
    print("🚀 [Media] SQLite detected. Enabling WAL mode for ULTIMATE SPEED...")
    # Hack: Mở kết nối tạm để bật chế độ Write-Ahead Logging (Đọc siêu nhanh)
    try:
        tmp_conn = sqlite3.connect(AUDIO_DB)
        tmp_conn.execute("PRAGMA journal_mode=WAL;") 
        tmp_conn.execute("PRAGMA synchronous=NORMAL;")
        tmp_conn.execute("PRAGMA cache_size=-64000;") # Dùng 64MB RAM cho cache SQLite
        tmp_conn.close()
    except:
        pass
    db_conn = get_db_connection()

# --- LRU CACHE (RAM CACHE) ---
# Lưu 500 file âm thanh gần nhất trực tiếp vào RAM.
# Lần truy cập thứ 2 sẽ tốn 0.00001 giây.
@lru_cache(maxsize=500)
def get_audio_blob_from_db(filename):
    if not db_conn: return None
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT data FROM media WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        
        # Smart Fallback (Nếu không thấy tên chính xác, thử đổi đuôi)
        if not row:
            stem = Path(filename).stem
            for ext in ['.opus', '.ogg', '.mp3', '.wav']:
                cursor.execute("SELECT data FROM media WHERE filename = ?", (f"{stem}{ext}",))
                row = cursor.fetchone()
                if row: break
        
        return row[0] if row else None
    except:
        return None

# ================= 4. GIST SYNC LOGIC =================
def download_from_gist(gist_id, local_filename):
    try:
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers, timeout=10)
        if r.status_code == 200:
            files = r.json()['files']
            # Lấy file đầu tiên trong Gist
            filename = list(files.keys())[0]
            content = files[filename]['content']
            with open(local_filename, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"☁️ [Cloud] Downloaded {local_filename} OK.")
        else:
            print(f"⚠️ [Cloud] Download Fail {local_filename}: {r.status_code}")
    except Exception as e:
        print(f"❌ [Cloud] Download Error: {e}")

def upload_to_gist(gist_id, local_filename):
    try:
        if not os.path.exists(local_filename): return
        with open(local_filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        remote_filename = os.path.basename(local_filename)
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json"
        }
        data = {"files": {remote_filename: {"content": content}}}
        
        requests.patch(f"https://api.github.com/gists/{gist_id}", headers=headers, json=data, timeout=10)
        print(f"☁️ [Cloud] Uploaded {local_filename} OK.")
    except Exception as e:
        print(f"❌ [Cloud] Upload Error: {e}")

# Các hàm wrapper để chạy sync
def sync_at_start():
    print("🔄 Initializing Cloud Sync...")
    download_from_gist(GIST_ID_BUNPRO, DB_FILE)
    download_from_gist(GIST_ID_WK, WK_DB_FILE)

def sync_at_exit():
    print("🔄 Shutting down... Uploading to Cloud...")
    upload_to_gist(GIST_ID_BUNPRO, DB_FILE)
    upload_to_gist(GIST_ID_WK, WK_DB_FILE)

def auto_save_periodic():
    while True:
        time.sleep(300) # 5 phút
        print("⏰ Auto-saving to Cloud...")
        upload_to_gist(GIST_ID_BUNPRO, DB_FILE)
        upload_to_gist(GIST_ID_WK, WK_DB_FILE)

# ================= 5. FASTAPI APP & LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP EVENT ---
    sync_at_start()
    # Chạy thread save định kỳ
    t = threading.Thread(target=auto_save_periodic, daemon=True)
    t.start()
    
    yield # Server hoạt động tại đây
    
    # --- SHUTDOWN EVENT ---
    sync_at_exit()

app = FastAPI(lifespan=lifespan)

# Cấu hình CORS để Anki (WebView) truy cập được
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helpers đọc/ghi JSON local
def load_json(path):
    if not os.path.exists(path): return {}
    with open(path, 'r', encoding='utf-8') as f: return json.load(f)

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2)

# ================= 6. API ENDPOINTS (HIGH PERFORMANCE) =================

@app.get("/media/{filename}")
def serve_audio(filename: str):
    # 1. KIỂM TRA RAM CACHE & DB
    # Đây là bước nhanh nhất, tốn < 1ms
    blob = get_audio_blob_from_db(filename)
    
    if blob:
        # ⚡ HARDCODE EFFECT ⚡
        # Header "Cache-Control: immutable" bảo trình duyệt/Anki lưu file này mãi mãi.
        # Lần sau Anki sẽ tự lấy từ cache của nó, KHÔNG CẦN gọi server nữa.
        return Response(
            content=blob, 
            media_type="audio/ogg", 
            headers={"Cache-Control": "public, max-age=31536000, immutable"}
        )

    # 2. FALLBACK: TÌM TRONG FOLDER (Chậm hơn chút)
    file_path = None
    if (DIR_GEN / filename).exists(): file_path = DIR_GEN / filename
    elif (DIR_BULK / filename).exists(): file_path = DIR_BULK / filename
    else:
        # Fallback tìm các đuôi khác
        stem = Path(filename).stem
        for ext in ['.ogg', '.mp3', '.wav', '.opus']:
            if (DIR_GEN / f"{stem}{ext}").exists(): 
                file_path = DIR_GEN / f"{stem}{ext}"
                break
            if (DIR_BULK / f"{stem}{ext}").exists():
                file_path = DIR_BULK / f"{stem}{ext}"
                break
    
    if file_path:
        with open(file_path, "rb") as f:
            content = f.read()
        return Response(content=content, media_type="audio/ogg")
        
    return Response(status_code=404)

# --- BUNPRO STATE ---
@app.get("/state/{word_key}")
def get_state(word_key: str):
    db = load_json(DB_FILE)
    return {"index": db.get(word_key, 0)}

@app.post("/state/{word_key}/next")
def next_state(word_key: str):
    db = load_json(DB_FILE)
    db[word_key] = db.get(word_key, 0) + 1
    save_json(DB_FILE, db)
    return {"index": db[word_key]}

# --- WANIKANI STATE ---
@app.get("/wk/state/{word_key}")
def get_wk_state(word_key: str):
    db = load_json(WK_DB_FILE)
    return {"index": db.get(word_key, 0)}

@app.post("/wk/state/{word_key}/next")
def next_wk_state(word_key: str):
    db = load_json(WK_DB_FILE)
    db[word_key] = db.get(word_key, 0) + 1
    save_json(WK_DB_FILE, db)
    return {"index": db[word_key]}

# ================= 7. SERVER RUNNER =================
if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 ANKI MEDIA SERVER - ULTIMATE SPEED EDITION")
    print("⚡ Powered by FastAPI + Uvicorn + SQLite WAL + RAM Cache")
    print(f"📁 Database: {AUDIO_DB}")
    print(f"☁️ Cloud Sync: ACTIVE (GitHub Gist)")
    print("="*50 + "\n")
    
    # Chạy server với Uvicorn (Production Grade Server)
    # log_level="error" để ẩn bớt log rác, tối ưu tốc độ
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")