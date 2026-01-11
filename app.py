import os
import json
import threading
import time
import sqlite3
import requests
from pathlib import Path
from contextlib import asynccontextmanager
from functools import lru_cache
from collections import defaultdict
import tempfile

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# ================= 1. CẤU HÌNH =================
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GIST_ID_BUNPRO = "1e5f68fe9035be83b832ce0d2d98c865"
GIST_ID_WK = "d505fa51844600a57a846ee1641be2e1"
GIST_ID_KAIWA = "38a1ab851c240b430a65fcc5feb9e055"

# ================= 2. FILE PATHS =================
AUDIO_DB = "bunpro_audio.db"
DIR_GEN = Path("bunpro_media_final_v2")
DIR_BULK = Path("bunpro_audio_opus")
DB_FILE = "anki_state.json"
WK_DB_FILE = "wanikani_state.json"

# KAIWA database
KAIWA_DB = "kaiwa_media.db"
KAIWA_BOOKMARKS_FILE = "kaiwa_bookmarks.json"

# ================= 3. IN-MEMORY DATABASE (ZERO DISK I/O) =================
class InMemoryDB:
    """
    🚀 TẤT CẢ ĐỌC/GHI Ở RAM - KHÔNG CÓ DISK DELAY
    Disk chỉ dùng để backup bất đồng bộ
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = defaultdict(int)  # RAM database
        self.lock = threading.RLock()  # Reentrant lock
        self.dirty = False  # Flag để biết có cần save không
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load lần đầu từ disk vào RAM"""
        try:
            if os.path.exists(self.filepath):
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        self.data.update(json.loads(content))
                print(f"✅ Loaded {self.filepath} → {len(self.data)} entries in RAM")
        except Exception as e:
            print(f"⚠️ Could not load {self.filepath}: {e}. Starting fresh.")
    
    def get(self, key):
        """ĐỌC TỪ RAM - INSTANT (< 0.001ms)"""
        with self.lock:
            return self.data.get(key, 0)
    
    def set(self, key, value):
        """GHI VÀO RAM - INSTANT (< 0.001ms)"""
        with self.lock:
            self.data[key] = value
            self.dirty = True  # Đánh dấu cần save
    
    def increment(self, key):
        """TĂNG GIÁ TRỊ - INSTANT"""
        with self.lock:
            self.data[key] = self.data.get(key, 0) + 1
            self.dirty = True
            return self.data[key]
    
    def save_to_disk_async(self):
        """
        Save bất đồng bộ - KHÔNG BLOCK API
        Chạy trong thread riêng, API vẫn xử lý bình thường
        """
        if not self.dirty:
            return  # Không có gì thay đổi, skip
        
        with self.lock:
            # Copy data để tránh lock lâu
            data_snapshot = dict(self.data)
            self.dirty = False
        
        # Ghi atomic (không block lock chính)
        try:
            temp_fd, temp_path = tempfile.mkstemp(suffix='.json', dir=os.path.dirname(self.filepath) or '.')
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                json.dump(data_snapshot, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            if os.name == 'nt' and os.path.exists(self.filepath):
                os.remove(self.filepath)
            os.rename(temp_path, self.filepath)
        except Exception as e:
            print(f"❌ Error saving {self.filepath}: {e}")
            try:
                os.unlink(temp_path)
            except:
                pass

# Khởi tạo 2 IN-MEMORY DB
bunpro_db = InMemoryDB(DB_FILE)
wk_db = InMemoryDB(WK_DB_FILE)

# ================= 4. BACKGROUND SAVER (Không ảnh hưởng performance) =================
class BackgroundSaver:
    """Thread chạy ngầm để save định kỳ"""
    def __init__(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
    
    def start(self):
        self.thread.start()
    
    def stop(self):
        self.running = False
    
    def _run(self):
        while self.running:
            time.sleep(10)  # Save mỗi 10 giây (có thể tăng lên 30-60s)
            
            # Save to disk (không block API)
            bunpro_db.save_to_disk_async()
            wk_db.save_to_disk_async()
            
            # Upload to cloud (mỗi 5 phút)
            if int(time.time()) % 300 < 15:  # Trong khoảng 15s đầu của mỗi 5 phút
                print("⏰ Auto-uploading to Cloud...")
                self._upload_to_cloud()
    
    def _upload_to_cloud(self):
        """Upload bất đồng bộ, không block"""
        try:
            # Bunpro
            if os.path.exists(DB_FILE):
                with open(DB_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                headers = {
                    "Authorization": f"token {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github+json"
                }
                data = {"files": {os.path.basename(DB_FILE): {"content": content}}}
                requests.patch(f"https://api.github.com/gists/{GIST_ID_BUNPRO}", 
                             headers=headers, json=data, timeout=10)
                print(f"☁️ Uploaded {DB_FILE}")
        except Exception as e:
            print(f"⚠️ Upload error (Bunpro): {e}")
        
        try:
            # WaniKani
            if os.path.exists(WK_DB_FILE):
                with open(WK_DB_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                headers = {
                    "Authorization": f"token {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github+json"
                }
                data = {"files": {os.path.basename(WK_DB_FILE): {"content": content}}}
                requests.patch(f"https://api.github.com/gists/{GIST_ID_WK}", 
                             headers=headers, json=data, timeout=10)
                print(f"☁️ Uploaded {WK_DB_FILE}")
        except Exception as e:
            print(f"⚠️ Upload error (WaniKani): {e}")
        
        try:
            # Kaiwa Bookmarks
            if os.path.exists(KAIWA_BOOKMARKS_FILE):
                with open(KAIWA_BOOKMARKS_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                headers = {
                    "Authorization": f"token {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github+json"
                }
                data = {"files": {os.path.basename(KAIWA_BOOKMARKS_FILE): {"content": content}}}
                requests.patch(f"https://api.github.com/gists/{GIST_ID_KAIWA}", 
                             headers=headers, json=data, timeout=10)
                print(f"☁️ Uploaded {KAIWA_BOOKMARKS_FILE}")
        except Exception as e:
            print(f"⚠️ Upload error (Kaiwa): {e}")

bg_saver = BackgroundSaver()

# ================= 5. STARTUP: DOWNLOAD FROM CLOUD =================
def download_from_gist(gist_id, local_filename):
    try:
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers, timeout=10)
        if r.status_code == 200:
            files = r.json()['files']
            filename = list(files.keys())[0]
            content = files[filename]['content']
            
            temp_fd, temp_path = tempfile.mkstemp(suffix='.json', dir=os.path.dirname(os.path.abspath(local_filename)) or '.')
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            if os.name == 'nt' and os.path.exists(local_filename):
                os.remove(local_filename)
            os.rename(temp_path, local_filename)
            
            print(f"☁️ Downloaded {local_filename}")
        else:
            print(f"⚠️ Download failed {local_filename}: {r.status_code}")
    except Exception as e:
        print(f"❌ Download error: {e}")

def sync_at_start():
    print("🔄 Downloading from Cloud...")
    download_from_gist(GIST_ID_BUNPRO, DB_FILE)
    download_from_gist(GIST_ID_WK, WK_DB_FILE)
    download_from_gist(GIST_ID_KAIWA, KAIWA_BOOKMARKS_FILE)
    
    # Reload vào RAM
    bunpro_db._load_from_disk()
    wk_db._load_from_disk()

def sync_at_exit():
    print("🔄 Final save before shutdown...")
    bg_saver.stop()
    bunpro_db.save_to_disk_async()
    wk_db.save_to_disk_async()
    time.sleep(0.5)  # Đợi ghi xong
    
    print("☁️ Uploading to Cloud...")
    bg_saver._upload_to_cloud()

# ================= 6. SQLITE (Giữ nguyên) =================
def get_db_connection():
    try:
        conn = sqlite3.connect(f"file:{AUDIO_DB}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return None

def get_kaiwa_db_connection():
    """Connection cho Kaiwa media DB"""
    try:
        conn = sqlite3.connect(f"file:{KAIWA_DB}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"❌ Kaiwa DB Connection Error: {e}")
        return None

db_conn = None
if os.path.exists(AUDIO_DB):
    print("🚀 SQLite WAL mode enabled")
    try:
        tmp_conn = sqlite3.connect(AUDIO_DB)
        tmp_conn.execute("PRAGMA journal_mode=WAL;")
        tmp_conn.execute("PRAGMA synchronous=NORMAL;")
        tmp_conn.execute("PRAGMA cache_size=-64000;")
        tmp_conn.close()
    except:
        pass
    db_conn = get_db_connection()

# Kaiwa DB connection
kaiwa_db_conn = None
if os.path.exists(KAIWA_DB):
    print("🎴 Kaiwa DB WAL mode enabled")
    try:
        tmp_conn = sqlite3.connect(KAIWA_DB)
        tmp_conn.execute("PRAGMA journal_mode=WAL;")
        tmp_conn.execute("PRAGMA synchronous=NORMAL;")
        tmp_conn.execute("PRAGMA cache_size=-64000;")
        tmp_conn.close()
    except:
        pass
    kaiwa_db_conn = get_kaiwa_db_connection()

@lru_cache(maxsize=500)
def get_audio_blob_from_db(filename):
    if not db_conn: return None
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT data FROM media WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        
        if not row:
            stem = Path(filename).stem
            for ext in ['.opus', '.ogg', '.mp3', '.wav']:
                cursor.execute("SELECT data FROM media WHERE filename = ?", (f"{stem}{ext}",))
                row = cursor.fetchone()
                if row: break
        
        return row[0] if row else None
    except:
        return None

@lru_cache(maxsize=1000)
def get_kaiwa_audio_from_db(episode_name, file_type, filename):
    """Fetch Kaiwa audio từ DB (cached)"""
    if not kaiwa_db_conn: return None
    try:
        cursor = kaiwa_db_conn.cursor()
        cursor.execute(
            "SELECT data FROM kaiwa_media WHERE episode_name = ? AND file_type = ? AND filename = ?",
            (episode_name, file_type, filename)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"❌ Kaiwa DB error: {e}")
        return None

# ================= 7. FASTAPI APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    sync_at_start()
    bg_saver.start()
    yield
    sync_at_exit()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= 8. API ENDPOINTS (ZERO DISK I/O) =================

@app.get("/media/{filename}")
def serve_audio(filename: str):
    blob = get_audio_blob_from_db(filename)
    
    if blob:
        return Response(
            content=blob,
            media_type="audio/ogg",
            headers={"Cache-Control": "public, max-age=31536000, immutable"}
        )

    file_path = None
    if (DIR_GEN / filename).exists(): 
        file_path = DIR_GEN / filename
    elif (DIR_BULK / filename).exists(): 
        file_path = DIR_BULK / filename
    else:
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

# --- BUNPRO STATE (INSTANT RAM ACCESS) ---
@app.get("/state/{word_key}")
def get_state(word_key: str):
    return {"index": bunpro_db.get(word_key)}

@app.post("/state/{word_key}/next")
def next_state(word_key: str):
    new_index = bunpro_db.increment(word_key)
    return {"index": new_index}

# --- WANIKANI STATE (INSTANT RAM ACCESS) ---
@app.get("/wk/state/{word_key}")
def get_wk_state(word_key: str):
    return {"index": wk_db.get(word_key)}

@app.post("/wk/state/{word_key}/next")
def next_wk_state(word_key: str):
    new_index = wk_db.increment(word_key)
    return {"index": new_index}

# ================= 10. KAIWA ENDPOINTS (DB VERSION) =================

@app.get("/kaiwa/audio/{episode_name}/{filename}")
def serve_kaiwa_audio(episode_name: str, filename: str):
    """Serve audio segments từ DB (cached, zero disk I/O)"""
    blob = get_kaiwa_audio_from_db(episode_name, "segment", filename)
    
    if blob:
        return Response(
            content=blob,
            media_type="audio/mpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable"}
        )
    
    return Response(status_code=404)

@app.get("/kaiwa/episode/{episode_name}/{filename}")
def serve_kaiwa_episode(episode_name: str, filename: str):
    """Serve full episode audio từ DB (cached)"""
    blob = get_kaiwa_audio_from_db(episode_name, "episode", filename)
    
    if blob:
        return Response(
            content=blob,
            media_type="audio/mpeg",
            headers={"Cache-Control": "public, max-age=3600"}
        )
    
    return Response(status_code=404)

# --- KAIWA BOOKMARKS (JSON FILE) ---
@app.get("/kaiwa/bookmarks/{episode_name}")
def get_bookmarks(episode_name: str):
    """Get bookmarks cho episode"""
    try:
        if os.path.exists(KAIWA_BOOKMARKS_FILE):
            with open(KAIWA_BOOKMARKS_FILE, 'r', encoding='utf-8') as f:
                all_bookmarks = json.load(f)
            return {"bookmarks": all_bookmarks.get(episode_name, [])}
        return {"bookmarks": []}
    except Exception as e:
        print(f"❌ Get bookmarks error: {e}")
        return {"bookmarks": []}

@app.post("/kaiwa/bookmarks/{episode_name}")
def save_bookmarks(episode_name: str, data: dict):
    """Save bookmarks cho episode"""
    try:
        bookmarks = data.get("bookmarks", [])
        
        # Load existing
        all_bookmarks = {}
        if os.path.exists(KAIWA_BOOKMARKS_FILE):
            with open(KAIWA_BOOKMARKS_FILE, 'r', encoding='utf-8') as f:
                all_bookmarks = json.load(f)
        
        # Update
        all_bookmarks[episode_name] = bookmarks
        
        # Save atomic
        temp_fd, temp_path = tempfile.mkstemp(suffix='.json', dir='.')
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            json.dump(all_bookmarks, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        
        if os.name == 'nt' and os.path.exists(KAIWA_BOOKMARKS_FILE):
            os.remove(KAIWA_BOOKMARKS_FILE)
        os.rename(temp_path, KAIWA_BOOKMARKS_FILE)
        
        return {"status": "ok"}
    except Exception as e:
        print(f"❌ Save bookmarks error: {e}")
        return {"status": "error", "message": str(e)}

# --- MANUAL SYNC TRIGGER (For Testing) ---
@app.post("/kaiwa/sync-cloud")
def manual_sync_cloud():
    """Manually trigger cloud sync (for testing)"""
    try:
        bg_saver._upload_to_cloud()
        return {"status": "ok", "message": "Cloud sync triggered"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ================= 9. SERVER RUNNER =================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 ANKI MEDIA SERVER - ZERO-DELAY IN-MEMORY EDITION")
    print("⚡ ALL DATA IN RAM - NO DISK I/O DURING REQUESTS")
    print(f"📁 Audio DB: {AUDIO_DB}")
    print(f"💾 State: {len(bunpro_db.data)} Bunpro + {len(wk_db.data)} WK entries")
    print(f"☁️ Cloud Sync: Every 5 minutes (background)")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")

    
    # Chạy server với Uvicorn (Production Grade Server)
    # log_level="error" để ẩn bớt log rác, tối ưu tốc độ
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
