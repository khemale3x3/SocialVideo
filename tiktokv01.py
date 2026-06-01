import os
import json
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import threading
import random
import queue
from tqdm import tqdm
import re
import hashlib
import shutil
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ==================== CONFIGURATION ====================
class ScraperConfig:
    def __init__(
        self,
        SESSION_IDS,
        MAX_WORKERS=3,
        MAX_VIDEOS=40,
        HEADLESS=True,
        INPUT_FILE="input.csv",
        DONE_FILE="inputdone.csv",
        OUTPUT_DIR="output",
        TEST_MODE=False,
        MAX_TEST_PROFILES=5,
        SCRAPE_INDIVIDUAL_VIDEOS=True,
        DOWNLOAD_VIDEOS=True,
        DOWNLOAD_THUMBNAILS=False,
        HD_QUALITY=True,
        SAVE_METADATA_JSON=True,
        SAVE_SUMMARY_CSV=True,
        RETRY_FAILED=True,
        MAX_RETRIES=3,
    ):
        self.SESSION_IDS = SESSION_IDS
        self.MAX_WORKERS = MAX_WORKERS
        self.MAX_VIDEOS = MAX_VIDEOS
        self.HEADLESS = HEADLESS
        self.INPUT_FILE = INPUT_FILE
        self.DONE_FILE = DONE_FILE
        self.OUTPUT_DIR = OUTPUT_DIR
        self.TEST_MODE = TEST_MODE
        self.MAX_TEST_PROFILES = MAX_TEST_PROFILES
        self.SCRAPE_INDIVIDUAL_VIDEOS = SCRAPE_INDIVIDUAL_VIDEOS
        self.DOWNLOAD_VIDEOS = DOWNLOAD_VIDEOS
        self.DOWNLOAD_THUMBNAILS = DOWNLOAD_THUMBNAILS
        self.HD_QUALITY = HD_QUALITY
        self.SAVE_METADATA_JSON = SAVE_METADATA_JSON
        self.SAVE_SUMMARY_CSV = SAVE_SUMMARY_CSV
        self.RETRY_FAILED = RETRY_FAILED
        self.MAX_RETRIES = MAX_RETRIES

        self.TARGET_ENDPOINTS = [
            "/api/post/item_list/",
            "/api/user/detail/",
            "/api/recommend/item_list/",
            "/api/item/detail/",
            "/node/share/user/",
            "/@",
            "/api/comment/list/",
            "/api/challenge/detail/",
        ]

        self.NETWORK_TIMEOUT = 180
        self.PAGE_LOAD_TIMEOUT = 60
        self.SCROLL_DELAY = 2

# ==================== FOLDER STRUCTURE ====================
#
#  output/
#  └── @username/
#      ├── profile/
#      │   ├── userInfo.json
#      │   └── <username>.jpg
#      ├── videos/
#      │   ├── <video_id>.mp4
#      │   └── <video_id>.json
#      ├── videoInfo.json
#      └── summary.csv
#
# ==================== GLOBAL STATE ====================

stats_lock = threading.Lock()
session_id_lock = threading.Lock()
file_write_lock = threading.Lock()
processed_videos_lock = threading.Lock()

session_id_index = 0
all_processed_video_ids = set()

# ==================== LOGGING ====================

def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "INFO":    "\033[94m",
        "SUCCESS": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR":   "\033[91m",
    }
    icons = {"INFO": "ℹ", "SUCCESS": "✓", "WARNING": "⚠", "ERROR": "✗"}
    print(f"{colors.get(level, '')}{icons.get(level, '*')} [{timestamp}] {message}\033[0m")

# ==================== SESSION ROTATION ====================

def get_next_session_id(config):
    global session_id_index
    with session_id_lock:
        sid = config.SESSION_IDS[session_id_index % len(config.SESSION_IDS)]
        session_id_index += 1
        return sid

# ==================== DRIVER SETUP ====================

def configure_driver(session_id, config):
    options = webdriver.ChromeOptions()
    if config.HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--autoplay-policy=user-gesture-required")
    options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.media_stream_mic": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.automatic_downloads": 2,
    })
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(config.PAGE_LOAD_TIMEOUT)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    driver.execute_cdp_cmd("Network.enable", {})

    if session_id:
        try:
            driver.get("https://www.tiktok.com/")
            time.sleep(2)
            driver.add_cookie({
                "name": "sessionid",
                "value": session_id,
                "domain": ".tiktok.com",
                "path": "/",
                "secure": True,
                "httpOnly": True,
            })
            log_message("Session cookie set")
        except Exception as e:
            log_message(f"Cookie error: {e}", level="WARNING")
    
    # Also capture any session that may be set by initial page load
    time.sleep(1)
    return driver


def get_driver_cookies(driver):
    """
    Extract all cookies from the driver as a dict suitable for requests.
    Captures sessionid, tt_chain_token, ttwid, and other auth cookies
    that TikTok requires for signed download URLs.
    """
    try:
        cookies = {}
        for cookie in driver.get_cookies():
            cookies[cookie["name"]] = cookie["value"]
        log_message(f"Captured {len(cookies)} cookies: {list(cookies.keys())}", level="INFO")
        return cookies
    except Exception as e:
        log_message(f"get_driver_cookies error: {e}", level="WARNING")
        return {}


def validate_driver_session(driver, config):
    """Test if the driver can access TikTok and validate session."""
    try:
        driver.get("https://www.tiktok.com/explore")
        time.sleep(3)
        
        # Check page title
        title = driver.title
        log_message(f"Session test - Page title: {title}", level="INFO")
        
        # Get cookies
        cookies = get_driver_cookies(driver)
        if not cookies:
            log_message("Warning: No cookies found after page load", level="WARNING")
        
        return True
    except Exception as e:
        log_message(f"Session validation failed: {e}", level="ERROR")
        return False

# ==================== URL / ID HELPERS ====================

def get_username(url):
    url = url.strip().rstrip("/")
    if "@" in url:
        return url.split("@")[-1].split("?")[0].split("/")[0]
    return url.split("/")[-1]

def extract_video_id(url):
    match = re.search(r"/video/(\d+)", url)
    return match.group(1) if match else None

def generate_video_hash(video_data):
    unique_string = (
        f"{video_data.get('id','')}_"
        f"{video_data.get('author',{}).get('id','')}_"
        f"{video_data.get('createTime','')}"
    )
    return hashlib.sha256(unique_string.encode()).hexdigest()

# ==================== FOLDER STRUCTURE HELPERS ====================

def make_profile_dirs(username, config):
    """Create and return the folder structure for a user."""
    base = Path(config.OUTPUT_DIR) / f"@{username}"
    dirs = {
        "base":    base,
        "profile": base / "profile",
        "videos":  base / "videos",
    }
    if config.DOWNLOAD_THUMBNAILS:
        dirs["thumbnails"] = base / "thumbnails"
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs

def video_file_paths(dirs, video_id):
    """Return file paths for a video and its metadata in the videos folder."""
    video_path = dirs["videos"] / f"{video_id}.mp4"
    meta_path  = dirs["videos"] / f"{video_id}.json"
    return video_path, meta_path

# ==================== DOWNLOAD HELPERS ====================

def build_download_headers(referer_url=None):
    """
    Build headers that mimic a real browser video request.
    The Range header is required — TikTok CDN returns 403 without it.
    """
    return {
        "Accept":             "video/webm,video/mp4,video/*;q=0.9,*/*;q=0.8",
        "Accept-Encoding":    "identity;q=1, *;q=0",
        "Accept-Language":    "en-US,en;q=0.9",
        "Cache-Control":      "no-cache",
        "Connection":         "keep-alive",
        "Pragma":             "no-cache",
        # Range is critical — TikTok CDN requires it for video downloads
        "Range":              "bytes=0-",
        "Referer":            referer_url or "https://www.tiktok.com/",
        "Sec-Fetch-Dest":     "video",
        "Sec-Fetch-Mode":     "no-cors",
        "Sec-Fetch-Site":     "cross-site",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }


def download_file(url, filepath, retries=3, chunk_size=1 << 17,
                  cookies=None, referer_url=None):
    """
    Download a file with retry logic.

    Key fixes vs original:
    - Adds Range: bytes=0- header (required by TikTok CDN)
    - Accepts HTTP 206 Partial Content as success
    - Passes all browser cookies (sessionid, tt_chain_token, ttwid, etc.)
    - Uses a proper Referer matching the video page
    - Retries with exponential back-off
    """
    headers  = build_download_headers(referer_url=referer_url)
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        try:
            with requests.get(
                url,
                headers=headers,
                cookies=cookies or {},
                timeout=120,
                stream=True,
                allow_redirects=True,
            ) as r:
                # 206 Partial Content is the normal success code when Range is sent
                if r.status_code not in (200, 206):
                    raise requests.HTTPError(
                        f"HTTP {r.status_code} for {url}", response=r
                    )
                content_type = r.headers.get("Content-Type", "")
                if "text/html" in content_type or "application/json" in content_type:
                    raise ValueError(
                        f"Got non-video content-type '{content_type}' — "
                        "URL may have expired or cookies are invalid"
                    )
                tmp = filepath.with_suffix(".tmp")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                # Sanity check: reject suspiciously small files (< 10 KB)
                if tmp.stat().st_size < 10_240:
                    tmp.unlink(missing_ok=True)
                    raise ValueError(
                        f"Downloaded file too small ({tmp.stat().st_size if tmp.exists() else 0} B) "
                        "— likely an error page"
                    )
                tmp.rename(filepath)
                return True
        except Exception as e:
            log_message(
                f"Download attempt {attempt}/{retries} failed ({filepath.name}): {e}",
                level="WARNING",
            )
            if attempt < retries:
                time.sleep(2 ** attempt)
    return False


def select_best_image_url(candidates):
    order = ["larger", "large", "origin", "medium", "thumb"]
    for hint in order:
        for url, q in candidates:
            if url and hint in q.lower():
                return url
    for url, _ in candidates:
        if url:
            return url
    return None


def select_best_video_url(video_meta, prefer_hd=True):
    """
    Return the highest-quality playable video URL.

    Priority order:
    1. bitrateInfo list sorted by bitrate (highest first) — watermark-free HD
    2. downloadAddr — usually watermark-free but may be lower quality
    3. playAddr — watermarked fallback

    Each URL list may contain multiple CDN mirrors; we try the first one.
    """
    if prefer_hd:
        bitrate_info = video_meta.get("bitrateInfo", [])
        if bitrate_info:
            sorted_info = sorted(
                bitrate_info,
                key=lambda x: x.get("Bitrate", 0),
                reverse=True,
            )
            for entry in sorted_info:
                play_addr = entry.get("PlayAddr", {})
                url_list  = play_addr.get("UrlList", [])
                if url_list:
                    # Prefer the mirror that doesn't have 'watermark' in the path
                    non_wm = [u for u in url_list if "watermark" not in u.lower()]
                    return non_wm[0] if non_wm else url_list[0]

    # Prefer downloadAddr (watermark-free) over playAddr (watermarked)
    return (
        video_meta.get("downloadAddr")
        or video_meta.get("playAddr")
        or None
    )

# ==================== DATA EXTRACTION ====================

def extract_complete_video_data(video_item):
    """Extract all fields from a raw video item dict."""
    try:
        video_meta = video_item.get("video", {})

        data = {
            # ---- Identity ----
            "id":            video_item.get("id"),
            "desc":          video_item.get("desc", ""),
            "createTime":    video_item.get("createTime"),
            "createTimeISO": (
                datetime.fromtimestamp(video_item["createTime"]).isoformat()
                if video_item.get("createTime") else None
            ),

            # ---- Author ----
            "author": {
                "id":             video_item.get("author", {}).get("id"),
                "uniqueId":       video_item.get("author", {}).get("uniqueId"),
                "nickname":       video_item.get("author", {}).get("nickname"),
                "avatarThumb":    video_item.get("author", {}).get("avatarThumb"),
                "avatarMedium":   video_item.get("author", {}).get("avatarMedium"),
                "avatarLarger":   video_item.get("author", {}).get("avatarLarger"),
                "signature":      video_item.get("author", {}).get("signature"),
                "verified":       video_item.get("author", {}).get("verified", False),
                "privateAccount": video_item.get("author", {}).get("privateAccount", False),
                "region":         video_item.get("author", {}).get("region"),
                "secUid":         video_item.get("author", {}).get("secUid"),
            },

            # ---- Author stats ----
            "authorStats": {
                "followerCount":  video_item.get("authorStats", {}).get("followerCount", 0),
                "followingCount": video_item.get("authorStats", {}).get("followingCount", 0),
                "heartCount":     video_item.get("authorStats", {}).get("heartCount", 0),
                "videoCount":     video_item.get("authorStats", {}).get("videoCount", 0),
                "diggCount":      video_item.get("authorStats", {}).get("diggCount", 0),
            },

            # ---- Video stats ----
            "stats": {
                "diggCount":    video_item.get("stats", {}).get("diggCount", 0),
                "shareCount":   video_item.get("stats", {}).get("shareCount", 0),
                "commentCount": video_item.get("stats", {}).get("commentCount", 0),
                "playCount":    video_item.get("stats", {}).get("playCount", 0),
                "collectCount": video_item.get("stats", {}).get("collectCount", 0),
            },

            # ---- Video technical ----
            "video": {
                "id":           video_meta.get("id"),
                "height":       video_meta.get("height"),
                "width":        video_meta.get("width"),
                "duration":     video_meta.get("duration"),
                "ratio":        video_meta.get("ratio"),
                "format":       video_meta.get("format"),
                "bitrate":      video_meta.get("bitrate"),
                "cover":        video_meta.get("cover"),
                "originCover":  video_meta.get("originCover"),
                "dynamicCover": video_meta.get("dynamicCover"),
                "playAddr":     video_meta.get("playAddr"),
                "downloadAddr": video_meta.get("downloadAddr"),
                "bitrateInfo":  video_meta.get("bitrateInfo", []),
            },

            # ---- Music ----
            "music": {
                "id":          video_item.get("music", {}).get("id"),
                "title":       video_item.get("music", {}).get("title"),
                "authorName":  video_item.get("music", {}).get("authorName"),
                "original":    video_item.get("music", {}).get("original", False),
                "duration":    video_item.get("music", {}).get("duration"),
                "playUrl":     video_item.get("music", {}).get("playUrl"),
                "coverLarge":  video_item.get("music", {}).get("coverLarge"),
                "coverMedium": video_item.get("music", {}).get("coverMedium"),
                "coverThumb":  video_item.get("music", {}).get("coverThumb"),
            },

            # ---- Hashtags / challenges ----
            "challenges": [
                {
                    "id":         c.get("id"),
                    "title":      c.get("title"),
                    "desc":       c.get("desc"),
                    "isCommerce": c.get("isCommerce", False),
                }
                for c in video_item.get("challenges", [])
            ],

            # ---- Text extras ----
            "textExtra": video_item.get("textExtra", []),

            # ---- Stickers / effects ----
            "stickersOnItem": [
                {
                    "stickerType": s.get("stickerType"),
                    "stickerText": s.get("stickerText", []),
                }
                for s in video_item.get("stickersOnItem", [])
            ],

            # ---- Flags ----
            "isAd":          video_item.get("isAd", False),
            "isPinned":      video_item.get("isPinned", False),
            "secret":        video_item.get("secret", False),
            "duetEnabled":   video_item.get("duetEnabled", True),
            "stitchEnabled": video_item.get("stitchEnabled", True),
            "shareEnabled":  video_item.get("shareEnabled", True),

            # ---- Location ----
            "locationCreated": video_item.get("locationCreated"),

            # ---- Subtitle / warn ----
            "subtitleInfos": video_item.get("subtitleInfos", []),
            "warnInfo":      video_item.get("warnInfo", []),

            # ---- Computed ----
            "webVideoUrl": (
                f"https://www.tiktok.com/@"
                f"{video_item.get('author', {}).get('uniqueId')}/"
                f"video/{video_item.get('id')}"
                if video_item.get("id") and video_item.get("author", {}).get("uniqueId")
                else None
            ),
        }
        return data
    except Exception as e:
        log_message(f"extract_complete_video_data error: {e}", level="ERROR")
        return None


def extract_profile_data(profile_item):
    try:
        user  = profile_item.get("user", {})
        stats = profile_item.get("stats", {})
        return {
            "user": {
                "id":             user.get("id"),
                "uniqueId":       user.get("uniqueId"),
                "nickname":       user.get("nickname"),
                "avatarThumb":    user.get("avatarThumb"),
                "avatarMedium":   user.get("avatarMedium"),
                "avatarLarger":   user.get("avatarLarger"),
                "signature":      user.get("signature"),
                "verified":       user.get("verified", False),
                "privateAccount": user.get("privateAccount", False),
                "region":         user.get("region"),
                "bioLink":        user.get("bioLink", {}),
                "ttSeller":       user.get("ttSeller", False),
                "secUid":         user.get("secUid"),
            },
            "stats": {
                "followerCount":  stats.get("followerCount", 0),
                "followingCount": stats.get("followingCount", 0),
                "heartCount":     stats.get("heartCount", 0),
                "videoCount":     stats.get("videoCount", 0),
                "diggCount":      stats.get("diggCount", 0),
            },
            "scraped_at": datetime.now().isoformat(),
        }
    except Exception as e:
        log_message(f"extract_profile_data error: {e}", level="ERROR")
        return None


def extract_all_data(response_body):
    result = {"videos": [], "profile": None}
    if not isinstance(response_body, dict):
        return result

    try:
        # Try extracting videos from multiple possible locations
        items_found = 0
        
        # Method 1: Direct videoList from feed API
        if "videoList" in response_body and isinstance(response_body["videoList"], list):
            for item in response_body["videoList"]:
                v = extract_complete_video_data(item)
                if v:
                    result["videos"].append(v)
                    items_found += 1

        # Method 2: webapp.video-detail (individual video page)
        if "webapp.video-detail" in response_body:
            detail = response_body["webapp.video-detail"]
            item   = detail.get("itemInfo", {}).get("itemStruct")
            if item:
                v = extract_complete_video_data(item)
                if v:
                    result["videos"].append(v)
                    items_found += 1

        # Method 3: itemInfo.itemStruct
        if "itemInfo" in response_body:
            item = response_body["itemInfo"].get("itemStruct")
            if item:
                v = extract_complete_video_data(item)
                if v:
                    result["videos"].append(v)
                    items_found += 1

        # Method 4: itemList array
        if "itemList" in response_body and isinstance(response_body["itemList"], list):
            for item in response_body["itemList"]:
                v = extract_complete_video_data(item)
                if v:
                    result["videos"].append(v)
                    items_found += 1

        # Method 5: itemModule dict
        if "itemModule" in response_body and isinstance(response_body["itemModule"], dict):
            for item in response_body["itemModule"].values():
                v = extract_complete_video_data(item)
                if v:
                    result["videos"].append(v)
                    items_found += 1

        # Method 6: feedItems (alternative feed format)
        if "feedItems" in response_body and isinstance(response_body["feedItems"], list):
            for item in response_body["feedItems"]:
                video_item = item.get("itemInfo", {}).get("itemStruct")
                if video_item:
                    v = extract_complete_video_data(video_item)
                    if v:
                        result["videos"].append(v)
                        items_found += 1

        # Profile extraction
        if "userInfo" in response_body:
            result["profile"] = extract_profile_data(response_body["userInfo"])
        elif "userDetail" in response_body:
            result["profile"] = extract_profile_data(response_body["userDetail"])
        elif "user" in response_body and not result["profile"]:
            result["profile"] = extract_profile_data(response_body)

        if not result["profile"] and result["videos"]:
            fv = result["videos"][0]
            result["profile"] = {
                "user":       fv["author"],
                "stats":      fv.get("authorStats", {}),
                "scraped_at": datetime.now().isoformat(),
            }
        
        if items_found > 0:
            log_message(f"✓ Extracted {items_found} items from response", level="SUCCESS")

    except Exception as e:
        log_message(f"extract_all_data error: {e}", level="ERROR")

    return result

# ==================== NETWORK CAPTURE ====================

def capture_responses(driver, processed_ids):
    responses = []
    try:
        logs = driver.get_log("performance")
        log_count = len(logs)
        for log in logs:
            try:
                msg = json.loads(log["message"])["message"]
                if msg["method"] == "Network.responseReceived":
                    rid = msg["params"]["requestId"]
                    url = msg["params"]["response"]["url"]
                    if rid not in processed_ids:
                        responses.append(msg)
            except:
                continue
        if responses:
            log_message(f"📡 Captured {len(responses)} new responses from {log_count} perf logs", level="INFO")
    except Exception as e:
        log_message(f"capture_responses error: {e}", level="WARNING")
    return responses


def process_response(driver, response, config):
    try:
        request_id = response["params"]["requestId"]
        url        = response["params"]["response"]["url"]
        if not any(ep in url for ep in config.TARGET_ENDPOINTS):
            return None
        try:
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
            extracted = extract_all_data(json.loads(body["body"]))
            if extracted and (extracted.get("videos") or extracted.get("profile")):
                log_message(f"✓ Captured response: {len(extracted.get('videos', []))} videos, profile={'Yes' if extracted.get('profile') else 'No'}", level="INFO")
            return extracted
        except Exception as e:
            log_message(f"Network response body error for {url}: {str(e)[:100]}", level="WARNING")
            return None
    except Exception as e:
        log_message(f"process_response error: {str(e)[:100]}", level="WARNING")
        return None

# ==================== HD ASSET SAVING ====================

def save_profile_assets(username, profile_data, dirs, config, cookies=None):
    """Download HD avatar and save userInfo.json."""
    profile_dir = dirs["profile"]

    info_path = profile_dir / "userInfo.json"
    with file_write_lock:
        with open(info_path, "w", encoding="utf-8") as f:
            json.dump(profile_data, f, indent=2, ensure_ascii=False)

    user_info = profile_data.get("user", {})
    avatar_candidates = [
        (user_info.get("avatarLarger"), "larger"),
        (user_info.get("avatarMedium"), "medium"),
        (user_info.get("avatarThumb"),  "thumb"),
    ]
    avatar_url = select_best_image_url(avatar_candidates)
    if avatar_url:
        ext = "jpg" if ".jpg" in avatar_url.lower() else "png"
        download_file(
            avatar_url,
            profile_dir / f"{username}.{ext}",
            cookies=cookies,
        )


def save_video_assets(video_data, dirs, config, cookies=None):
    """
    Download HD video file and per-video metadata JSON.

    FIX: cookies (including tt_chain_token, ttwid, sessionid) are now
    passed in from the live driver so TikTok's signed URLs are accepted.
    FIX: Referer header is set to the actual video page URL.
    FIX: select_best_video_url prefers non-watermarked HD streams.
    """
    video_id   = video_data.get("id")
    video_meta = video_data.get("video", {})

    dirs["videos"].mkdir(parents=True, exist_ok=True)

    # ---- Metadata JSON ----
    if config.SAVE_METADATA_JSON:
        _, meta_path = video_file_paths(dirs, video_id)
        with file_write_lock:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)

    # ---- HD Video ----
    if config.DOWNLOAD_VIDEOS:
        vid_path, _ = video_file_paths(dirs, video_id)

        # Skip if already downloaded
        if vid_path.exists() and vid_path.stat().st_size > 10_240:
            log_message(f"Already downloaded: {video_id}", level="INFO")
            return

        video_url = select_best_video_url(video_meta, prefer_hd=config.HD_QUALITY)
        if not video_url:
            log_message(f"No video URL found for {video_id}", level="WARNING")
            return

        # Build referer from video web URL (helps pass TikTok's hotlink check)
        referer = video_data.get("webVideoUrl") or "https://www.tiktok.com/"

        success = download_file(
            video_url,
            vid_path,
            retries=config.MAX_RETRIES,
            cookies=cookies,
            referer_url=referer,
        )
        if success:
            log_message(f"Downloaded: {video_id}.mp4", level="SUCCESS")
        else:
            log_message(f"Video download failed: {video_id}", level="WARNING")


def save_summary_csv(username, videos_data, dirs):
    """Write a flat summary.csv with one row per video."""
    rows = []
    for v in videos_data:
        rows.append({
            "video_id":      v.get("id"),
            "create_time":   v.get("createTimeISO"),
            "desc":          v.get("desc", ""),
            "play_count":    v.get("stats", {}).get("playCount", 0),
            "like_count":    v.get("stats", {}).get("diggCount", 0),
            "comment_count": v.get("stats", {}).get("commentCount", 0),
            "share_count":   v.get("stats", {}).get("shareCount", 0),
            "collect_count": v.get("stats", {}).get("collectCount", 0),
            "duration_sec":  v.get("video", {}).get("duration"),
            "width":         v.get("video", {}).get("width"),
            "height":        v.get("video", {}).get("height"),
            "bitrate":       v.get("video", {}).get("bitrate"),
            "music_title":   v.get("music", {}).get("title"),
            "music_author":  v.get("music", {}).get("authorName"),
            "hashtags": " ".join(
                f"#{c['title']}" for c in v.get("challenges", []) if c.get("title")
            ),
            "location":  v.get("locationCreated"),
            "is_ad":     v.get("isAd", False),
            "is_pinned": v.get("isPinned", False),
            "web_url":   v.get("webVideoUrl"),
        })

    if rows:
        csv_path = dirs["base"] / "summary.csv"
        pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")


def save_videoinfo_json(username, videos_data, dirs):
    """Write consolidated videoInfo.json (all videos)."""
    payload = {
        "username":   username,
        "count":      len(videos_data),
        "scraped_at": datetime.now().isoformat(),
        "videos":     videos_data,
    }
    path = dirs["base"] / "videoInfo.json"
    with file_write_lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)


def update_user_files(username, profile_data, videos_data, config, dirs=None, cookies=None):
    """
    Central function: save everything for a user.
    cookies should be the full cookie dict from the live driver.
    """
    if dirs is None:
        dirs = make_profile_dirs(username, config)

    if profile_data:
        save_profile_assets(username, profile_data, dirs, config, cookies=cookies)

    if videos_data:
        threads = []
        for vd in videos_data:
            t = threading.Thread(
                target=save_video_assets,
                args=(vd, dirs, config),
                kwargs={"cookies": cookies},
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        save_videoinfo_json(username, videos_data, dirs)

        if config.SAVE_SUMMARY_CSV:
            save_summary_csv(username, videos_data, dirs)

    return True

# ==================== DONE FILE ====================

def append_to_done_file(url, config):
    try:
        with file_write_lock:
            if not os.path.exists(config.DONE_FILE):
                pd.DataFrame({"url": [], "processed_at": []}).to_csv(
                    config.DONE_FILE, index=False
                )
            df = pd.read_csv(config.DONE_FILE)
            
            # Avoid duplicates
            if url in df["url"].values:
                log_message(f"URL already in done file: {url[:50]}...", level="WARNING")
                return
                
            new_row = pd.DataFrame({
                "url":          [url],
                "processed_at": [datetime.now().isoformat()],
            })
            result_df = pd.concat([df, new_row], ignore_index=True)
            result_df.to_csv(config.DONE_FILE, index=False)
            log_message(f"✓ Added to done file: {url[:50]}...", level="SUCCESS")
    except Exception as e:
        log_message(f"Done-file error: {e}", level="WARNING")

# ==================== PROFILE SCRAPER ====================

def scrape_profile(driver, url, config, stats):
    username = get_username(url)
    log_message(f"Scraping: @{username}")

    dirs = make_profile_dirs(username, config)

    collected = {
        "profile":      None,
        "videos":       [],
        "video_ids":    set(),
        "video_hashes": set(),
    }

    try:
        driver.get(f"https://www.tiktok.com/@{username}")
        time.sleep(config.SCROLL_DELAY)
        
        # Check if page loaded successfully
        try:
            page_title = driver.title
            log_message(f"Loaded page: {page_title}", level="INFO")
        except:
            log_message(f"Warning: Could not get page title", level="WARNING")

        processed_req_ids = set()
        scroll_count  = 0
        no_new_count  = 0
        last_save_cnt = 0
        max_scrolls   = 50

        while scroll_count < max_scrolls and len(collected["videos"]) < config.MAX_VIDEOS:
            responses = capture_responses(driver, processed_req_ids)
            
            if not responses:
                log_message(f"ℹ [Scroll {scroll_count}] No responses captured", level="INFO")
            else:
                log_message(f"ℹ [Scroll {scroll_count}] Processing {len(responses)} responses", level="INFO")
            
            for response in responses:
                rid = response["params"]["requestId"]
                if rid in processed_req_ids:
                    continue
                processed_req_ids.add(rid)

                data = process_response(driver, response, config)
                if not data:
                    continue

                if data["profile"] and not collected["profile"]:
                    collected["profile"] = data["profile"]
                    log_message(f"✓ Profile captured: @{username}", level="SUCCESS")

                for video in data["videos"]:
                    vid_id = video.get("id")
                    if not vid_id:
                        continue
                    if vid_id in collected["video_ids"]:
                        continue
                    vh = generate_video_hash(video)
                    if vh in collected["video_hashes"]:
                        continue
                    with processed_videos_lock:
                        if vid_id in all_processed_video_ids:
                            continue
                        all_processed_video_ids.add(vid_id)

                    collected["videos"].append(video)
                    collected["video_ids"].add(vid_id)
                    collected["video_hashes"].add(vh)

            cur = len(collected["videos"])

            # Incremental save every 5 new videos — pass live cookies each time
            if cur >= last_save_cnt + 5:
                live_cookies = get_driver_cookies(driver)
                update_user_files(
                    username, collected["profile"], collected["videos"],
                    config, dirs, cookies=live_cookies,
                )
                log_message(f"💾 Saved {cur} videos so far", level="SUCCESS")
                last_save_cnt = cur

            if cur >= config.MAX_VIDEOS:
                log_message(f"Max videos ({config.MAX_VIDEOS}) reached")
                break

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(config.SCROLL_DELAY)
            scroll_count += 1

            if len(collected["videos"]) == cur:
                no_new_count += 1
                if no_new_count >= 5:
                    log_message(f"No new videos after {no_new_count} scrolls — stopping", level="WARNING")
                    break
            else:
                no_new_count = 0

        # Detailed per-video scrape
        if config.SCRAPE_INDIVIDUAL_VIDEOS and collected["videos"]:
            log_message(f"Scraping individual video pages for @{username}…")
            live_cookies = get_driver_cookies(driver)
            detailed = scrape_individual_videos(
                driver, username,
                collected["videos"][: config.MAX_VIDEOS],
                config,
            )
            if detailed:
                vid_map = {v.get("id"): v for v in detailed}
                for i, v in enumerate(collected["videos"]):
                    if v.get("id") in vid_map:
                        collected["videos"][i] = {**v, **vid_map[v["id"]]}

        # Final save — capture cookies right before the last download batch
        live_cookies = get_driver_cookies(driver)
        update_user_files(
            username, collected["profile"], collected["videos"],
            config, dirs, cookies=live_cookies,
        )
        
        # Move URL to done file
        append_to_done_file(url, config)
        log_message(f"✓ Moved {username} to done file", level="SUCCESS")

        with stats_lock:
            if collected["profile"]:
                stats["profiles_saved"] += 1
            stats["videos_saved"] += len(collected["videos"])

        log_message(
            f"✓ @{username} done | {len(collected['videos'])} videos",
            level="SUCCESS",
        )
        return True

    except Exception as e:
        log_message(f"Error @{username}: {e}", level="ERROR")
        return False


def scrape_individual_videos(driver, username, videos, config):
    detailed = []
    for idx, video in enumerate(videos, 1):
        vid_id = video.get("id")
        if not vid_id:
            continue
        try:
            driver.get(f"https://www.tiktok.com/@{username}/video/{vid_id}")
            time.sleep(2)
            proc = set()
            for resp in capture_responses(driver, proc):
                proc.add(resp["params"]["requestId"])
                data = process_response(driver, resp, config)
                if data and data["videos"]:
                    detailed.append(data["videos"][0])
                    log_message(f"  Detail {idx}/{len(videos)}: {vid_id}")
                    break
            time.sleep(1)
        except Exception as e:
            log_message(f"Individual video error {vid_id}: {e}", level="WARNING")
    return detailed

# ==================== WORKER / MAIN ====================

def worker_thread(url_queue, config, stats, progress_bar):
    sid    = get_next_session_id(config)
    driver = configure_driver(sid, config)
    try:
        # Validate session before starting
        if not validate_driver_session(driver, config):
            log_message(f"Session validation failed for worker", level="ERROR")
        
        while not url_queue.empty():
            try:
                url = url_queue.get(block=False)
                scrape_profile(driver, url, config, stats)
                progress_bar.update(1)
            except queue.Empty:
                break
            except Exception as e:
                log_message(f"Worker error: {e}", level="ERROR")
                progress_bar.update(1)
            finally:
                url_queue.task_done()
    finally:
        try:
            driver.quit()
        except:
            pass


def load_urls(config):
    try:
        if not os.path.exists(config.INPUT_FILE):
            log_message(f"Creating {config.INPUT_FILE}", level="WARNING")
            pd.DataFrame({"url": []}).to_csv(config.INPUT_FILE, index=False)
            return []

        df   = pd.read_csv(config.INPUT_FILE)
        urls = df["url"].dropna().tolist()

        if os.path.exists(config.DONE_FILE):
            done_urls = set(pd.read_csv(config.DONE_FILE)["url"].dropna().tolist())
            urls = [u for u in urls if u not in done_urls]
            log_message(f"Skipping {len(done_urls)} already-processed URLs")

        log_message(f"Loaded {len(urls)} URLs")
        return urls
    except Exception as e:
        log_message(f"load_urls error: {e}", level="ERROR")
        return []


def main(config):
    print("\n" + "=" * 70)
    print("🚀 TIKTOK SCRAPER v2 — HD DOWNLOADS + STRUCTURED OUTPUT")
    print("=" * 70)
    print(f"📂 Output folder  : {config.OUTPUT_DIR}/")
    print(f"🎬 Max videos     : {config.MAX_VIDEOS}")
    print(f"🔧 Workers        : {config.MAX_WORKERS}")
    print(f"🖼️  HD quality     : {'ON' if config.HD_QUALITY else 'OFF'}")
    print(f"📥 Download videos: {'ON' if config.DOWNLOAD_VIDEOS else 'OFF'}")
    print(f"🖼️  Thumbnails     : {'ON' if config.DOWNLOAD_THUMBNAILS else 'OFF'}")
    print(f"📝 Per-video JSON : {'ON' if config.SAVE_METADATA_JSON else 'OFF'}")
    print(f"📊 Summary CSV    : {'ON' if config.SAVE_SUMMARY_CSV else 'OFF'}")
    print(f"🧪 Test mode      : {'ON' if config.TEST_MODE else 'OFF'}")
    print("=" * 70)

    start  = time.time()
    stats  = {"total": 0, "profiles_saved": 0, "videos_saved": 0}
    urls   = load_urls(config)

    if config.TEST_MODE:
        urls = urls[: config.MAX_TEST_PROFILES]
        log_message(f"Test mode: {len(urls)} profiles only", level="WARNING")

    stats["total"] = len(urls)
    if not urls:
        log_message("No URLs to process!", level="WARNING")
        return

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(config.DONE_FILE):
        pd.DataFrame({"url": [], "processed_at": []}).to_csv(config.DONE_FILE, index=False)

    url_queue = queue.Queue()
    for u in urls:
        url_queue.put(u)

    threads = []
    with tqdm(total=len(urls), desc="Profiles", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        for _ in range(config.MAX_WORKERS):
            t = threading.Thread(target=worker_thread, args=(url_queue, config, stats, pbar))
            t.daemon = True
            t.start()
            threads.append(t)
            time.sleep(0.5)
        url_queue.join()

    for t in threads:
        t.join(timeout=30)

    elapsed = time.time() - start
    print("\n" + "=" * 70)
    print("✅ DONE")
    print("=" * 70)
    print(f"📁 Profiles saved : {stats['profiles_saved']}")
    print(f"🎬 Videos saved   : {stats['videos_saved']}")
    print(f"⏱️  Time elapsed   : {elapsed:.1f}s ({elapsed/60:.1f} min)")
    if stats["profiles_saved"] > 0:
        print(f"📊 Avg videos/prof: {stats['videos_saved']/stats['profiles_saved']:.1f}")
    print(f"📂 Output dir     : ./{config.OUTPUT_DIR}/")
    print("=" * 70 + "\n")

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    config = ScraperConfig(
        SESSION_IDS=[
            "04b22f2e89883f0e81f6e699bc3b3c70",
            # "YOUR_SESSION_ID_2",
        ],
        MAX_WORKERS=3,
        MAX_VIDEOS=60,
        HEADLESS=True,
        INPUT_FILE="tiktokinput.csv",
        DONE_FILE="tiktokdone.csv",
        OUTPUT_DIR="tiktokoutput",
        HD_QUALITY=True,
        DOWNLOAD_VIDEOS=True,
        DOWNLOAD_THUMBNAILS=False,
        SAVE_METADATA_JSON=True,
        SAVE_SUMMARY_CSV=True,
        RETRY_FAILED=True,
        MAX_RETRIES=3,
        SCRAPE_INDIVIDUAL_VIDEOS=True,
        TEST_MODE=False,
        MAX_TEST_PROFILES=3,
    )
    main(config)