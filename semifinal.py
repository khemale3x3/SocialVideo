import os
import json
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import threading
import random
import queue
from tqdm import tqdm
from pathlib import Path
import re

# ==================== CONFIGURATION ====================
class Config:
    SESSION_IDS = [
        "78524688621%3Aoqgv2pZ9cI7m0I%3A27%3AAYjWsANe7EObEu9XAEl0bxcO5VjpqyctHNOjdrOFdA",
        "79094843931%3A2LO8YBAJ0BPwsG%3A29%3AAYiqLMBzHL6R0qBgSBmRoXKgK3DIwlh3NmgDrWI_dw",
        "78639983601%3AXVwHy9YtHDyTs9%3A13%3AAYjxCxDr2QjIgk17tzGti1kdMQa-bmIwk4UNtc_mWQ",
    ]
    MAX_WORKERS     = 2       # Keep low to avoid rate limits
    MAX_VIDEOS      = 9999    # Effectively unlimited — get everything
    HEADLESS        = True
    INPUT_FILE      = "input.csv"
    DONE_FILE       = "inputdone.csv"
    TEST_MODE       = False
    MAX_TEST        = 1
    DOWNLOAD_REELS  = True
    DOWNLOAD_POSTS  = True
    MAX_SCROLLS     = 150     # Per section (timeline / reels / tagged)
    SCROLL_PAUSE    = 2.5     # Seconds between scrolls
    NO_NEW_LIMIT    = 8       # Stop after this many scrolls with no new posts

# ==================== GLOBALS ====================
stats_lock       = threading.Lock()
done_lock        = threading.Lock()
session_lock     = threading.Lock()
_session_idx     = 0

# ==================== LOGGING ====================
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    sym = {"INFO": "·", "SUCCESS": "✓", "WARNING": "!", "ERROR": "✗"}.get(level, "·")
    colors = {"INFO": "\033[94m", "SUCCESS": "\033[92m", "WARNING": "\033[93m", "ERROR": "\033[91m"}
    c = colors.get(level, "\033[0m")
    try:
        print(f"{c}[{ts}] [{sym}] {msg}\033[0m", flush=True)
    except UnicodeEncodeError:
        print(f"[{ts}] [{level}] {msg}", flush=True)

# ==================== SESSION ====================
def next_session():
    global _session_idx
    with session_lock:
        sid = Config.SESSION_IDS[_session_idx % len(Config.SESSION_IDS)]
        _session_idx += 1
        return sid

# ==================== DRIVER ====================
def make_driver(session_id):
    opts = webdriver.ChromeOptions()
    if Config.HEADLESS:
        opts.add_argument("--headless=new")
    for arg in [
        "--disable-extensions", "--disable-gpu", "--no-sandbox",
        "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled",
        "--window-size=1920,1080", "--mute-audio",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2
    })
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Page.enable", {})

    # Set Instagram session cookie
    driver.get("https://www.instagram.com/")
    time.sleep(1.5)
    driver.add_cookie({
        "name": "sessionid", "value": session_id,
        "domain": ".instagram.com", "path": "/",
        "secure": True, "httpOnly": True,
    })
    log(f"Session set: ...{session_id[-12:]}")
    return driver

# ==================== UTILS ====================
def username_from_url(url):
    return url.strip().rstrip("/").split("/")[-1].split("?")[0]

def safe_ts(ts):
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else None
    except Exception:
        return None

def clean_filename(s, maxlen=40):
    s = re.sub(r"[^\w\s-]", "", s or "")
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:maxlen]

# ==================== NETWORK CAPTURE ====================
def drain_logs(driver):
    """Return all unread performance log entries and clear the buffer."""
    try:
        return driver.get_log("performance")
    except Exception:
        return []

def fetch_response_body(driver, request_id):
    try:
        body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
        return json.loads(body.get("body", "{}"))
    except Exception:
        return {}

def parse_graphql_responses(driver, seen_req_ids: set):
    """
    Scan performance logs for NEW GraphQL responses.
    Returns dict: {timeline: [...nodes], reels: [...nodes], profile: {}}
    """
    results = {"timeline": [], "reels": [], "profile": {}}

    for entry in drain_logs(driver):
        try:
            msg = json.loads(entry["message"])["message"]
            method = msg.get("method", "")
            if method not in ("Network.responseReceived", "Network.loadingFinished"):
                continue

            params = msg.get("params", {})
            req_id = params.get("requestId")
            if not req_id or req_id in seen_req_ids:
                continue

            response = params.get("response", {})
            url = response.get("url", "")
            if "graphql/query" not in url and "api/v1" not in url:
                continue

            seen_req_ids.add(req_id)
            body = fetch_response_body(driver, req_id)
            if not isinstance(body, dict):
                continue

            data = body.get("data", {})
            if not data:
                continue

            # ---- Timeline posts ----
            for key in list(data.keys()):
                if "timeline" in key.lower() or "feed" in key.lower():
                    edges = _safe_edges(data[key])
                    if edges:
                        results["timeline"].extend(edges)

            # ---- Reels / clips ----
            for key in list(data.keys()):
                if "clips" in key.lower() or "reels" in key.lower():
                    edges = _safe_edges(data[key])
                    if edges:
                        results["reels"].extend(edges)

            # ---- Profile info ----
            if "user" in data and not results["profile"]:
                results["profile"] = data.get("user", {})

        except Exception:
            pass

    return results

def _safe_edges(obj):
    """Safely extract edge nodes from various GraphQL structures."""
    if not obj:
        return []
    # Direct edges list
    if isinstance(obj, dict):
        edges = obj.get("edges", [])
        if edges:
            return [e.get("node", e) for e in edges if e]
        # Sometimes it's wrapped in another key
        for v in obj.values():
            if isinstance(v, dict):
                edges = v.get("edges", [])
                if edges:
                    return [e.get("node", e) for e in edges if e]
    return []

# ==================== VIDEO URL EXTRACTION ====================
def extract_video_url(node):
    """
    Try every known location for a video URL in an Instagram GraphQL node.
    Returns (url_string | None).
    """
    if not node:
        return None

    # 1. Direct field
    url = node.get("video_url")
    if url:
        return url

    # 2. video_versions list (new API)
    vv = node.get("video_versions")
    if vv and isinstance(vv, list) and len(vv) > 0:
        # Pick highest-resolution version
        try:
            best = max(vv, key=lambda x: (x or {}).get("width", 0) if isinstance(x, dict) else 0)
            if isinstance(best, dict) and best.get("url"):
                return best["url"]
        except Exception:
            pass
        # Fallback: first element
        first = vv[0]
        if isinstance(first, dict):
            return first.get("url")

    # 3. Nested under media
    media = node.get("media")
    if media and isinstance(media, dict):
        return extract_video_url(media)

    return None

def extract_thumbnail(node):
    if not node:
        return None
    # image_versions2
    iv = node.get("image_versions2")
    if iv and isinstance(iv, dict):
        candidates = iv.get("candidates", [])
        if candidates and isinstance(candidates, list):
            return candidates[0].get("url") if isinstance(candidates[0], dict) else None
    return node.get("display_url") or node.get("thumbnail_url")

def node_is_video(node):
    """Return True if this node is or contains a video."""
    media_type = node.get("media_type")  # 2 = video, 1 = photo, 8 = carousel
    if media_type == 2:
        return True
    if node.get("is_video"):
        return True
    product = (node.get("product_type") or "").lower()
    if product in ("clips", "igtv", "reel", "feed"):
        return True
    if node.get("video_url") or node.get("video_versions"):
        return True
    return False

def build_metadata(node, source="timeline"):
    """Build a comprehensive metadata dict from a GraphQL node."""
    owner = node.get("owner") or node.get("user") or {}

    # Caption
    cap_text = ""
    cap_edges = node.get("edge_media_to_caption", {})
    if cap_edges:
        edges = cap_edges.get("edges", [])
        if edges:
            cap_text = (edges[0].get("node") or {}).get("text", "")
    if not cap_text:
        cap_obj = node.get("caption")
        if isinstance(cap_obj, dict):
            cap_text = cap_obj.get("text", "")
        elif isinstance(cap_obj, str):
            cap_text = cap_obj

    # Engagement
    likes = (
        (node.get("edge_liked_by") or {}).get("count")
        or (node.get("edge_media_preview_like") or {}).get("count")
        or node.get("like_count", 0) or 0
    )
    comments = (
        (node.get("edge_media_to_comment") or {}).get("count")
        or node.get("comment_count", 0) or 0
    )
    views = (
        node.get("video_view_count")
        or node.get("view_count")
        or node.get("play_count")
        or 0
    )

    taken_at = node.get("taken_at_timestamp") or node.get("taken_at")

    return {
        "id":               node.get("id"),
        "shortcode":        node.get("code") or node.get("shortcode"),
        "source":           source,
        "media_type":       node.get("media_type"),
        "product_type":     node.get("product_type"),
        "taken_at":         taken_at,
        "taken_at_fmt":     safe_ts(taken_at),
        "video_url":        extract_video_url(node),
        "thumbnail_url":    extract_thumbnail(node),
        "caption":          cap_text,
        "hashtags":         re.findall(r"#(\w+)", cap_text),
        "mentions":         re.findall(r"@(\w+)", cap_text),
        "likes":            likes,
        "comments":         comments,
        "views":            views,
        "shares":           node.get("share_count", 0),
        "saves":            node.get("saved_count", 0),
        "is_paid":          node.get("is_paid_partnership", False),
        "location":         (node.get("location") or {}).get("name"),
        "owner_id":         owner.get("id"),
        "owner_username":   owner.get("username"),
        "owner_fullname":   owner.get("full_name"),
        "owner_verified":   owner.get("is_verified", False),
        "owner_followers":  (owner.get("edge_followed_by") or {}).get("count", 0),
        "owner_following":  (owner.get("edge_follow") or {}).get("count", 0),
        "owner_posts":      (owner.get("edge_owner_to_timeline_media") or {}).get("count", 0),
        "music_title":      ((node.get("clips_metadata") or {}).get("music_info") or {}).get("title"),
        "music_artist":     ((node.get("clips_metadata") or {}).get("music_info") or {}).get("artist"),
    }

def extract_image_url(node):
    if not node or not isinstance(node, dict):
        return None

    for key in ("display_url", "thumbnail_url", "image_url", "display_src"):
        url = node.get(key)
        if url:
            return url

    iv = node.get("image_versions2")
    if iv and isinstance(iv, dict):
        candidates = iv.get("candidates", [])
        if isinstance(candidates, list):
            best = max(
                (c for c in candidates if isinstance(c, dict) and c.get("url")),
                key=lambda x: x.get("width", 0),
                default=None,
            )
            if best:
                return best.get("url")

    for key in ("display_resources", "resources"):
        resources = node.get(key)
        if isinstance(resources, list):
            best = max(
                (r for r in resources if isinstance(r, dict) and (r.get("src") or r.get("url"))),
                key=lambda x: x.get("config_width", 0) or x.get("width", 0),
                default=None,
            )
            if best:
                return best.get("src") or best.get("url")

    return None


def collect_image_entries(node):
    images = []

    def add_image(src_node, position=None):
        if not isinstance(src_node, dict):
            return
        url = extract_image_url(src_node)
        if not url:
            return

        entry = {
            "position": position,
            "id": src_node.get("id") or src_node.get("shortcode"),
            "media_type": src_node.get("media_type"),
            "is_video": bool(node_is_video(src_node)),
            "image_url": url,
            "thumbnail_url": src_node.get("thumbnail_url") or extract_thumbnail(src_node),
            "dimensions": {
                "width": (src_node.get("dimensions") or {}).get("width"),
                "height": (src_node.get("dimensions") or {}).get("height"),
            },
            "image_versions": [],
        }

        iv = src_node.get("image_versions2")
        if iv and isinstance(iv, dict):
            for candidate in iv.get("candidates", []) or []:
                if isinstance(candidate, dict) and candidate.get("url"):
                    entry["image_versions"].append({
                        "url": candidate.get("url"),
                        "width": candidate.get("width"),
                        "height": candidate.get("height"),
                    })

        for key in ("display_resources", "resources"):
            resources = src_node.get(key)
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict) and (resource.get("src") or resource.get("url")):
                        entry["image_versions"].append({
                            "url": resource.get("src") or resource.get("url"),
                            "width": resource.get("config_width") or resource.get("width"),
                            "height": resource.get("config_height") or resource.get("height"),
                        })

        images.append(entry)

    if node.get("media_type") == 8 or node.get("carousel_media") or node.get("edge_sidecar_to_children"):
        children = node.get("carousel_media") or []
        if not children:
            children = [e.get("node", e) for e in node.get("edge_sidecar_to_children", {}).get("edges", []) if e]
        for idx, child in enumerate(children, start=1):
            add_image(child, idx)
        if not images:
            add_image(node, 1)
    else:
        add_image(node, 1)

    return images


def extract_all_media_from_nodes(nodes, source="timeline"):
    """
    Given a list of GraphQL nodes, return a list of media dicts.
    Includes video and image posts, and extracts carousel children.
    """
    media_items = []
    for node in nodes:
        if not node or not isinstance(node, dict):
            continue

        if node.get("media_type") == 8 or node.get("carousel_media") or node.get("edge_sidecar_to_children"):
            children = node.get("carousel_media") or []
            if not children:
                children = [e.get("node", e) for e in node.get("edge_sidecar_to_children", {}).get("edges", []) if e]
            for child in children:
                media_items.extend(extract_all_media_from_nodes([child], source))
            continue

        metadata = build_metadata(node, source)
        metadata["images"] = collect_image_entries(node)

        if node_is_video(node):
            url = extract_video_url(node)
            if url:
                media_items.append({"url": url, "type": "video", "metadata": metadata})
        else:
            for image_entry in metadata["images"]:
                image_meta = dict(metadata)
                image_meta["image_url"] = image_entry["image_url"]
                image_meta["image_position"] = image_entry["position"]
                image_meta["image_dimensions"] = image_entry["dimensions"]
                image_meta["image_versions"] = image_entry["image_versions"]
                image_meta["thumbnail_url"] = image_entry["thumbnail_url"]
                media_items.append({"url": image_entry["image_url"], "type": "image", "metadata": image_meta})

    return media_items

# ==================== SCRAPING ====================
def scrape_section(driver, url, section_label, post_list: list, seen_ids: set, seen_req_ids: set):
    """
    Navigate to `url`, scroll until no new posts appear, collect nodes into post_list.
    Returns the profile info object found during scraping.
    """
    log(f"  → Navigating: {url}")
    driver.get(url)
    time.sleep(random.uniform(3, 5))

    section_profile = {}
    no_new_count = 0
    for scroll_n in range(Config.MAX_SCROLLS):
        parsed = parse_graphql_responses(driver, seen_req_ids)
        if parsed.get("profile") and not section_profile:
            section_profile = parsed["profile"]

        if section_label == "timeline":
            new_nodes = parsed["timeline"]
        elif section_label == "reels":
            new_nodes = parsed["reels"] + parsed["timeline"]
        else:
            new_nodes = parsed["timeline"] + parsed["reels"]

        added = 0
        for node in new_nodes:
            nid = node.get("id") or node.get("shortcode") or node.get("code")
            if nid and nid not in seen_ids:
                seen_ids.add(nid)
                node["_section"] = section_label
                post_list.append(node)
                added += 1

        if added == 0:
            no_new_count += 1
        else:
            no_new_count = 0
            log(f"  [{section_label}] Collected {len(post_list)} nodes (+{added})")

        if no_new_count >= Config.NO_NEW_LIMIT:
            log(f"  [{section_label}] No new data for {Config.NO_NEW_LIMIT} scrolls — stopping at {len(post_list)} nodes")
            break

    return section_profile

def scrape_profile(driver, profile_url):
    """
    Full profile scrape: timeline → reels → tagged.
    Returns: { "posts": [...media], "reels": [...media], "profile_info": {...} }
    """
    username = username_from_url(profile_url)
    seen_ids = set()
    seen_reqs = set()

    timeline_nodes = []
    reels_nodes = []
    tagged_nodes = []
    profile_info = {}

    # --- Timeline ---
    if Config.DOWNLOAD_POSTS:
        log(f"[{username}] Scraping timeline posts …")
        section_profile = scrape_section(driver, profile_url, "timeline", timeline_nodes, seen_ids, seen_reqs)
        if section_profile:
            profile_info = profile_info or section_profile

    # --- Reels ---
    if Config.DOWNLOAD_REELS:
        log(f"[{username}] Scraping reels …")
        section_profile = scrape_section(driver, f"{profile_url.rstrip('/')}/reels/", "reels", reels_nodes, seen_ids, seen_reqs)
        if section_profile:
            profile_info = profile_info or section_profile

    # --- Tagged ---
    log(f"[{username}] Scraping tagged posts …")
    section_profile = scrape_section(driver, f"{profile_url.rstrip('/')}/tagged/", "tagged", tagged_nodes, seen_ids, seen_reqs)
    if section_profile:
        profile_info = profile_info or section_profile

    if not profile_info and timeline_nodes:
        profile_info = (timeline_nodes[0].get("owner") or timeline_nodes[0].get("user") or {})

    post_media = extract_all_media_from_nodes(timeline_nodes + tagged_nodes, source="post")
    reels_media = extract_all_media_from_nodes(reels_nodes, source="reel")

    log(f"[{username}] Posts found: {len(post_media)} | Reels found: {len(reels_media)}", "SUCCESS")
    return {"posts": post_media, "reels": reels_media, "profile_info": profile_info}

# ==================== DOWNLOADING ====================
def download_media_item(media_dict, out_dir, label, idx, total):
    url = media_dict["url"]
    meta = media_dict["metadata"]
    media_type = media_dict.get("type", "video")
    ext = ".mp4" if media_type == "video" else ".jpg"

    sc = meta.get("shortcode") or meta.get("id") or f"media_{idx}"
    ts = meta.get("taken_at_fmt", "")[:10].replace("-", "") if meta.get("taken_at_fmt") else "unknown"
    cap_snip = clean_filename(meta.get("caption", "")[:35])
    suffix = '' if media_type == 'video' else ''
    fname = f"{ts}_{sc}{'_' + cap_snip if cap_snip else ''}{ext}"
    fname = re.sub(r'[<>:"/\\|?*]', "", fname)
    fpath = os.path.join(out_dir, fname)

    if os.path.exists(fpath) and os.path.getsize(fpath) > 1024:
        log(f"  [{label}] [{idx}/{total}] Already exists: {fname}")
        return True

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://www.instagram.com/",
    }

    for attempt in range(3):
        try:
            r = requests.get(url, stream=True, timeout=60, headers=headers)
            r.raise_for_status()
            with open(fpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            size_mb = os.path.getsize(fpath) / 1048576
            log(f"  [{label}] [{idx}/{total}] ✓ {fname}  ({size_mb:.1f} MB)", "SUCCESS")
            return True

        except Exception as e:
            if attempt == 2:
                log(f"  [{label}] [{idx}/{total}] FAILED: {fname} — {e}", "ERROR")
                if os.path.exists(fpath):
                    os.remove(fpath)
                return False
            time.sleep(2 ** attempt)

    return False

def save_metadata(username, posts, reels, profile_info, base_dir):
    """Save JSON + CSV metadata files for posts, reels, profile info, and all media."""
    def _rows(media_items):
        rows = []
        for item in media_items:
            m = item["metadata"]
            rows.append({
                "type":           item.get("type"),
                "shortcode":      m.get("shortcode"),
                "id":             m.get("id"),
                "source":         m.get("source"),
                "taken_at":       m.get("taken_at_fmt"),
                "caption":        (m.get("caption") or "")[:300],
                "hashtags":       ", ".join(m.get("hashtags") or []),
                "mentions":       ", ".join(m.get("mentions") or []),
                "likes":          m.get("likes"),
                "comments":       m.get("comments"),
                "views":          m.get("views"),
                "shares":         m.get("shares"),
                "saves":          m.get("saves"),
                "is_paid":        m.get("is_paid"),
                "location":       m.get("location"),
                "music_title":    m.get("music_title"),
                "music_artist":   m.get("music_artist"),
                "owner_username": m.get("owner_username"),
                "owner_fullname": m.get("owner_fullname"),
                "owner_verified": m.get("owner_verified"),
                "owner_followers":m.get("owner_followers"),
                "media_url":      item.get("url"),
                "image_url":      m.get("image_url"),
                "thumbnail_url":  m.get("thumbnail_url"),
                "image_position": m.get("image_position"),
            })
        return rows

    posts_rows = _rows(posts)
    reels_rows = _rows(reels)

    if posts_rows:
        pd.DataFrame(posts_rows).to_csv(
            os.path.join(base_dir, f"{username}_posts_metadata.csv"),
            index=False,
            encoding="utf-8-sig",
        )
    if reels_rows:
        pd.DataFrame(reels_rows).to_csv(
            os.path.join(base_dir, f"{username}_reels_metadata.csv"),
            index=False,
            encoding="utf-8-sig",
        )

    summary = {
        "username": username,
        "profile_info": profile_info or {},
        "scraped_at": datetime.now().isoformat(),
        "posts": posts_rows,
        "reels": reels_rows,
    }

    with open(os.path.join(base_dir, f"{username}_full_metadata.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    if profile_info:
        with open(os.path.join(base_dir, "profile_info.json"), "w", encoding="utf-8") as f:
            json.dump(profile_info, f, indent=2, ensure_ascii=False)

    log(f"[{username}] Metadata saved ({len(posts)} posts, {len(reels)} reels)", "SUCCESS")

def download_profile_picture(url, path):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.instagram.com/",
        }
        r = requests.get(url, stream=True, timeout=60, headers=headers)
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        log(f"  [PROFILE PIC] Saved profile image: {os.path.basename(path)}", "SUCCESS")
        return True
    except Exception as e:
        log(f"  [PROFILE PIC] Failed to save profile image: {e}", "WARNING")
        return False


def save_profile_info(username, profile_info, base_dir):
    if not profile_info:
        return

    profile_path = os.path.join(base_dir, "profile_info.json")
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile_info, f, indent=2, ensure_ascii=False)

    hd_url = None
    if isinstance(profile_info.get("hd_profile_pic_url_info"), dict):
        hd_url = profile_info.get("hd_profile_pic_url_info", {}).get("url")
    hd_url = hd_url or profile_info.get("profile_pic_url")

    if hd_url:
        download_profile_picture(hd_url, os.path.join(base_dir, f"{username}.jpg"))


def process_profile(profile_url, driver, stats):
    username = username_from_url(profile_url)
    base_dir = os.path.join("videos", username)
    posts_videos_dir = os.path.join(base_dir, "posts", "videos")
    posts_images_dir = os.path.join(base_dir, "posts", "images")
    reels_videos_dir = os.path.join(base_dir, "reels", "videos")
    reels_images_dir = os.path.join(base_dir, "reels", "images")
    os.makedirs(posts_videos_dir, exist_ok=True)
    os.makedirs(posts_images_dir, exist_ok=True)
    os.makedirs(reels_videos_dir, exist_ok=True)
    os.makedirs(reels_images_dir, exist_ok=True)

    log(f"{'='*60}")
    log(f"Processing: {username}")

    data = scrape_profile(driver, profile_url)
    posts = data.get("posts", [])
    reels = data.get("reels", [])
    profile_info = data.get("profile_info", {})

    if not posts and not reels:
        log(f"[{username}] No media found (private or no content)", "WARNING")
        with stats_lock:
            stats["failed"] += 1
        return False

    save_metadata(username, posts, reels, profile_info, base_dir)
    save_profile_info(username, profile_info, base_dir)

    post_videos = [m for m in posts if m.get("type") == "video"]
    post_images = [m for m in posts if m.get("type") == "image"]
    reel_videos = [m for m in reels if m.get("type") == "video"]
    reel_images = [m for m in reels if m.get("type") == "image"]

    dl_ok = 0
    if post_videos:
        log(f"[{username}] Downloading {len(post_videos)} post videos …")
        for i, media in enumerate(post_videos, 1):
            ok = download_media_item(media, posts_videos_dir, "POST-VIDEO", i, len(post_videos))
            if ok:
                dl_ok += 1
                with stats_lock:
                    stats["videos_downloaded"] += 1
            else:
                with stats_lock:
                    stats["videos_failed"] += 1
            time.sleep(random.uniform(0.3, 1.0))

    if post_images:
        log(f"[{username}] Downloading {len(post_images)} post images …")
        for i, media in enumerate(post_images, 1):
            ok = download_media_item(media, posts_images_dir, "POST-IMAGE", i, len(post_images))
            if ok:
                dl_ok += 1
                with stats_lock:
                    stats["images_downloaded"] += 1
            else:
                with stats_lock:
                    stats["images_failed"] += 1
            time.sleep(random.uniform(0.3, 1.0))

    if reel_videos:
        log(f"[{username}] Downloading {len(reel_videos)} reel videos …")
        for i, media in enumerate(reel_videos, 1):
            ok = download_media_item(media, reels_videos_dir, "REEL-VIDEO", i, len(reel_videos))
            if ok:
                dl_ok += 1
                with stats_lock:
                    stats["videos_downloaded"] += 1
            else:
                with stats_lock:
                    stats["videos_failed"] += 1
            time.sleep(random.uniform(0.3, 1.0))

    if reel_images:
        log(f"[{username}] Downloading {len(reel_images)} reel images …")
        for i, media in enumerate(reel_images, 1):
            ok = download_media_item(media, reels_images_dir, "REEL-IMAGE", i, len(reel_images))
            if ok:
                dl_ok += 1
                with stats_lock:
                    stats["images_downloaded"] += 1
            else:
                with stats_lock:
                    stats["images_failed"] += 1
            time.sleep(random.uniform(0.3, 1.0))

    if dl_ok > 0 or (posts or reels):
        with stats_lock:
            stats["profiles_completed"] += 1
        return True
    return False

# ==================== WORKER ====================
def worker(url_queue, stats, no_response, pbar, done_list):
    sid    = next_session()
    driver = None
    try:
        driver = make_driver(sid)
    except Exception as e:
        log(f"Driver creation failed: {e}", "ERROR")
        return

    try:
        while True:
            try:
                url = url_queue.get_nowait()
            except queue.Empty:
                break

            try:
                success = process_profile(url, driver, stats)
                if success:
                    with done_lock:
                        done_list.append(url)
                        _append_done(url)
                else:
                    with done_lock:
                        no_response.append(url)
            except Exception as e:
                log(f"Worker error [{username_from_url(url)}]: {e}", "ERROR")
                with stats_lock:
                    stats["failed"] += 1
            finally:
                pbar.update(1)
                url_queue.task_done()

    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ==================== FILE MANAGEMENT ====================
def load_urls():
    try:
        df = pd.read_csv(Config.INPUT_FILE)
        all_urls = [str(u).strip().rstrip("/") for u in df["url"].tolist()]
    except Exception as e:
        log(f"Cannot read {Config.INPUT_FILE}: {e}", "ERROR")
        return [], []

    done_urls = set()
    if os.path.exists(Config.DONE_FILE):
        try:
            df_done = pd.read_csv(Config.DONE_FILE)
            done_urls = {str(u).strip().rstrip("/") for u in df_done["url"].tolist()}
        except Exception:
            pass

    pending = [u for u in all_urls if u not in done_urls]
    log(f"Loaded {len(all_urls)} URLs | {len(done_urls)} done | {len(pending)} pending")
    return pending, list(done_urls)

def _append_done(url):
    if not os.path.exists(Config.DONE_FILE):
        with open(Config.DONE_FILE, "w") as f:
            f.write("url\n")
    with open(Config.DONE_FILE, "a") as f:
        f.write(url + "\n")
    # Remove from input
    try:
        df = pd.read_csv(Config.INPUT_FILE)
        df = df[df["url"].str.strip().str.rstrip("/") != url]
        df.to_csv(Config.INPUT_FILE, index=False)
    except Exception:
        pass

# ==================== MAIN ====================
def main():
    log("=" * 60)
    log("Instagram Downloader — Posts & Reels Separated")
    log("=" * 60)

    pending, _ = load_urls()
    if not pending:
        log("Nothing to process.", "SUCCESS")
        return

    if Config.TEST_MODE:
        pending = pending[:Config.MAX_TEST]
        log(f"TEST MODE: {len(pending)} profile(s)", "WARNING")

    os.makedirs("videos", exist_ok=True)

    stats = {
        "total": len(pending),
        "profiles_completed": 0,
        "failed": 0,
        "videos_downloaded": 0,
        "videos_failed": 0,
        "images_downloaded": 0,
        "images_failed": 0,
    }
    no_response = []
    done_list   = []

    url_q = queue.Queue()
    for u in pending:
        url_q.put(u)

    workers = []
    t0 = time.time()

    with tqdm(total=len(pending), desc="Profiles",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
        for i in range(min(Config.MAX_WORKERS, len(pending))):
            t = threading.Thread(
                target=worker,
                args=(url_q, stats, no_response, pbar, done_list),
                name=f"W{i+1}",
                daemon=True,
            )
            t.start()
            workers.append(t)
            time.sleep(1)

        url_q.join()

    for t in workers:
        t.join(timeout=10)

    if no_response:
        pd.DataFrame({"url": no_response}).to_csv("videos/failed.csv", index=False)
        log(f"Failed URLs → videos/failed.csv", "WARNING")

    elapsed = time.time() - t0
    log("=" * 60, "SUCCESS")
    log(f"DONE  —  {elapsed:.0f}s  ({elapsed/60:.1f} min)", "SUCCESS")
    log(f"Profiles completed : {stats['profiles_completed']}/{stats['total']}", "SUCCESS")
    log(f"Videos downloaded  : {stats['videos_downloaded']}", "SUCCESS")
    log(f"Videos failed      : {stats['videos_failed']}", "WARNING" if stats['videos_failed'] else "INFO")
    log(f"Images downloaded  : {stats['images_downloaded']}", "SUCCESS")
    log(f"Images failed      : {stats['images_failed']}", "WARNING" if stats['images_failed'] else "INFO")
    log("=" * 60, "SUCCESS")

if __name__ == "__main__":
    main()