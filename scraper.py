import os
import re
import time
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional
from facebook_scraper import get_posts
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
LOVABLE_WEBHOOK_URL = os.environ.get("LOVABLE_WEBHOOK_URL", "")
POSTS_PER_GROUP = 20
DELAY_BETWEEN_GROUPS = 5
COOKIES_FILE = os.environ.get("FB_COOKIES_FILE", None)
FB_COOKIES = os.environ.get("FB_COOKIES", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_cookies_dict():
    """Parse FB_COOKIES env var into a dict for facebook_scraper."""
    if FB_COOKIES:
        try:
            return json.loads(FB_COOKIES)
        except json.JSONDecodeError:
            cookies = {}
            for pair in FB_COOKIES.split(";"):
                pair = pair.strip()
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    cookies[k.strip()] = v.strip()
            return cookies if cookies else None
    return None


def extract_price(text):
    if not text:
        return None
    patterns = [
        r'(\d{1,3}(?:,\d{3})+)\s*(?:₪|שח|\u05e9"ח|שקל)',
        r'(?:₪|שח|\u05e9"ח|שקל)\s*(\d{1,3}(?:,\d{3})+)',
        r'(\d{4,7})\s*(?:₪|שח|\u05e9"ח|שקל)',
        r'(?:מחיר|price)\s*:?\s*(\d{1,3}(?:,\d{3})*)'
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except:
                continue
    return None


def extract_rooms(text):
    if not text:
        return None
    m = re.search(r'(\d(?:\.\d)?)\s*חדר', text)
    return float(m.group(1)) if m else None


CITIES = [
    "תל אביב", "ירושלים", "חיפה", "באר שבע",
    "רמת גן", "גבעתיים", "פתח תקווה",
    "ראשון לציון", "חולון", "בת ים",
    "נתניה", "הרצליה", "רעננה",
    "כפר סבא", "הוד השרון",
    "רחובות", "אשדוד", "אשקלון",
    "מודיעין", "בני ברק"
]


def extract_city(text):
    if not text:
        return None
    for c in CITIES:
        if c in text:
            return c
    return None


def extract_listing_type(text):
    if not text:
        return None
    for kw in ["להשכרה", "לשכירות", "שכירות", "rent", "לחודש"]:
        if kw in text.lower():
            return "rent"
    for kw in ["למכירה", "מכירה", "for sale"]:
        if kw in text.lower():
            return "sale"
    return None


def get_active_groups():
    return supabase.table("facebook_groups").select("*").eq("is_active", True).execute().data


def extract_group_id(url):
    m = re.search(r'facebook\.com/groups/([^/?]+)', url)
    return m.group(1) if m else url


def forward_to_lovable(post_data):
    if not LOVABLE_WEBHOOK_URL:
        return False
    try:
        payload = {
            "text": post_data.get("post_text", ""),
            "city": post_data.get("city", ""),
            "post_url": post_data.get("post_url", ""),
            "post_id": post_data.get("facebook_post_id", ""),
            "images": post_data.get("images", []),
            "price": post_data.get("price"),
            "rooms": post_data.get("rooms"),
            "listing_type": post_data.get("listing_type"),
            "group_name": post_data.get("group_name", ""),
            "source": "facebook"
        }
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            LOVABLE_WEBHOOK_URL,
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status == 200
    except Exception as e:
        logger.warning(f"  Webhook forward error: {e}")
        return False


def scrape_group(group):
    gid = extract_group_id(group["group_url"])
    name = group.get("group_name", gid)
    logger.info(f"Scraping: {name}")
    posts = []
    try:
        opts = {"posts_per_page": 5, "allow_extra_requests": False}
        cookies_dict = get_cookies_dict()
        if cookies_dict:
            opts["cookies"] = cookies_dict
            logger.info(f"  Using cookies from FB_COOKIES env var")
        elif COOKIES_FILE and os.path.exists(COOKIES_FILE):
            opts["cookies"] = COOKIES_FILE
            logger.info(f"  Using cookies from file: {COOKIES_FILE}")
        else:
            logger.warning(f"  No cookies available - may not access private groups")
        for post in get_posts(group=gid, pages=POSTS_PER_GROUP // 5, options=opts):
            text = post.get("text") or post.get("post_text") or ""
            pid = post.get("post_id")
            if not pid:
                continue
            imgs = list(post.get("images", [])) if post.get("images") else (
                [post["image"]] if post.get("image") else [])
            posts.append({
                "facebook_post_id": str(pid),
                "group_id": group["id"],
                "group_name": name,
                "author_name": post.get("username"),
                "post_text": text,
                "post_url": post.get("post_url", f"https://facebook.com/{pid}"),
                "images": imgs,
                "post_date": post.get("time", datetime.utcnow()).isoformat() if post.get("time") else None,
                "price": extract_price(text),
                "city": extract_city(text),
                "rooms": extract_rooms(text),
                "listing_type": extract_listing_type(text),
                "likes_count": post.get("likes", 0) or 0,
                "comments_count": post.get("comments", 0) or 0,
                "scraped_at": datetime.utcnow().isoformat()
            })
        logger.info(f"  Found {len(posts)} posts")
    except Exception as e:
        logger.error(f"  Error: {e}")
    return posts


def save_posts(posts):
    saved = 0
    forwarded = 0
    for p in posts:
        try:
            supabase.table("posts").upsert(p, on_conflict="facebook_post_id").execute()
            saved += 1
            if forward_to_lovable(p):
                forwarded += 1
        except Exception as e:
            logger.warning(f"  Save error: {e}")
    if LOVABLE_WEBHOOK_URL:
        logger.info(f"  Forwarded {forwarded}/{saved} posts to Lovable")
    return saved


def run_scraper():
    logger.info("Starting scrape...")
    if LOVABLE_WEBHOOK_URL:
        logger.info(f"Lovable webhook configured")
    else:
        logger.warning("LOVABLE_WEBHOOK_URL not set - posts will NOT be forwarded to Lovable")
    cookies_dict = get_cookies_dict()
    if cookies_dict:
        logger.info(f"FB_COOKIES loaded with keys: {list(cookies_dict.keys())}")
    elif COOKIES_FILE:
        logger.info(f"Using cookies file: {COOKIES_FILE}")
    else:
        logger.warning("No Facebook cookies configured - scraper may not access private groups")
    groups = get_active_groups()
    logger.info(f"Found {len(groups)} active groups")
    total = 0
    for i, g in enumerate(groups):
        posts = scrape_group(g)
        total += save_posts(posts)
        supabase.table("facebook_groups").update(
            {"last_scraped_at": datetime.utcnow().isoformat()}
        ).eq("id", g["id"]).execute()
        if i < len(groups) - 1:
            time.sleep(DELAY_BETWEEN_GROUPS)
    logger.info(f"Done! Saved {total} posts from {len(groups)} groups")


def add_group(url, name=None):
    gid = extract_group_id(url)
    supabase.table("facebook_groups").upsert(
        {"group_url": url, "group_name": name or gid, "is_active": True},
        on_conflict="group_url"
    ).execute()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "add-group":
        add_group(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
    elif len(sys.argv) > 1 and sys.argv[1] == "add-from-file":
        with open(sys.argv[2]) as f:
            for g in json.load(f):
                add_group(g.get("url", g.get("group_url")), g.get("name", g.get("group_name")))
    else:
        run_scraper()
