import os
import re
import time
import json
import logging
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── Config ──────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
LOVABLE_WEBHOOK_URL = os.environ.get("LOVABLE_WEBHOOK_URL", "")
POSTS_PER_GROUP = 20
DELAY_BETWEEN_GROUPS = 5
FB_COOKIES = os.environ.get("FB_COOKIES", "")
PROXY_URL = os.environ.get("PROXY_URL", "")  # e.g. http://user:pass@host:port

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# ── Cookie handling ─────────────────────────────────────────────────────
def get_cookies_dict():
    """Parse FB_COOKIES env var into a dict. URL-decodes values for proper sending."""
    if FB_COOKIES:
        try:
            raw = json.loads(FB_COOKIES)
        except json.JSONDecodeError:
            raw = {}
            for pair in FB_COOKIES.split(";"):
                pair = pair.strip()
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    raw[k.strip()] = v.strip()
        if raw:
            # URL-decode values (e.g. %3A → :) so cookies match what browser sends
            decoded = {k: urllib.parse.unquote(v) for k, v in raw.items()}
            logger.info(f"  Cookies decoded: xs starts with '{decoded.get('xs','')[:20]}...'")
            return decoded
    return None

# ── Text extraction helpers ─────────────────────────────────────────────
def extract_price(text):
    if not text:
        return None
    patterns = [
        r'(\d{1,3}(?:,\d{3})*)\s*₪',
        r'₪\s*(\d{1,3}(?:,\d{3})*)',
        r'(\d{1,3}(?:,\d{3})*)\s*ש"ח',
        r'(\d{1,3}(?:,\d{3})*)\s*שח',
        r'(\d{1,3}(?:,\d{3})*)\s*שקל',
        r'(\d{4,6})\s*(?:לחודש|per month|ל?חודש)',
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            price_str = m.group(1).replace(",", "")
            price = int(price_str)
            if 500 <= price <= 50000:
                return price
    return None

def extract_rooms(text):
    if not text:
        return None
    m = re.search(r'(\d(?:\.\d)?)\s*חדר', text)
    if m:
        rooms = float(m.group(1))
        if 1 <= rooms <= 12:
            return rooms
    return None

CITIES = [
    "תל אביב", "ירושלים", "חיפה", "באר שבע", "רמת גן", "גבעתיים",
    "פתח תקווה", "ראשון לציון", "חולון", "בת ים", "נתניה", "הרצליה",
    "רעננה", "כפר סבא", "הוד השרון", "רחובות", "אשדוד", "אשקלון",
    "מודיעין", "בני ברק",
]

def extract_city(text):
    if not text:
        return None
    for city in CITIES:
        if city in text:
            return city
    return None

def extract_listing_type(text):
    if not text:
        return "rent"
    sell_kw = ["למכירה", "מכירה", "for sale"]
    sub_kw = ["סאבלט", "sublet", "סב-לט"]
    for kw in sub_kw:
        if kw in text.lower():
            return "sublet"
    for kw in sell_kw:
        if kw in text.lower():
            return "sale"
    return "rent"

# ── Supabase helpers ────────────────────────────────────────────────────
def get_active_groups():
    resp = supabase.table("facebook_groups").select("*").eq("is_active", True).execute()
    return resp.data or []

def extract_group_id(url):
    m = re.search(r'groups/([^/?]+)', url)
    return m.group(1) if m else url

# ── Webhook ─────────────────────────────────────────────────────────────
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
            "source": "facebook",
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            LOVABLE_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status == 200
    except Exception as e:
        logger.warning(f"  Webhook forward error: {e}")
        return False

# ── Custom mbasic.facebook.com scraper ──────────────────────────────────
def handle_splash_page(session, html, base_url):
    """Try to bypass Facebook splash/interstitial page by following forms or links."""
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: Find a form with a "Continue" or similar submit button
    forms = soup.find_all("form")
    for form in forms:
        action = form.get("action", "")
        # Look for forms that seem to be "continue" or "accept" forms
        submit_btn = form.find("input", {"type": "submit"})
        if submit_btn or action:
            form_data = {}
            for inp in form.find_all("input"):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    form_data[name] = value

            if action.startswith("/"):
                # Determine base domain from base_url
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                form_url = f"{parsed.scheme}://{parsed.netloc}{action}"
            elif action.startswith("http"):
                form_url = action
            else:
                form_url = base_url

            logger.info(f"  Splash bypass: submitting form to {form_url[:80]}...")
            try:
                resp2 = session.post(form_url, data=form_data, headers=HEADERS, timeout=20, allow_redirects=True)
                if resp2.status_code == 200 and "splashScreenAttribution" not in resp2.text:
                    logger.info(f"  Splash bypass SUCCESS via form submit!")
                    return resp2.text
            except Exception as e:
                logger.warning(f"  Splash form submit error: {e}")

    # Strategy 2: Find "Continue" / "המשך" links
    continue_patterns = [r'Continue', r'המשך', r'OK', r'Confirm', r'אישור']
    for pattern in continue_patterns:
        link = soup.find("a", string=re.compile(pattern, re.IGNORECASE))
        if link and link.get("href"):
            href = link["href"]
            if href.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                href = f"{parsed.scheme}://{parsed.netloc}{href}"
            logger.info(f"  Splash bypass: following '{pattern}' link to {href[:80]}...")
            try:
                resp2 = session.get(href, headers=HEADERS, timeout=20, allow_redirects=True)
                if resp2.status_code == 200 and "splashScreenAttribution" not in resp2.text:
                    logger.info(f"  Splash bypass SUCCESS via link!")
                    return resp2.text
            except Exception as e:
                logger.warning(f"  Splash link follow error: {e}")

    # Strategy 3: Look for meta refresh or redirect URL in the page
    meta_refresh = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if meta_refresh:
        content = meta_refresh.get("content", "")
        m = re.search(r'url=(.+)', content, re.I)
        if m:
            redirect_url = m.group(1).strip()
            if redirect_url.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                redirect_url = f"{parsed.scheme}://{parsed.netloc}{redirect_url}"
            logger.info(f"  Splash bypass: following meta refresh to {redirect_url[:80]}...")
            try:
                resp2 = session.get(redirect_url, headers=HEADERS, timeout=20, allow_redirects=True)
                if resp2.status_code == 200:
                    logger.info(f"  Splash bypass SUCCESS via meta refresh!")
                    return resp2.text
            except Exception as e:
                logger.warning(f"  Meta refresh error: {e}")

    # Strategy 4: Log splash page details for debugging
    logger.warning(f"  Could not bypass splash page. Forms found: {len(forms)}")
    # Log form actions for debugging
    for i, form in enumerate(forms):
        logger.info(f"  Form {i}: action={form.get('action', 'none')[:100]}")
        inputs = [(inp.get('name'), inp.get('type'), inp.get('value', '')[:30]) for inp in form.find_all('input')]
        logger.info(f"  Form {i} inputs: {inputs[:10]}")
    # Log links that might be useful
    all_links = soup.find_all("a", href=True)
    for link in all_links[:10]:
        logger.info(f"  Link: {link.get_text(strip=True)[:50]} -> {link['href'][:100]}")

    return None

def fetch_mbasic_page(session, group_id, next_url=None):
    """Fetch a page from mbasic.facebook.com for a group. Falls back to m.facebook.com."""
    urls_to_try = []
    if next_url:
        urls_to_try.append(next_url)
    else:
        urls_to_try.append(f"https://mbasic.facebook.com/groups/{group_id}")
        urls_to_try.append(f"https://m.facebook.com/groups/{group_id}")

    # Log cookies being sent (first call only for debugging)
    cookie_names = [c.name for c in session.cookies]
    logger.info(f"  Cookies on session: {cookie_names}")

    for url in urls_to_try:
        try:
            logger.info(f"  Trying: {url}")
            resp = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            logger.info(f"  Response URL: {resp.url} (status={resp.status_code})")
            if resp.status_code != 200:
                logger.error(f"  HTTP {resp.status_code} for {url}")
                continue

            # Check if we got a splash page (no real content)
            if "splashScreenAttribution" in resp.text:
                logger.warning(f"  Got splash page from {url}, trying to bypass...")
                bypassed = handle_splash_page(session, resp.text, url)
                if bypassed:
                    return bypassed
                logger.warning(f"  Splash bypass failed for {url}, trying next...")
                continue

            return resp.text
        except Exception as e:
            logger.error(f"  Request error for {url}: {e}")
            continue
    return None

def parse_mbasic_posts(html, group_id):
    """Parse posts from mbasic.facebook.com HTML. Tries multiple strategies."""
    soup = BeautifulSoup(html, "html.parser")
    posts = []

    # Check if we got a login page instead of group content
    if soup.find("input", {"name": "email"}) and soup.find("input", {"name": "pass"}):
        logger.warning("  Got login page - cookies may be invalid or expired")
        return posts, None

    # Strategy 1: Find divs with data-ft attribute (traditional mbasic structure)
    post_containers = soup.find_all("div", attrs={"data-ft": True})
    if post_containers:
        logger.info(f"  Strategy 1 (data-ft): found {len(post_containers)} elements")
        for container in post_containers:
            post = extract_post_from_container(container, group_id)
            if post:
                posts.append(post)

    # Strategy 2: Find article elements
    if not posts:
        articles = soup.find_all("article")
        if articles:
            logger.info(f"  Strategy 2 (article): found {len(articles)} elements")
            for article in articles:
                post = extract_post_from_container(article, group_id)
                if post:
                    posts.append(post)

    # Strategy 3: Find story divs by class pattern
    if not posts:
        story_divs = soup.find_all("div", class_=re.compile(r"(story|userContent|_5pbx|_3576)"))
        if story_divs:
            logger.info(f"  Strategy 3 (story class): found {len(story_divs)} elements")
            for div in story_divs:
                post = extract_post_from_container(div, group_id)
                if post:
                    posts.append(post)

    # Strategy 4: Find post-like sections by looking for timestamp links
    if not posts:
        # On mbasic, each post typically has an <abbr> with timestamp or a link to the post
        post_links = soup.find_all("a", href=re.compile(r"/groups/\d+/permalink/\d+/|/story\.php\?"))
        if post_links:
            logger.info(f"  Strategy 4 (permalink links): found {len(post_links)} links")
            seen_parents = set()
            for link in post_links:
                # Walk up to find the post container
                parent = link
                for _ in range(8):
                    parent = parent.parent
                    if parent is None:
                        break
                    parent_id = id(parent)
                    if parent_id in seen_parents:
                        break
                    # Check if this looks like a post container (has some text)
                    text = parent.get_text(strip=True)
                    if len(text) > 50:
                        seen_parents.add(parent_id)
                        post = extract_post_from_container(parent, group_id, permalink_link=link)
                        if post:
                            posts.append(post)
                        break

    # Strategy 5: Find all section/div blocks with substantial text content
    if not posts:
        # Last resort: look for any div with significant Hebrew text
        all_divs = soup.find_all(["div", "section"])
        candidates = []
        for div in all_divs:
            text = div.get_text(strip=True)
            # Look for divs with Hebrew text that look like listings
            if len(text) > 80 and any(c in text for c in ["להשכרה", "חדרים", "דירה", "₪", "שכירות"]):
                candidates.append(div)
        if candidates:
            logger.info(f"  Strategy 5 (Hebrew text blocks): found {len(candidates)} candidates")
            # De-duplicate by checking if one is parent of another
            for div in candidates[:20]:  # Limit to 20
                post = extract_post_from_container(div, group_id)
                if post:
                    posts.append(post)

    # Debug logging if nothing found
    if not posts:
        logger.warning(f"  No posts found with any strategy")
        # Log HTML structure hints
        body = soup.find("body")
        if body:
            # Log top-level structure
            children = list(body.children)
            tag_names = [c.name for c in children if hasattr(c, "name") and c.name]
            logger.info(f"  Body children tags: {tag_names[:20]}")
            # Log first 1000 chars of body text
            body_text = body.get_text(strip=True)[:1000]
            logger.info(f"  Body text preview: {body_text[:500]}")
            # Log all div classes
            all_classes = set()
            for div in soup.find_all("div", class_=True):
                for cls in div.get("class", []):
                    all_classes.add(cls)
            logger.info(f"  All div classes: {sorted(list(all_classes))[:50]}")
            # Log all data- attributes
            data_attrs = set()
            for tag in soup.find_all(True):
                for attr in tag.attrs:
                    if attr.startswith("data-"):
                        data_attrs.add(attr)
            logger.info(f"  Data attributes found: {sorted(list(data_attrs))[:30]}")

    # Find next page URL
    next_page_url = None
    see_more = soup.find("a", string=re.compile(r"(See More|הצג עוד|ראו עוד|עוד פוסטים|See more posts)"))
    if see_more and see_more.get("href"):
        href = see_more["href"]
        if href.startswith("/"):
            next_page_url = f"https://mbasic.facebook.com{href}"
        elif href.startswith("http"):
            next_page_url = href
    # Also look for "See More Posts" link pattern
    if not next_page_url:
        more_link = soup.find("a", href=re.compile(r"/groups/.*\?bacr="))
        if more_link:
            href = more_link["href"]
            next_page_url = f"https://mbasic.facebook.com{href}" if href.startswith("/") else href

    return posts, next_page_url

def extract_post_from_container(container, group_id, permalink_link=None):
    """Extract post data from an HTML container element."""
    text = container.get_text(separator="\n", strip=True)
    if not text or len(text) < 20:
        return None

    # Extract post ID from permalink
    post_id = None
    post_url = None

    # Check data-ft attribute for post ID
    data_ft = container.get("data-ft")
    if data_ft:
        try:
            ft = json.loads(data_ft)
            post_id = str(ft.get("top_level_post_id", ft.get("tl_objid", "")))
        except (json.JSONDecodeError, TypeError):
            pass

    # Find permalink link
    if not post_id:
        link = permalink_link or container.find("a", href=re.compile(
            r"/groups/\d+/permalink/(\d+)|/story\.php\?story_fbid=(\d+)"
        ))
        if link:
            href = link.get("href", "")
            m = re.search(r"permalink/(\d+)|story_fbid=(\d+)", href)
            if m:
                post_id = m.group(1) or m.group(2)
            if href.startswith("/"):
                post_url = f"https://facebook.com{href}"
            else:
                post_url = href

    if not post_id:
        # Generate a hash-based ID from text content
        post_id = f"gen_{abs(hash(text[:200]))}"

    if not post_url:
        post_url = f"https://facebook.com/{post_id}"

    # Extract images
    images = []
    for img in container.find_all("img"):
        src = img.get("src", "")
        if src and "scontent" in src and "emoji" not in src.lower():
            images.append(src)

    # Extract author
    author = None
    # On mbasic, author is usually the first <strong> or first link with a profile URL
    author_tag = container.find("strong")
    if author_tag:
        author_link = author_tag.find("a")
        if author_link:
            author = author_link.get_text(strip=True)
    if not author:
        profile_link = container.find("a", href=re.compile(r"/profile\.php|facebook\.com/[a-zA-Z]"))
        if profile_link:
            author = profile_link.get_text(strip=True)

    return {
        "facebook_post_id": str(post_id),
        "group_id": group_id if isinstance(group_id, int) else None,
        "group_name": "",  # Will be set by caller
        "author_name": author,
        "post_text": text[:5000],  # Limit text length
        "post_url": post_url,
        "images": images,
        "post_date": datetime.now(timezone.utc).isoformat(),
        "price": extract_price(text),
        "city": extract_city(text),
        "rooms": extract_rooms(text),
        "listing_type": extract_listing_type(text),
        "likes_count": 0,
        "comments_count": 0,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

# ── Main scraping logic ────────────────────────────────────────────────
def scrape_group(group):
    gid = extract_group_id(group["group_url"])
    name = group.get("group_name", gid)
    logger.info(f"Scraping: {name} (id={gid})")

    cookies_dict = get_cookies_dict()
    if not cookies_dict:
        logger.warning("  No cookies available - cannot scrape")
        return []

    session = requests.Session()

    # Set proxy if configured
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
        logger.info(f"  Using proxy: {PROXY_URL[:30]}...")

    # Set cookies both ways: cookie jar with domain AND raw Cookie header
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
    for key, value in cookies_dict.items():
        session.cookies.set(key, value, domain=".facebook.com")
    # Also set Cookie header directly as fallback
    session.headers.update({"Cookie": cookie_str})
    logger.info(f"  Cookie header set: {cookie_str[:80]}...")

    all_posts = []
    next_url = None
    pages_fetched = 0
    max_pages = POSTS_PER_GROUP // 5  # ~4 pages

    while pages_fetched < max_pages:
        html = fetch_mbasic_page(session, gid, next_url)
        if not html:
            break

        if pages_fetched == 0:
            logger.info(f"  HTML length: {len(html)} chars")

        posts, next_url = parse_mbasic_posts(html, group["id"])

        for p in posts:
            p["group_name"] = name
            p["group_id"] = group["id"]

        all_posts.extend(posts)
        pages_fetched += 1

        if not next_url:
            break
        time.sleep(1)  # Be polite between pages

    logger.info(f"  Found {len(all_posts)} posts across {pages_fetched} pages")
    return all_posts

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
    logger.info("Starting scrape (custom mbasic scraper)...")
    if LOVABLE_WEBHOOK_URL:
        logger.info(f"Lovable webhook configured")
    else:
        logger.warning("LOVABLE_WEBHOOK_URL not set")
    cookies_dict = get_cookies_dict()
    if cookies_dict:
        logger.info(f"FB_COOKIES loaded with keys: {list(cookies_dict.keys())}")
    else:
        logger.warning("No Facebook cookies configured - scraping will fail")
        return

    groups = get_active_groups()
    logger.info(f"Found {len(groups)} active groups")

    total = 0
    for i, g in enumerate(groups):
        posts = scrape_group(g)
        total += save_posts(posts)
        supabase.table("facebook_groups").update(
            {"last_scraped_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", g["id"]).execute()
        if i < len(groups) - 1:
            time.sleep(DELAY_BETWEEN_GROUPS)
        # Log progress every 10 groups
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{len(groups)} groups scraped, {total} posts saved")

    logger.info(f"Done! Saved {total} posts from {len(groups)} groups")

def add_group(url, name=None):
    gid = extract_group_id(url)
    data = {
        "group_url": url,
        "group_id": gid,
        "group_name": name or gid,
        "is_active": True,
    }
    try:
        supabase.table("facebook_groups").upsert(data, on_conflict="group_id").execute()
        logger.info(f"Added group: {name or gid}")
    except Exception as e:
        logger.error(f"Failed to add group: {e}")

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
