import logging
import os
import re
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse, urlunparse

import feedparser
import psycopg2
import requests
from psycopg2.extras import execute_values

# ============================================================
# SETTINGS
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
TOPICS_FILE = os.getenv("TOPICS_FILE", "topics.txt")

SCAN_INTERVAL = 40 * 60      # a new scan every 40 minutes
REQUEST_TIMEOUT = 15
MAX_WORKERS = 8              # parallel downloads
REQUEST_DELAY = 0.5          # seconds each worker rests after a request (be polite)
CHUNK_SIZE = 100             # feeds processed + saved per batch
USER_AGENT = "Mozilla/5.0 (compatible; NewsHubBot/1.0)"

# GitHub Actions mode: RUN_ONCE=1 scans one time and exits (the workflow repeats it).
RUN_ONCE = os.getenv("RUN_ONCE") == "1"
# Max seconds one scan may run. Feeds not reached resume on the next run.
TIME_BUDGET = int(os.getenv("TIME_BUDGET", str(SCAN_INTERVAL - 60)))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("news_hub")

# ============================================================
# 1) CURATED FEEDS
# ============================================================

GN = "https://news.google.com/rss"
GN_TAIL = "hl=en-US&gl=US&ceid=US:en"


def google_section(code):
    return f"{GN}/headlines/section/topic/{code}?{GN_TAIL}"


def google_search(query):
    # when:1d -> only last 24h, keeps results fresh
    return f"{GN}/search?q={quote_plus(query)}%20when:1d&{GN_TAIL}"


CURATED = {
    "BBC": {
        "world": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "business": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "technology": "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "science": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
        "health": "https://feeds.bbci.co.uk/news/health/rss.xml",
        "politics": "https://feeds.bbci.co.uk/news/politics/rss.xml",
        "entertainment": "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        "education": "https://feeds.bbci.co.uk/news/education/rss.xml",
        "sports": "https://feeds.bbci.co.uk/sport/rss.xml",
    },
    "Google News": {
        "world": google_section("WORLD"),
        "business": google_section("BUSINESS"),
        "technology": google_section("TECHNOLOGY"),
        "science": google_section("SCIENCE"),
        "health": google_section("HEALTH"),
        "sports": google_section("SPORTS"),
        "entertainment": google_section("ENTERTAINMENT"),
    },
    "The Guardian": {
        "world": "https://www.theguardian.com/world/rss",
        "business": "https://www.theguardian.com/business/rss",
        "technology": "https://www.theguardian.com/technology/rss",
        "science": "https://www.theguardian.com/science/rss",
        "politics": "https://www.theguardian.com/politics/rss",
        "environment": "https://www.theguardian.com/environment/rss",
        "education": "https://www.theguardian.com/education/rss",
        "sports": "https://www.theguardian.com/sport/rss",
        "entertainment": "https://www.theguardian.com/culture/rss",
        "travel": "https://www.theguardian.com/travel/rss",
        "food": "https://www.theguardian.com/food/rss",
    },
    "NYT": {
        "world": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "business": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "technology": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
        "health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
        "politics": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        "environment": "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml",
        "education": "https://rss.nytimes.com/services/xml/rss/nyt/Education.xml",
        "sports": "https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml",
        "entertainment": "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",
        "travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    },
}

# ============================================================
# 2) COUNTRY EDITIONS (top headlines per country)
# ============================================================

COUNTRIES = {
    "united states": "US", "united kingdom": "GB", "india": "IN", "canada": "CA",
    "australia": "AU", "new zealand": "NZ", "ireland": "IE", "singapore": "SG",
    "south africa": "ZA", "nigeria": "NG", "kenya": "KE", "ghana": "GH",
    "philippines": "PH", "malaysia": "MY", "hong kong": "HK", "pakistan": "PK",
    "bangladesh": "BD", "sri lanka": "LK", "egypt": "EG", "ethiopia": "ET",
    "tanzania": "TZ", "uganda": "UG", "zimbabwe": "ZW", "nepal": "NP",
    "united arab emirates": "AE", "israel": "IL", "saudi arabia": "SA",
}

# ============================================================
# 3) BUILT-IN KEYWORD TOPICS  (comma separated, add as many as you like)
# ============================================================

SEED_TOPICS = """
stock market, cryptocurrency, bitcoin, ethereum, inflation, interest rates, federal reserve, oil prices,
gold price, forex, real estate, housing market, startups, venture capital, ipo, mergers and acquisitions,
banking, fintech, retail, e-commerce, supply chain, jobs market, unemployment, taxes, trade war, tariffs,
commodities, mortgage rates, insurance, earnings, small business, economy, recession, global trade,
artificial intelligence, chatgpt, openai, machine learning, robotics, cybersecurity, data breach,
cloud computing, semiconductors, nvidia, apple, google, microsoft, amazon, meta, tesla, spacex, samsung,
smartphones, iphone, android, gadgets, video games, playstation, xbox, nintendo, virtual reality,
quantum computing, 5g, social media, tiktok, software, drones, blockchain, 3d printing, wearables,
space, nasa, mars, astronomy, climate change, physics, biology, genetics, archaeology, dinosaurs, oceans,
earthquakes, volcanoes, weather, hurricanes, wildfires, floods, renewable energy, solar power, wind power,
nuclear energy, batteries, wildlife, biodiversity, pollution, recycling, water crisis,
covid, vaccines, cancer, diabetes, mental health, nutrition, fitness, public health, pharmaceuticals, fda,
obesity, heart disease, alzheimer's, medical research, healthcare, hospitals, pandemic, flu, sleep, longevity,
football, premier league, champions league, la liga, serie a, bundesliga, world cup, nba, nfl, mlb, nhl,
wnba, cricket, ipl, tennis, wimbledon, golf, formula 1, olympics, boxing, ufc, rugby, cycling, athletics,
esports, badminton, hockey, swimming, motogp, nascar, chess,
movies, box office, hollywood, bollywood, netflix, streaming, television, music, concerts, celebrities,
oscars, grammys, k-pop, anime, books, theatre, comedy, fashion, art, museums, podcasts,
elections, white house, congress, supreme court, european union, united nations, nato, china, russia,
ukraine, gaza, iran, middle east, africa, latin america, brexit, immigration,
human rights, diplomacy, defense, military, terrorism, protests, sanctions, geopolitics, g20,
education, universities, students, crime, courts, police, travel, tourism, airlines, food, restaurants,
cars, electric vehicles, motorcycles, lifestyle, parenting, religion, philosophy, charity, agriculture,
farming, transport, railways, aviation, shipping, energy, mining, construction, architecture, design
""".replace("\n", " ")


def clean_topic(t):
    return re.sub(r"\s+", " ", t.strip().lower())


def load_file_topics():
    if not os.path.exists(TOPICS_FILE):
        return []
    with open(TOPICS_FILE, encoding="utf-8") as f:
        return [clean_topic(l) for l in f if l.strip() and not l.lstrip().startswith("#")]


def load_db_topics():
    try:
        conn = get_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT topic FROM scan_topics WHERE active;")
                return [clean_topic(r[0]) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        log.warning("Could not read scan_topics: %s", e)
        return []


def build_feed_list():
    """Returns [(source, topic, url)], dedicated feeds first."""
    feeds, seen_urls = [], set()

    def add(source, topic, url):
        if url not in seen_urls:
            seen_urls.add(url)
            feeds.append((source, topic, url))

    for source, topics in CURATED.items():
        for topic, url in topics.items():
            add(source, topic, url)

    for name, gl in COUNTRIES.items():
        add("Google News", name, f"{GN}?hl=en-{gl}&gl={gl}&ceid={gl}:en")

    keywords = [clean_topic(t) for t in SEED_TOPICS.split(",") if t.strip()]
    keywords += load_file_topics() + load_db_topics()
    for topic in dict.fromkeys(keywords):          # unique, keeps order
        add("Google News", topic, google_search(topic))

    return feeds


# ============================================================
# DATABASE
# ============================================================

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


def create_tables():
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id          SERIAL PRIMARY KEY,
                    title       TEXT NOT NULL,
                    description TEXT,
                    url         TEXT UNIQUE NOT NULL,
                    image       TEXT,
                    source      TEXT,
                    topic       TEXT,
                    published   TEXT,
                    saved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}';")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_topic ON news_articles (LOWER(topic));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_tags ON news_articles USING GIN (tags);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_saved_at ON news_articles (saved_at DESC);")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scan_topics (
                    topic  TEXT PRIMARY KEY,
                    active BOOLEAN NOT NULL DEFAULT TRUE
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scanner_state (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                );
            """)
    finally:
        conn.close()


def get_article_count():
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM news_articles;")
            return cur.fetchone()[0]
    finally:
        conn.close()


def save_articles(articles):
    """Insert new articles; if the URL exists, just add the new topic tags."""
    if not articles:
        return 0

    rows = [(a["title"], a["description"], a["url"], a["image"], a["source"],
             a["topic"], a["published"], sorted(a["tags"])) for a in articles]

    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            result = execute_values(
                cur,
                """
                INSERT INTO news_articles
                    (title, description, url, image, source, topic, published, tags)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                    SET tags = ARRAY(SELECT DISTINCT unnest(news_articles.tags || EXCLUDED.tags))
                    WHERE NOT news_articles.tags @> EXCLUDED.tags
                RETURNING (xmax = 0)
                """,
                rows,
                template="(%s,%s,%s,%s,%s,%s,%s,%s::text[])",
                page_size=200,
                fetch=True,
            )
        return sum(1 for r in result if r[0])      # True = brand-new row
    finally:
        conn.close()


# ============================================================
# PARSING HELPERS
# ============================================================

TAG_RE = re.compile(r"<[^>]+>")
IMG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
TRACKING = {"cmp", "ocid", "at_medium", "at_campaign", "fbclid", "gclid"}


def clean_text(text):
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", text or ""))).strip()


def normalize_url(url):
    parts = urlparse(url.strip())
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in TRACKING]
    return urlunparse(parts._replace(query=urlencode(query), fragment=""))


def get_image(entry):
    for item in entry.get("media_content", []) or []:
        if item.get("url"):
            return item["url"]
    for item in entry.get("media_thumbnail", []) or []:
        if item.get("url"):
            return item["url"]
    for item in entry.get("enclosures", []) or []:
        url = item.get("href") or item.get("url")
        if url and item.get("type", "").startswith("image"):
            return url
    blobs = [entry.get("summary", "")] + [c.get("value", "") for c in entry.get("content", []) or []]
    for blob in blobs:
        m = IMG_RE.search(blob or "")
        if m:
            return unescape(m.group(1))
    return ""


def get_published_iso(entry):
    now = datetime.now(timezone.utc)
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        dt = datetime(*parsed[:6], tzinfo=timezone.utc)
        return min(dt, now).isoformat()           # never in the future
    return now.isoformat()


def get_source_name(entry, default):
    src = entry.get("source")
    if isinstance(src, dict) and src.get("title"):
        return src["title"].strip()
    return default


# ============================================================
# SCAN
# ============================================================

stop_event = threading.Event()


def fetch(url):
    """GET with retries and back-off when the server says 'slow down'."""
    for attempt in range(1, 4):
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (429, 503):
            stop_event.wait(5 * attempt * attempt)   # 5s, 20s, 45s
            continue
        resp.raise_for_status()
        return resp.content
    raise RuntimeError("rate limited (429/503)")


def scan_feed(source, topic, url):
    content = fetch(url)
    stop_event.wait(REQUEST_DELAY)
    feed = feedparser.parse(content)

    articles = []
    for entry in feed.entries:
        title = clean_text(entry.get("title", ""))
        link = (entry.get("link") or "").strip()
        if not title or not link.startswith("http"):
            continue
        description = clean_text(entry.get("summary", ""))
        if description.lower().startswith(title.lower()[:40]):
            description = ""                      # Google News just repeats the title
        articles.append({
            "title": title,
            "description": description[:1000],
            "url": normalize_url(link),
            "image": get_image(entry),
            "source": get_source_name(entry, source),
            "topic": topic,
            "tags": {topic},
            "published": get_published_iso(entry),
        })
    return articles


def merge(into, articles):
    """One entry per URL; collect every topic it appeared under."""
    for a in articles:
        existing = into.get(a["url"])
        if existing is None:
            into[a["url"]] = a
        else:
            existing["tags"] |= a["tags"]
            if not existing["image"] and a["image"]:
                existing["image"] = a["image"]


# ============================================================
# MAIN LOOP
# ============================================================

def get_cursor():
    """Where the last run stopped (saved in Neon, so it survives between GitHub runs)."""
    try:
        conn = get_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT value FROM scanner_state WHERE key = 'cursor';")
                row = cur.fetchone()
                return row[0] if row else 0
        finally:
            conn.close()
    except Exception as e:
        log.warning("Could not read cursor: %s", e)
        return 0


def set_cursor(value):
    try:
        conn = get_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scanner_state (key, value) VALUES ('cursor', %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
                """, (value,))
        finally:
            conn.close()
    except Exception as e:
        log.warning("Could not save cursor: %s", e)


def run_cycle():
    feeds = build_feed_list()
    total = len(feeds)
    cursor = get_cursor() % max(total, 1)
    deadline = time.monotonic() + TIME_BUDGET
    ordered = feeds[cursor:] + feeds[:cursor]

    log.info("Scan started: %d feeds (%d workers)", total, MAX_WORKERS)
    done = new_total = found_total = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for i in range(0, total, CHUNK_SIZE):
            if stop_event.is_set() or time.monotonic() > deadline:
                log.warning("Time budget reached; remaining feeds continue next cycle.")
                break

            chunk = ordered[i:i + CHUNK_SIZE]
            tasks = {pool.submit(scan_feed, s, t, u): (s, t) for s, t, u in chunk}
            merged = {}
            for task in as_completed(tasks):
                try:
                    merge(merged, task.result())
                except Exception as e:
                    failed += 1
                    log.debug("FAIL %s: %s", tasks[task], e)

            try:
                new_total += save_articles(list(merged.values()))
            except Exception as e:
                log.error("Save failed for chunk: %s", e)

            found_total += len(merged)
            done += len(chunk)
            log.info("Progress %d/%d feeds | %d unique articles | %d new",
                     done, total, found_total, new_total)

    set_cursor((cursor + done) % max(total, 1))
    log.info("Scan finished: %d feeds done, %d failed, %d new articles | Database total: %d",
             done, failed, new_total, get_article_count())


def handle_stop(*_):
    log.info("Stopping...")
    stop_event.set()


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    create_tables()
    log.info("NEWS_HUB scanner started | every %d min | nothing is deleted",
             SCAN_INTERVAL // 60)

    if RUN_ONCE:
        try:
            run_cycle()
        except Exception as e:
            log.error("Scan failed: %s", e)
            raise
        return

    next_run = time.monotonic()

    while not stop_event.is_set():
        try:
            run_cycle()
        except Exception as e:
            log.error("Scan cycle failed: %s", e)

        next_run += SCAN_INTERVAL
        wait = next_run - time.monotonic()

        if wait < 0:
            next_run, wait = time.monotonic(), 0

        log.info("Next scan in %d minutes", round(wait / 60))
        stop_event.wait(wait)


if __name__ == "__main__":
    main()
