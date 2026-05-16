"""
Bitcoin News DAG
Sources:
  - The Guardian Open API (historical backfill + daily delta) — free, 500 req/day
  - Finnhub (daily delta) — free crypto news, 60 calls/min
  - AlphaVantage NEWS_SENTIMENT (daily delta) — free, 25 req/day per key;
      rotates randomly across ALPHAVANTAGE_API_KEYS list to stay under limit

Rate-limit budget per daily run (delta path):
  Guardian      :  ~2 req  (1-2 pages @ 200 articles)  → well within 500/day
  Finnhub       :  1 req   (one call for crypto news)   → well within 60/min
  AlphaVantage  :  1 req   (single time-windowed call)  → 1 of 25 per key/day

Historical backfill (Guardian only):
  3 months × ~1-3 pages/month ≈ 3-9 Guardian requests per run → safe.

Requires env vars:
  GUARDIAN_API_KEY        — register free at open-platform.theguardian.com
  FINNHUB_API_KEY         — register free at finnhub.io
  ALPHAVANTAGE_API_KEYS   — JSON list, e.g. ["KEY1","KEY2","KEY3"]
"""

import calendar
import json
import os
import random
import base64
import time
import requests
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bitcoin_news',
    default_args=default_args,
    description='Fetch Bitcoin news from Guardian + Finnhub + AlphaVantage, store in Snowflake',
    schedule='0 1 * * *',  # Daily at 01:00 UTC
    catchup=False,
    tags=['bitcoin', 'news', 'snowflake', 'production'],
)

GUARDIAN_URL         = "https://content.guardianapis.com/search"
AV_NEWS_URL          = "https://www.alphavantage.co/query"
GUARDIAN_START       = date(2014, 1, 1)   # Guardian's earliest meaningful Bitcoin coverage
MAX_MONTHS_PER_RUN   = 3                  # Months backfilled per daily run (~50 runs total)
DELTA_LOOKBACK_HOURS = 26                 # Buffer ensures no gaps between daily runs


# ── Infrastructure ─────────────────────────────────────────────────────────────

def ensure_schema_and_table(**context):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    hook.run("CREATE DATABASE IF NOT EXISTS BITCOIN_DATA")
    hook.run("CREATE SCHEMA IF NOT EXISTS BITCOIN_DATA.RAW")
    hook.run("""
        CREATE TABLE IF NOT EXISTS BITCOIN_DATA.RAW.BITCOIN_NEWS (
            datetime   TIMESTAMP,
            headline   VARCHAR(2000),
            summary    VARCHAR(10000),
            source     VARCHAR(500),
            url        VARCHAR(1000),
            categories VARCHAR(1000),
            tags       VARCHAR(1000),
            api_source VARCHAR(100),
            file_name  VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("✅ BITCOIN_DATA.RAW.BITCOIN_NEWS ensured")


# ── Branch ─────────────────────────────────────────────────────────────────────

def check_historical_data_exists(**context):
    """
    Route to historical backfill unless we already have Guardian articles going
    back to at least 2015. Once the backfill is done, only the delta path runs.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    try:
        result = hook.get_first("""
            SELECT COUNT(*), MIN(datetime)
            FROM BITCOIN_DATA.RAW.BITCOIN_NEWS
            WHERE api_source = 'guardian'
        """)
        count  = result[0] if result else 0
        min_dt = result[1] if result else None
        threshold = datetime(2015, 1, 1)

        if count > 0 and min_dt and min_dt < threshold:
            print(f"✅ History OK — oldest: {min_dt} ({count} articles). Running delta.")
            return ['fetch_guardian_delta', 'fetch_finnhub_news', 'fetch_alphavantage_news']

        print(f"⚠️ Insufficient history (oldest: {min_dt}, count: {count}). Running backfill.")
        return 'initialize_historical_guardian'

    except Exception as e:
        print(f"⚠️ Branch check error: {e}. Defaulting to backfill.")
        return 'initialize_historical_guardian'


# ── Guardian helpers ───────────────────────────────────────────────────────────

def _guardian_key():
    key = os.getenv('GUARDIAN_API_KEY')
    if not key:
        raise ValueError("GUARDIAN_API_KEY env var not set — register free at open-platform.theguardian.com")
    return key


def _fetch_guardian_page(api_key, from_date, to_date, page=1):
    """Fetch one page of Guardian bitcoin articles for a given date range."""
    resp = requests.get(GUARDIAN_URL, params={
        'q':           'bitcoin',
        'api-key':     api_key,
        'show-fields': 'headline,trailText',
        'page-size':   200,
        'from-date':   from_date.isoformat(),
        'to-date':     to_date.isoformat(),
        'order-by':    'oldest',
        'page':        page,
    }, timeout=15)
    resp.raise_for_status()
    body = resp.json()
    if body.get('response', {}).get('status') != 'ok':
        raise ValueError(f"Guardian API error: {body}")
    return body['response']


def _guardian_article_to_value_string(article, file_name):
    """Convert a Guardian API result dict to a SQL VALUES(...) string."""
    pub_raw  = article.get('webPublicationDate', '')[:19].replace('T', ' ')
    fields   = article.get('fields', {})
    headline = (fields.get('headline') or article.get('webTitle', '')).replace("'", "''")[:1999]
    summary  = (fields.get('trailText') or '').replace("'", "''")[:9999]
    url      = article.get('webUrl', '').replace("'", "''")[:999]
    category = article.get('sectionName', '').replace("'", "''")[:999]
    fname    = file_name.replace("'", "''")

    return (
        f"('{pub_raw}', '{headline}', '{summary}', 'The Guardian', '{url}', "
        f"'{category}', '', 'guardian', '{fname}')"
    )


def _bulk_merge_to_snowflake(hook, value_strings):
    """MERGE rows into BITCOIN_NEWS; dedup by URL (never overwrites existing rows)."""
    if not value_strings:
        return
    bulk = ',\n  '.join(value_strings)
    hook.run(f"""
        MERGE INTO BITCOIN_DATA.RAW.BITCOIN_NEWS AS t
        USING (
            SELECT column1 AS dt,        column2 AS headline, column3 AS summary,
                   column4 AS source,    column5 AS url,      column6 AS categories,
                   column7 AS tags,      column8 AS api_source, column9 AS file_name
            FROM VALUES
            {bulk}
        ) AS s ON t.url = s.url
        WHEN NOT MATCHED THEN INSERT
            (datetime, headline, summary, source, url, categories, tags, api_source, file_name)
        VALUES
            (TRY_TO_TIMESTAMP(s.dt), s.headline, s.summary, s.source,
             s.url, s.categories, s.tags, s.api_source, s.file_name)
    """)


# ── Month arithmetic helpers ───────────────────────────────────────────────────

def _month_first_last(year, month):
    first = date(year, month, 1)
    last  = date(year, month, calendar.monthrange(year, month)[1])
    return first, last


def _prev_month(year, month):
    return (year - 1, 12) if month == 1 else (year, month - 1)


# ── AlphaVantage helpers ───────────────────────────────────────────────────────

def _alphavantage_key():
    """Pick a random AlphaVantage API key from ALPHAVANTAGE_API_KEYS list."""
    keys_raw = os.getenv('ALPHAVANTAGE_API_KEYS', '[]').strip()
    try:
        keys = json.loads(keys_raw)
    except (json.JSONDecodeError, ValueError):
        keys = [k.strip().strip('"\'') for k in keys_raw.strip('[]').split(',')]

    valid = [k for k in keys if k and k not in ('API_KEY_1', 'API_KEY_2', 'API_KEY_3')]
    if not valid:
        raise ValueError("ALPHAVANTAGE_API_KEYS not set or contains only placeholder values")

    chosen = random.choice(valid)
    print(f"AlphaVantage: selected key ...{chosen[-4:]}")
    return chosen


def _parse_av_datetime(pub_str):
    """Parse AlphaVantage time_published (YYYYMMDDTHHMMSS or YYYYMMDDTHHMM)."""
    for fmt in ('%Y%m%dT%H%M%S', '%Y%m%dT%H%M'):
        try:
            return datetime.strptime(pub_str[:len(fmt.replace('%', 'XX').replace('XX', '00'))], fmt)
        except ValueError:
            continue
    # fallback: try stripping to known lengths
    for length, fmt in [(15, '%Y%m%dT%H%M%S'), (13, '%Y%m%dT%H%M')]:
        try:
            return datetime.strptime(pub_str[:length], fmt)
        except ValueError:
            continue
    return None


# ── Path A: Historical backfill ────────────────────────────────────────────────

def initialize_historical_guardian(**context):
    """
    Fetch Guardian BTC news month by month going backwards from:
      - The oldest datetime already in Snowflake (to continue a partial backfill)
      - Or today (first run)
    Stops after MAX_MONTHS_PER_RUN months or when GUARDIAN_START is reached.
    """
    api_key = _guardian_key()
    hook    = SnowflakeHook(snowflake_conn_id='snowflake_default')

    result = hook.get_first("""
        SELECT MIN(datetime)
        FROM BITCOIN_DATA.RAW.BITCOIN_NEWS
        WHERE api_source = 'guardian'
    """)
    min_stored = result[0] if (result and result[0]) else None
    pivot_date = min_stored.date() - timedelta(days=1) if min_stored else date.today()

    print(f"📅 Backfill starting from {pivot_date} going back to {GUARDIAN_START}")

    total_inserted = 0
    file_name = f"guardian_historical_{datetime.now().strftime('%Y-%m-%d')}.csv"

    for i in range(MAX_MONTHS_PER_RUN):
        year, month = pivot_date.year, pivot_date.month
        month_first, month_last = _month_first_last(year, month)

        if month_last < GUARDIAN_START:
            print("🎉 Historical backfill reached GUARDIAN_START — complete!")
            break
        month_first = max(month_first, GUARDIAN_START)

        print(f"📥 Month {i+1}/{MAX_MONTHS_PER_RUN}: {month_first} → {month_last}")
        page = 1
        month_total = 0

        while True:
            resp_body = _fetch_guardian_page(api_key, month_first, month_last, page=page)
            results   = resp_body.get('results', [])
            if not results:
                break

            vals = [_guardian_article_to_value_string(a, file_name) for a in results]
            _bulk_merge_to_snowflake(hook, vals)
            month_total   += len(results)
            total_inserted += len(results)

            total_pages = resp_body.get('pages', 1)
            print(f"  page {page}/{total_pages} — {len(results)} articles")
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)   # Guardian: stay well under 500 req/day

        print(f"✅ Month {month_first.strftime('%Y-%m')}: {month_total} articles")

        prev_year, prev_month = _prev_month(year, month)
        _, prev_last = _month_first_last(prev_year, prev_month)
        pivot_date = prev_last
        if pivot_date < GUARDIAN_START:
            print("🎉 Reached GUARDIAN_START — historical backfill complete!")
            break

        time.sleep(1)   # brief pause between months

    remaining_months = max(
        0,
        (pivot_date.year - GUARDIAN_START.year) * 12 + (pivot_date.month - GUARDIAN_START.month)
    )
    if remaining_months > 0:
        print(f"⏳ {remaining_months} months still to backfill. DAG continues tomorrow.")
    print(f"📊 This run: {total_inserted} articles inserted/updated")

    context['task_instance'].xcom_push(key='init_count', value=total_inserted)
    return total_inserted


# ── Path B: Daily delta ────────────────────────────────────────────────────────

def fetch_guardian_delta(**context):
    """Fetch Guardian BTC news published in the last DELTA_LOOKBACK_HOURS hours."""
    api_key = _guardian_key()
    cutoff  = datetime.utcnow() - timedelta(hours=DELTA_LOOKBACK_HOURS)
    from_dt = cutoff.date()
    to_dt   = date.today()

    print(f"🔍 Guardian delta — {from_dt} → {to_dt}")

    all_articles = []
    page = 1
    while True:
        resp_body = _fetch_guardian_page(api_key, from_dt, to_dt, page=page)
        results   = resp_body.get('results', [])
        if not results:
            break

        for a in results:
            pub_str = a.get('webPublicationDate', '')[:19]
            try:
                pub_dt = datetime.strptime(pub_str, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                continue
            if pub_dt >= cutoff:
                all_articles.append(a)

        if page >= resp_body.get('pages', 1):
            break
        page += 1
        time.sleep(0.5)   # Guardian: stay under 500 req/day

    print(f"✅ Guardian delta: {len(all_articles)} articles")

    transformed = [
        {
            'datetime':   a.get('webPublicationDate', '')[:19].replace('T', ' '),
            'headline':   (a.get('fields', {}).get('headline') or a.get('webTitle', '')),
            'summary':    a.get('fields', {}).get('trailText', ''),
            'source':     'The Guardian',
            'url':        a.get('webUrl', ''),
            'categories': a.get('sectionName', ''),
            'tags':       '',
            'api_source': 'guardian',
        }
        for a in all_articles
    ]
    context['task_instance'].xcom_push(key='guardian_news', value=transformed)
    return len(transformed)


def _get_snowflake_max_datetime():
    try:
        hook   = SnowflakeHook(snowflake_conn_id='snowflake_default')
        result = hook.get_first(
            "SELECT COALESCE(MAX(datetime), '1970-01-01'::TIMESTAMP) FROM BITCOIN_DATA.RAW.BITCOIN_NEWS"
        )
        return result[0] if (result and result[0]) else datetime(1970, 1, 1)
    except Exception:
        return datetime(1970, 1, 1)


def fetch_finnhub_news(**context):
    """Fetch Bitcoin news from Finnhub (60 calls/min free tier — 1 call/day here)."""
    last_dt = _get_snowflake_max_datetime()
    api_key = os.getenv('FINNHUB_API_KEY')
    if not api_key:
        print("Finnhub API key not found — skipping")
        context['task_instance'].xcom_push(key='finnhub_news', value=[])
        return 0

    try:
        resp = requests.get(
            f"https://finnhub.io/api/v1/news?category=crypto&token={api_key}",
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        articles = []
        for item in (data if isinstance(data, list) else []):
            pub_dt = datetime.fromtimestamp(item.get('datetime', 0))
            if pub_dt > last_dt and item.get('summary', '').strip():
                articles.append({
                    'datetime':   pub_dt.isoformat(),
                    'headline':   item.get('headline', ''),
                    'summary':    item.get('summary', ''),
                    'source':     item.get('source', ''),
                    'url':        item.get('url', ''),
                    'categories': '',
                    'tags':       '',
                    'api_source': 'finnhub',
                })
        print(f"✅ Finnhub: {len(articles)} new articles")
        context['task_instance'].xcom_push(key='finnhub_news', value=articles)
        return len(articles)

    except Exception as e:
        print(f"Finnhub fetch failed: {e}")
        context['task_instance'].xcom_push(key='finnhub_news', value=[])
        return 0


def fetch_alphavantage_news(**context):
    """
    Fetch Bitcoin news from AlphaVantage NEWS_SENTIMENT API.
    Uses a randomly chosen key from ALPHAVANTAGE_API_KEYS (25 req/day per key).
    One call per run covers the full DELTA_LOOKBACK_HOURS window (up to 1000 articles).
    """
    try:
        api_key = _alphavantage_key()
    except ValueError as e:
        print(f"AlphaVantage keys not configured: {e} — skipping")
        context['task_instance'].xcom_push(key='av_news', value=[])
        return 0

    cutoff   = datetime.utcnow() - timedelta(hours=DELTA_LOOKBACK_HOURS)
    time_from = cutoff.strftime('%Y%m%dT%H%M')
    time_to   = datetime.utcnow().strftime('%Y%m%dT%H%M')

    print(f"🔍 AlphaVantage delta — {time_from} → {time_to}")

    try:
        resp = requests.get(AV_NEWS_URL, params={
            'function':  'NEWS_SENTIMENT',
            'tickers':   'CRYPTO:BTC',
            'topics':    'blockchain',
            'time_from': time_from,
            'time_to':   time_to,
            'sort':      'LATEST',
            'limit':     1000,
            'apikey':    api_key,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Rate-limit or auth error returned as JSON body
        if 'Information' in data or 'Note' in data:
            msg = data.get('Information') or data.get('Note', '')
            print(f"⚠️ AlphaVantage limit/auth: {msg} — skipping this run")
            context['task_instance'].xcom_push(key='av_news', value=[])
            return 0

        feed = data.get('feed', [])
        articles = []
        for item in feed:
            pub_dt = _parse_av_datetime(item.get('time_published', ''))
            if pub_dt is None or pub_dt < cutoff:
                continue

            topics   = ','.join(t.get('topic', '') for t in item.get('topics', []))
            tickers  = ','.join(t.get('ticker', '') for t in item.get('ticker_sentiment', []))
            articles.append({
                'datetime':   pub_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'headline':   item.get('title', ''),
                'summary':    item.get('summary', ''),
                'source':     item.get('source', ''),
                'url':        item.get('url', ''),
                'categories': topics,
                'tags':       tickers,
                'api_source': 'alphavantage',
            })

        print(f"✅ AlphaVantage: {len(articles)} articles in last {DELTA_LOOKBACK_HOURS}h")
        context['task_instance'].xcom_push(key='av_news', value=articles)
        return len(articles)

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            print("AlphaVantage HTTP 429 — rate limited, skipping")
        else:
            print(f"AlphaVantage HTTP error: {e}")
        context['task_instance'].xcom_push(key='av_news', value=[])
        return 0
    except Exception as e:
        print(f"AlphaVantage fetch failed: {e}")
        context['task_instance'].xcom_push(key='av_news', value=[])
        return 0


def merge_and_deduplicate_news(**context):
    """Combine Guardian + Finnhub + AlphaVantage articles, deduplicate by URL."""
    g_news  = context['task_instance'].xcom_pull(task_ids='fetch_guardian_delta',     key='guardian_news') or []
    fh_news = context['task_instance'].xcom_pull(task_ids='fetch_finnhub_news',        key='finnhub_news')  or []
    av_news = context['task_instance'].xcom_pull(task_ids='fetch_alphavantage_news',   key='av_news')       or []

    seen, unique = set(), []
    for item in g_news + fh_news + av_news:
        key = item.get('url', '').strip().lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(item)

    print(f"Guardian: {len(g_news)} | Finnhub: {len(fh_news)} | AlphaVantage: {len(av_news)} | Unique: {len(unique)}")
    context['task_instance'].xcom_push(key='unique_news', value=unique)
    context['task_instance'].xcom_push(key='g_count',     value=len(g_news))
    context['task_instance'].xcom_push(key='fh_count',    value=len(fh_news))
    context['task_instance'].xcom_push(key='av_count',    value=len(av_news))
    context['task_instance'].xcom_push(key='total_count', value=len(unique))
    return len(unique)


def insert_news_to_snowflake(**context):
    """Bulk MERGE unique delta articles — insert-if-not-exists by URL."""
    unique = context['task_instance'].xcom_pull(task_ids='merge_and_deduplicate_news', key='unique_news') or []
    if not unique:
        print("No new articles to insert")
        context['task_instance'].xcom_push(key='inserted_count', value=0)
        context['task_instance'].xcom_push(key='failed_count',   value=0)
        return 0

    hook      = SnowflakeHook(snowflake_conn_id='snowflake_default')
    file_name = f"airflow_daily_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"

    value_strings = []
    for item in unique:
        dt       = str(item.get('datetime', '')).replace("'", "''")
        headline = item.get('headline', '').replace("'", "''")[:1999]
        summary  = item.get('summary',  '').replace("'", "''")[:9999]
        source   = item.get('source',   '').replace("'", "''")[:499]
        url      = item.get('url',      '').replace("'", "''")[:999]
        cats     = item.get('categories','').replace("'", "''")[:999]
        tags     = item.get('tags',      '').replace("'", "''")[:999]
        api_src  = item.get('api_source','')[:99]
        fname    = file_name.replace("'", "''")
        value_strings.append(
            f"('{dt}', '{headline}', '{summary}', '{source}', '{url}', "
            f"'{cats}', '{tags}', '{api_src}', '{fname}')"
        )

    _bulk_merge_to_snowflake(hook, value_strings)
    print(f"✅ Merged {len(unique)} delta articles into Snowflake")
    context['task_instance'].xcom_push(key='inserted_count', value=len(unique))
    context['task_instance'].xcom_push(key='failed_count',   value=0)
    return len(unique)


# ── GitHub backup ──────────────────────────────────────────────────────────────

def backup_to_github(**context):
    unique = context['task_instance'].xcom_pull(task_ids='merge_and_deduplicate_news', key='unique_news') or []
    if not unique:
        return "No data to backup"

    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        print("GitHub token not found — skipping backup")
        return "Skipped"

    now      = datetime.now() + timedelta(hours=2)
    date_str = now.strftime('%Y-%m-%d_%H-%M')
    folder   = now.strftime('%d%m%Y')

    rows = ['datetime,headline,summary,source,url,categories,tags,api_source']
    for item in unique:
        row = [
            item.get('datetime', ''),
            f'"{item.get("headline","").replace(chr(34), chr(34)*2)}"',
            f'"{item.get("summary","").replace(chr(34), chr(34)*2)}"',
            item.get('source', '').replace(',', ''),
            item.get('url', ''),
            f'"{item.get("categories","")}"',
            f'"{item.get("tags","")}"',
            item.get('api_source', ''),
        ]
        rows.append(','.join(row))

    content_b64 = base64.b64encode('\n'.join(rows).encode()).decode()
    file_path   = f"{folder}/bitcoin_news_{date_str}.csv"
    api_url     = f"https://api.github.com/repos/mouadja02/bitcoin-news-data/contents/{file_path}"
    headers     = {'Authorization': f'token {github_token}', 'Accept': 'application/vnd.github.v3+json'}
    body        = {'message': f'Backup: BTC news {date_str}', 'content': content_b64}

    try:
        check = requests.get(api_url, headers=headers, timeout=15)
        if check.status_code == 200:
            body['sha'] = check.json()['sha']
        requests.put(api_url, headers=headers, json=body, timeout=15).raise_for_status()
        print(f"✅ Backed up {len(unique)} articles to GitHub")
        return f"Backed up {len(unique)} articles"
    except Exception as e:
        print(f"GitHub backup failed: {e}")
        return f"Backup failed: {e}"


# ── Notification ───────────────────────────────────────────────────────────────

def send_production_notification(**context):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id   = os.getenv('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        return

    ti         = context['task_instance']
    init_count = ti.xcom_pull(task_ids='initialize_historical_guardian', key='init_count')
    g_count    = ti.xcom_pull(task_ids='merge_and_deduplicate_news', key='g_count')
    fh_count   = ti.xcom_pull(task_ids='merge_and_deduplicate_news', key='fh_count')
    av_count   = ti.xcom_pull(task_ids='merge_and_deduplicate_news', key='av_count')
    inserted   = ti.xcom_pull(task_ids='insert_news_to_snowflake',   key='inserted_count')
    now_str    = datetime.now().strftime('%Y-%m-%d %H:%M UTC')

    if init_count is not None:
        msg = (
            f"✅ BTC News — Backfill batch complete\n"
            f"🕐 {now_str}\n"
            f"📰 {init_count} Guardian articles ingested this run"
        )
    else:
        msg = (
            f"✅ BTC News — Daily update\n"
            f"🕐 {now_str}\n"
            f"📰 Guardian: {g_count or 0} | Finnhub: {fh_count or 0} | AlphaVantage: {av_count or 0}\n"
            f"❄️  Snowflake: {inserted or 0} new articles merged"
        )

    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data={'chat_id': chat_id, 'text': msg},
            timeout=10
        ).raise_for_status()
        print("Telegram notification sent")
    except Exception as e:
        print(f"Telegram notification failed: {e}")


# ── Task definitions ───────────────────────────────────────────────────────────

ensure_db_task = PythonOperator(
    task_id='ensure_schema_and_table',
    python_callable=ensure_schema_and_table,
    dag=dag,
)

check_data_task = BranchPythonOperator(
    task_id='check_historical_data',
    python_callable=check_historical_data_exists,
    dag=dag,
)

# Path A — historical backfill (Guardian only)
init_historical_task = PythonOperator(
    task_id='initialize_historical_guardian',
    python_callable=initialize_historical_guardian,
    dag=dag,
)

# Path B — daily delta (three parallel fetches)
fetch_guardian_delta_task = PythonOperator(
    task_id='fetch_guardian_delta',
    python_callable=fetch_guardian_delta,
    dag=dag,
)

fetch_finnhub_task = PythonOperator(
    task_id='fetch_finnhub_news',
    python_callable=fetch_finnhub_news,
    dag=dag,
)

fetch_alphavantage_task = PythonOperator(
    task_id='fetch_alphavantage_news',
    python_callable=fetch_alphavantage_news,
    dag=dag,
)

merge_dedup_task = PythonOperator(
    task_id='merge_and_deduplicate_news',
    python_callable=merge_and_deduplicate_news,
    dag=dag,
)

insert_snowflake_task = PythonOperator(
    task_id='insert_news_to_snowflake',
    python_callable=insert_news_to_snowflake,
    dag=dag,
)

backup_github_task = PythonOperator(
    task_id='backup_to_github',
    python_callable=backup_to_github,
    dag=dag,
)

join_task = EmptyOperator(
    task_id='join_paths',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

notification_task = PythonOperator(
    task_id='send_production_notification',
    python_callable=send_production_notification,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# ── Dependencies ───────────────────────────────────────────────────────────────
#
#  ensure_schema_and_table
#      >> check_historical_data
#           ├── initialize_historical_guardian ─────────────────────────────────► join_paths
#           └── [fetch_guardian_delta, fetch_finnhub_news, fetch_alphavantage_news]
#                    >> merge_and_deduplicate_news
#                    >> insert_news_to_snowflake
#                    >> backup_to_github ─────────────────────────────────────► join_paths
#  join_paths >> send_production_notification
#

ensure_db_task >> check_data_task >> [
    init_historical_task,
    fetch_guardian_delta_task,
    fetch_finnhub_task,
    fetch_alphavantage_task,
]

init_historical_task >> join_task

[fetch_guardian_delta_task, fetch_finnhub_task, fetch_alphavantage_task] >> merge_dedup_task >> insert_snowflake_task >> backup_github_task >> join_task

join_task >> notification_task
