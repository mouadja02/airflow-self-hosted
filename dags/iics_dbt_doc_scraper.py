"""
Airflow DAG: dbt & IICS Documentation Scraper — IICS → dbt Migration Scope
Runs weekly to update Pinecone vector database for a technical RAG agent
focused on migrating Informatica IICS mappings to dbt projects.

IICS scope  : mappings, transformations, connectors, expression language,
              PowerCenter migration, parameters, pushdown optimization
dbt scope   : models, sources, incremental/snapshot strategies, Jinja/macros,
              project config, adapters (Snowflake / Redshift / BigQuery),
              tests, contracts, hooks — everything needed to reproduce
              IICS logic as dbt code.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime
import requests
import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
import hashlib
import re

# ---------------------------------------------------------------------------
# DAG default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "dataops",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "dbt_iics_docs_scraper",
    default_args=default_args,
    description="Scrape dbt and IICS documentation and update Pinecone (Delta mode)",
    schedule="0 4 * * 0",  # Every Sunday at 4 AM (offset from SAA @ 3 AM, Snowflake @ 2 AM)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "iics", "informatica", "scraping", "pinecone", "rag"],
)

# ---------------------------------------------------------------------------
# Source configuration
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# dbt — single sitemap, full coverage then filtered by topic
# ---------------------------------------------------------------------------
DBT_SITEMAPS = {
    "dbt-core": "https://docs.getdbt.com/sitemap.xml",
}

# ---------------------------------------------------------------------------
# IICS — only the guides that matter for mapping migration:
#   • Data Integration (mappings, tasks, transformations)
#   • Data Integration Connectors (source/target config)
#   • PowerCenter migration guide
# ---------------------------------------------------------------------------
IICS_SITEMAPS = {
    "iics-data-integration": "https://docs.informatica.com/integration-cloud/data-integration/current-version/sitemap.xml",
    "iics-connectors":        "https://docs.informatica.com/integration-cloud/data-integration-connectors/current-version/sitemap.xml",
    "iics-pc-migration":      "https://docs.informatica.com/data-integration/powercenter/current-version/sitemap.xml",
}

# ---------------------------------------------------------------------------
# Fallback seed URLs — used when a sitemap returns non-200
# ---------------------------------------------------------------------------
FALLBACK_SEEDS = {
    # dbt — pages directly relevant to rewriting IICS logic
    "dbt-core": [
        "https://docs.getdbt.com/docs/introduction",
        "https://docs.getdbt.com/docs/build/models",
        "https://docs.getdbt.com/docs/build/sql-models",
        "https://docs.getdbt.com/docs/build/python-models",
        "https://docs.getdbt.com/docs/build/sources",
        "https://docs.getdbt.com/docs/build/tests",
        "https://docs.getdbt.com/docs/build/snapshots",
        "https://docs.getdbt.com/docs/build/jinja-macros",
        "https://docs.getdbt.com/reference/model-configs",
        "https://docs.getdbt.com/reference/source-configs",
        "https://docs.getdbt.com/reference/project-configs",
        "https://docs.getdbt.com/reference/dbt-commands",
        "https://docs.getdbt.com/reference/resource-configs/incremental_strategy",
        "https://docs.getdbt.com/reference/resource-configs/on_schema_change",
        "https://docs.getdbt.com/docs/build/incremental-models",
        "https://docs.getdbt.com/docs/build/incremental-strategy",
        "https://docs.getdbt.com/reference/resource-configs/snowflake-configs",
        "https://docs.getdbt.com/reference/resource-configs/bigquery-configs",
        "https://docs.getdbt.com/reference/resource-configs/redshift-configs",
        "https://docs.getdbt.com/docs/build/hooks-operations",
        "https://docs.getdbt.com/reference/resource-configs/grants",
        "https://docs.getdbt.com/docs/build/contracts",
        "https://docs.getdbt.com/docs/build/packages",
        "https://docs.getdbt.com/reference/dbt-jinja-functions/ref",
        "https://docs.getdbt.com/reference/dbt-jinja-functions/source",
        "https://docs.getdbt.com/reference/dbt-jinja-functions/var",
        "https://docs.getdbt.com/reference/dbt-jinja-functions/env_var",
        "https://docs.getdbt.com/reference/dbt-jinja-functions/run_query",
    ],
    # IICS — core mapping & transformation pages
    "iics-data-integration": [
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings-overview.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/mapping-tasks/mapping-tasks-overview.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/transformations-overview.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/expression-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/filter-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/joiner-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/aggregator-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/router-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/lookup-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/union-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/sorter-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/rank-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/update-strategy-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/sequence-generator-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/normalizer-transformation.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/mapping-parameters-and-variables/mapping-parameters-and-variables-overview.html",
        "https://docs.informatica.com/integration-cloud/data-integration/current-version/pushdown-optimization/pushdown-optimization-overview.html",
    ],
    "iics-pc-migration": [
        "https://docs.informatica.com/data-integration/powercenter/current-version/pc-to-iics-migration-guide/migration-overview.html",
    ],
}

# ---------------------------------------------------------------------------
# dbt topic filters — focused on what you need to reproduce IICS logic
# ---------------------------------------------------------------------------
DBT_RELEVANT_TOPICS = [
    # Building blocks
    "models", "sources", "tests", "snapshots", "seeds", "macros",
    # Incremental / merge strategies — key for IICS CDC mappings
    "incremental", "incremental-strategy", "merge", "insert-overwrite",
    "on_schema_change", "unique_key", "partition",
    # Jinja / templating — replaces IICS expression language
    "jinja", "ref", "source", "var", "env_var", "run_query",
    "statement", "adapter", "execute",
    # SQL patterns that map to IICS transformations
    "join", "union", "filter", "aggregate", "window",
    "deduplication", "surrogate-key", "hash",
    # Project structure
    "project", "profiles", "dbt_project", "packages", "dependencies",
    "config", "schema", "database", "alias",
    # Materializations
    "table", "view", "incremental", "ephemeral", "materialized",
    # Testing & data quality — replaces IICS validation
    "tests", "generic-tests", "singular-tests",
    "not_null", "unique", "accepted_values", "relationships",
    "dbt-expectations", "elementary",
    # Hooks (replaces IICS pre/post-session SQL)
    "hooks", "operations", "on-run-start", "on-run-end",
    # Contracts & constraints
    "contracts", "constraints", "grants",
    # Adapter-specific configs used in migrations
    "snowflake-configs", "bigquery-configs", "redshift-configs",
    "databricks-configs", "merge-behavior",
    # Commands used during migration workflow
    "run", "test", "compile", "debug", "build", "deps",
    # Docs generation
    "generate", "serve", "documentation",
    # Getting started / concepts
    "introduction", "getting-started", "tutorial", "best-practice",
    "style-guide", "project-structure",
]

# ---------------------------------------------------------------------------
# IICS topic filters — only mapping & migration relevant content
# ---------------------------------------------------------------------------
IICS_RELEVANT_TOPICS = [
    # Mapping anatomy
    "mapping", "mapping-overview", "mapping-task",
    "source-transformation", "target-transformation",
    # All transformations (each maps to SQL logic in dbt)
    "expression-transformation", "filter-transformation",
    "joiner-transformation", "aggregator-transformation",
    "router-transformation", "lookup-transformation",
    "union-transformation", "sorter-transformation",
    "rank-transformation", "update-strategy-transformation",
    "sequence-generator", "normalizer-transformation",
    "hierarchy-parser", "hierarchy-builder",
    "data-masking-transformation", "validate-transformation",
    # Generic transformation terms
    "transformation", "expression", "filter", "joiner", "sorter",
    "aggregator", "router", "union", "lookup", "rank",
    "sequence-generator", "normalizer", "update-strategy",
    # Mapping parameters & variables — maps to dbt vars/env_vars
    "parameter", "variable", "mapping-parameter", "input-field",
    "parameterization",
    # Pushdown & optimization — helps understand what logic can be rewritten as SQL
    "pushdown", "pushdown-optimization", "sql-override",
    # Connectivity (helps map source/target to dbt sources/seeds)
    "connector", "connection", "source", "target",
    "snowflake", "redshift", "bigquery", "s3", "oracle",
    "sql-server", "mysql", "postgresql", "salesforce", "jdbc",
    # PowerCenter migration — direct migration context
    "powercenter", "migration", "migrate", "convert", "import",
    "pc-to-iics", "upgrade",
    # Expression language — must translate to Jinja/SQL in dbt
    "expression-language", "built-in-function", "string-function",
    "date-function", "numeric-function", "null-function",
    "decode", "iif", "in", "instr", "ltrim", "rtrim",
    "to_date", "to_char", "trunc", "sysdate",
    # CDC & incremental patterns
    "cdc", "change-data-capture", "incremental", "delta",
    "insert", "update", "delete", "upsert",
    # General
    "overview", "introduction", "getting-started", "concepts",
    "best-practice", "example", "guide",
]

# ---------------------------------------------------------------------------
# Helper: Pinecone & OpenAI clients
# ---------------------------------------------------------------------------

def get_pinecone_client():
    api_key = os.environ.get("PINECONE_API_KEY")
    return Pinecone(api_key=api_key)


def get_openai_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


# ---------------------------------------------------------------------------
# Task 1 – fetch URLs from sitemaps
# ---------------------------------------------------------------------------

def fetch_sitemaps(**context):
    """
    Fetch URLs from dbt and IICS sitemaps.
    Falls back to seed URLs when a sitemap is unavailable.
    """
    print("Fetching sitemaps for dbt and IICS...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    all_source_configs = {
        **{k: {"sitemap": v, "type": "dbt"} for k, v in DBT_SITEMAPS.items()},
        **{k: {"sitemap": v, "type": "iics"} for k, v in IICS_SITEMAPS.items()},
    }

    all_urls = []  # list of {"url": ..., "service": ..., "type": ...}
    failed_sitemaps = []

    def _parse_sitemap(content, service_name, service_type):
        """Parse sitemap XML and return list of url dicts."""
        urls = []
        try:
            root = ET.fromstring(content)
            # Handle both sitemap index and urlset
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//ns:url/ns:loc", ns):
                clean_url = loc.text.strip().replace("\n", "")
                urls.append({"url": clean_url, "service": service_name, "type": service_type})
        except ET.ParseError as e:
            print(f"  XML parse error for {service_name}: {e}")
        return urls

    for service_name, cfg in all_source_configs.items():
        sitemap_url = cfg["sitemap"]
        service_type = cfg["type"]

        print(f"Fetching sitemap for [{service_type}] {service_name} ...")
        try:
            response = session.get(sitemap_url, timeout=30)
            if response.status_code == 200:
                parsed = _parse_sitemap(response.content, service_name, service_type)
                all_urls.extend(parsed)
                print(f"  Found {len(parsed)} URLs")
            else:
                print(f"  Warning: status {response.status_code}, falling back to seeds")
                failed_sitemaps.append(service_name)
        except Exception as e:
            print(f"  Error: {e}, falling back to seeds")
            failed_sitemaps.append(service_name)

        time.sleep(0.3)

    # Add fallback seeds for failed sitemaps
    for service_name in failed_sitemaps:
        seeds = FALLBACK_SEEDS.get(service_name, [])
        service_type = "dbt" if service_name.startswith("dbt") else "iics"
        for seed_url in seeds:
            all_urls.append({"url": seed_url, "service": service_name, "type": service_type})
        if seeds:
            print(f"  Added {len(seeds)} seed URLs for {service_name}")

    print(f"\nTotal URLs before filtering: {len(all_urls)}")

    # Deduplicate by URL
    seen = set()
    unique_urls = []
    for u in all_urls:
        if u["url"] not in seen:
            seen.add(u["url"])
            unique_urls.append(u)

    # Filter by relevance
    filtered = []
    skip_patterns = ["/release-notes/", "/api-reference/", "/changelog/", "/whats-new/"]

    for url_info in unique_urls:
        url_lower = url_info["url"].lower()

        if any(skip in url_lower for skip in skip_patterns):
            continue

        relevant_topics = DBT_RELEVANT_TOPICS if url_info["type"] == "dbt" else IICS_RELEVANT_TOPICS
        if any(topic in url_lower for topic in relevant_topics):
            filtered.append(url_info)

    print(f"Filtered to {len(filtered)} relevant URLs ({sum(1 for u in filtered if u['type'] == 'dbt')} dbt, {sum(1 for u in filtered if u['type'] == 'iics')} IICS)")

    context["task_instance"].xcom_push(key="all_urls", value=filtered)
    context["task_instance"].xcom_push(key="failed_sitemaps", value=failed_sitemaps)

    return len(filtered)


# ---------------------------------------------------------------------------
# Task 2 – compute delta
# ---------------------------------------------------------------------------

def compute_delta_urls(**context):
    """
    Compare fetched URLs against previously scraped ones stored in an
    Airflow Variable and return only new URLs.
    """
    all_urls = context["task_instance"].xcom_pull(
        task_ids="fetch_sitemaps", key="all_urls"
    )

    try:
        scraped_metadata = Variable.get("dbt_iics_scraped_urls", deserialize_json=True) or {}
    except Exception:
        scraped_metadata = {}

    new_urls = [u for u in all_urls if u["url"] not in scraped_metadata]

    print(f"Delta: {len(new_urls)} new URLs to scrape")
    print(f"Previously scraped: {len(scraped_metadata)} URLs")

    # Cap per-run volume to keep task times reasonable
    MAX_URLS_PER_RUN = 800
    if len(new_urls) > MAX_URLS_PER_RUN:
        print(f"Capping to {MAX_URLS_PER_RUN} URLs for this run")
        new_urls = new_urls[:MAX_URLS_PER_RUN]

    context["task_instance"].xcom_push(key="delta_urls", value=new_urls)
    context["task_instance"].xcom_push(key="scraped_metadata", value=scraped_metadata)

    return len(new_urls)


# ---------------------------------------------------------------------------
# Task 3 – scrape pages
# ---------------------------------------------------------------------------

def scrape_pages(**context):
    """
    Sequentially scrape delta URLs and extract structured content sections.
    Handles both docs.getdbt.com (React/Docusaurus) and docs.informatica.com layouts.
    """
    delta_urls = context["task_instance"].xcom_pull(
        task_ids="compute_delta_urls", key="delta_urls"
    )

    if not delta_urls:
        print("No new URLs to scrape")
        context["task_instance"].xcom_push(key="scraped_pages", value=[])
        return 0

    print(f"Scraping {len(delta_urls)} pages...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    def clean_text(element):
        for tag in element(["script", "style", "nav", "footer", "header", "aside", "noscript"]):
            tag.decompose()
        text = element.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return " ".join(chunk for chunk in chunks if chunk)

    def clean_title(raw_title, service_type):
        """Strip site-name suffixes from page titles."""
        suffixes = [
            r"\s*[-–|]\s*dbt Docs.*$",
            r"\s*[-–|]\s*Informatica.*$",
            r"\s*[-–|]\s*dbt Developer Hub.*$",
        ]
        for suffix in suffixes:
            raw_title = re.sub(suffix, "", raw_title, flags=re.IGNORECASE)
        return raw_title.strip()

    def find_main_content(soup, service_type):
        """Locate the primary content container for each doc site."""
        if service_type == "dbt":
            # Docusaurus layout used by docs.getdbt.com
            return (
                soup.find("article")
                or soup.find("div", {"class": re.compile(r"docItemContainer|markdown")})
                or soup.find("main")
                or soup.find("body")
            )
        else:
            # Informatica docs layout
            return (
                soup.find("div", {"id": re.compile(r"content|main", re.I)})
                or soup.find("main")
                or soup.find("article")
                or soup.find("body")
            )

    def scrape_single_url(url_info):
        url = url_info["url"]
        service = url_info["service"]
        service_type = url_info["type"]

        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "lxml")

            # Remove sidebar/navigation clutter
            for tag in soup.find_all(["nav", "aside", "footer", "header"]):
                tag.decompose()

            title_tag = soup.find("h1") or soup.find("title")
            raw_title = title_tag.get_text().strip() if title_tag else ""
            title = clean_title(raw_title, service_type)

            main_content = find_main_content(soup, service_type)

            sections = []
            current_section = {"heading": "", "content": ""}

            if main_content:
                for element in main_content.find_all(
                    ["h1", "h2", "h3", "h4", "p", "ul", "ol", "pre", "table", "blockquote"]
                ):
                    if element.name in ["h1", "h2", "h3", "h4"]:
                        if current_section["content"].strip():
                            sections.append(current_section)
                        current_section = {
                            "heading": element.get_text().strip(),
                            "content": "",
                        }
                    else:
                        text = clean_text(element)
                        if text:
                            current_section["content"] += text + "\n\n"

                if current_section["content"].strip():
                    sections.append(current_section)

            # Filter out near-empty sections
            sections = [s for s in sections if len(s["content"].strip()) > 30]

            return {
                "url": url,
                "service": service,
                "type": service_type,
                "title": title,
                "sections": sections,
                "scraped_at": datetime.now().isoformat(),
            }

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    scraped_pages = []

    for i, url_info in enumerate(delta_urls):
        if (i + 1) % 50 == 0:
            print(f"Progress: {i + 1}/{len(delta_urls)}")

        result = scrape_single_url(url_info)
        if result and result["sections"]:
            scraped_pages.append(result)

        # Rate limiting
        if i < len(delta_urls) - 1:
            time.sleep(0.5)

    dbt_pages = sum(1 for p in scraped_pages if p["type"] == "dbt")
    iics_pages = sum(1 for p in scraped_pages if p["type"] == "iics")
    print(f"Successfully scraped {len(scraped_pages)} pages ({dbt_pages} dbt, {iics_pages} IICS)")

    context["task_instance"].xcom_push(key="scraped_pages", value=scraped_pages)
    return len(scraped_pages)


# ---------------------------------------------------------------------------
# Task 4 – update Pinecone
# ---------------------------------------------------------------------------

def update_pinecone(**context):
    """
    Embed scraped sections with OpenAI text-embedding-ada-002 and upsert
    to a Pinecone index. Metadata includes service type so the RAG agent
    can filter by source (dbt vs IICS).
    """
    scraped_pages = context["task_instance"].xcom_pull(
        task_ids="scrape_pages", key="scraped_pages"
    )

    if not scraped_pages:
        print("No pages to update in Pinecone")
        return 0

    pc = get_pinecone_client()
    dbt_index_name = os.environ.get("PINECONE_DBT_INDEX_NAME", "dbt-docs")
    iics_index_name = os.environ.get("PINECONE_IICS_INDEX_NAME", "iics-docs")

    # Create indexes if they do not exist
    existing_indexes = [idx.name for idx in pc.list_indexes()]
    for idx_name in (dbt_index_name, iics_index_name):
        if idx_name not in existing_indexes:
            print(f"Creating Pinecone index: {idx_name}")
            pc.create_index(
                name=idx_name,
                dimension=1536,  # text-embedding-ada-002
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
    if dbt_index_name not in existing_indexes or iics_index_name not in existing_indexes:
        time.sleep(10)  # Wait for new indexes to be ready

    dbt_index = pc.Index(dbt_index_name)
    iics_index = pc.Index(iics_index_name)
    openai_client = get_openai_client()

    chunks_to_upsert = []

    for page in scraped_pages:
        url = page["url"]
        service = page["service"]
        service_type = page["type"]  # "dbt" or "iics"
        title = page["title"]

        for i, section in enumerate(page["sections"]):
            heading = section["heading"]
            content = section["content"]

            if not content.strip() or len(content) < 50:
                continue

            # Build chunk text with migration-aware context prefix
            # This framing helps the RAG agent retrieve relevant cross-tool answers
            # e.g. "how do I replace an IICS Joiner transformation in dbt?"
            if service_type == "dbt":
                tool_label = "dbt"
                context_label = "dbt (migration target)"
            else:
                tool_label = "Informatica IICS"
                context_label = "Informatica IICS (migration source)"

            chunk_text = f"Migration context: IICS → dbt\n"
            chunk_text += f"Tool: {context_label}\n"
            chunk_text += f"Service: {service}\n"
            chunk_text += f"Page: {title}\n"
            if heading:
                chunk_text += f"Section: {heading}\n\n"
            chunk_text += content

            chunk_id = hashlib.md5(f"{url}_{i}".encode()).hexdigest()

            try:
                embedding_response = openai_client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=chunk_text[:8000],
                )
                embedding = embedding_response.data[0].embedding

                chunks_to_upsert.append({
                    "id": chunk_id,
                    "values": embedding,
                    "metadata": {
                        "url": url,
                        "service": service,
                        "type": service_type,           # "dbt" | "iics" — use for metadata filtering
                        "role": context_label,          # human-readable migration role
                        "title": title,
                        "section": heading,
                        "text": chunk_text[:1000],      # preview stored for retrieval display
                        "scraped_at": page["scraped_at"],
                    },
                })

            except Exception as e:
                print(f"Embedding error for {url} section {i}: {e}")
                continue

            # Respect OpenAI rate limits
            time.sleep(0.1)

    # Split chunks by destination index
    dbt_chunks_list = [c for c in chunks_to_upsert if c["metadata"]["type"] == "dbt"]
    iics_chunks_list = [c for c in chunks_to_upsert if c["metadata"]["type"] == "iics"]

    # Batch upsert helper
    def _batch_upsert(index, chunks, label):
        batch_size = 100
        total_batches = (len(chunks) + batch_size - 1) // batch_size
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            index.upsert(vectors=batch)
            print(f"[{label}] Upserted batch {i // batch_size + 1}/{total_batches}")
            time.sleep(1)

    _batch_upsert(dbt_index, dbt_chunks_list, dbt_index_name)
    _batch_upsert(iics_index, iics_chunks_list, iics_index_name)

    dbt_chunks = len(dbt_chunks_list)
    iics_chunks = len(iics_chunks_list)
    print(f"Upserted {len(chunks_to_upsert)} vectors total → {dbt_chunks} to '{dbt_index_name}', {iics_chunks} to '{iics_index_name}'")

    return len(chunks_to_upsert)


# ---------------------------------------------------------------------------
# Task 5 – update Airflow Variable (scraped URL registry)
# ---------------------------------------------------------------------------

def update_scraped_urls_variable(**context):
    """
    Persist the set of successfully scraped URLs into an Airflow Variable
    so the next run only processes genuinely new pages (delta mode).
    """
    scraped_pages = context["task_instance"].xcom_pull(
        task_ids="scrape_pages", key="scraped_pages"
    )
    scraped_metadata = context["task_instance"].xcom_pull(
        task_ids="compute_delta_urls", key="scraped_metadata"
    )

    if not scraped_pages:
        print("No new pages scraped — variable unchanged")
        return len(scraped_metadata) if scraped_metadata else 0

    for page in scraped_pages:
        scraped_metadata[page["url"]] = {
            "scraped_at": page["scraped_at"],
            "title": page["title"],
            "service": page["service"],
            "type": page["type"],
        }

    Variable.set("dbt_iics_scraped_urls", scraped_metadata, serialize_json=True)

    dbt_count = sum(1 for v in scraped_metadata.values() if v.get("type") == "dbt")
    iics_count = sum(1 for v in scraped_metadata.values() if v.get("type") == "iics")
    print(f"Registry updated: {len(scraped_metadata)} total URLs ({dbt_count} dbt, {iics_count} IICS)")

    return len(scraped_metadata)


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_fetch_sitemaps = PythonOperator(
    task_id="fetch_sitemaps",
    python_callable=fetch_sitemaps,
    dag=dag,
)

task_compute_delta = PythonOperator(
    task_id="compute_delta_urls",
    python_callable=compute_delta_urls,
    dag=dag,
)

task_scrape = PythonOperator(
    task_id="scrape_pages",
    python_callable=scrape_pages,
    dag=dag,
)

task_update_pinecone = PythonOperator(
    task_id="update_pinecone",
    python_callable=update_pinecone,
    dag=dag,
)

task_update_variable = PythonOperator(
    task_id="update_scraped_urls_variable",
    python_callable=update_scraped_urls_variable,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Pipeline
task_fetch_sitemaps >> task_compute_delta >> task_scrape >> task_update_pinecone >> task_update_variable
# ---------------------------------------------------------------------------
