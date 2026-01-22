"""
Airflow DAG: Snowflake Documentation Scraper with Delta Mode
Runs weekly to update Pinecone vector database
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

# Default args for the DAG
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'snowflake_docs_scraper',
    default_args=default_args,
    description='Scrape Snowflake documentation and update Pinecone (Delta mode)',
    schedule='0 2 * * 0',  # Weekly on Sunday at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['snowflake', 'scraping', 'pinecone']
 )

def get_pinecone_client():
    api_key = os.environ.get("PINECONE_API_KEY")
    return Pinecone(api_key=api_key)

def get_openai_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


def fetch_sitemap_urls(**context):
    """
    Task 1: Fetch all URLs from Snowflake sitemap
    """
    base_url = "https://docs.snowflake.com"
    sitemap_url = f"{base_url}/sitemap.xml"
    
    print(f"Fetching sitemap from {sitemap_url}...")
    
    response = requests.get(sitemap_url, timeout=30)
    response.raise_for_status()
    
    root = ET.fromstring(response.content)
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    all_urls = []
    for url in root.findall('.//ns:url/ns:loc', namespace):
        clean_url = url.text.strip().replace('\n', '')
        all_urls.append(clean_url)
    
    # Filter for SnowPro Core relevant topics
    relevant_topics = [
                'warehouses',
                'tables',
                'data-load',
                'data-unload',
                'security',
                'account',
                'user-management',
                'sql-reference',
                'performance',
                'data-sharing',
                'database',
                'schemas',
                'views',
                'cloning',
                'time-travel',
                'fail-safe',
                'streams',
                'tasks',
                'stored-procedures',
                'functions',
                'stages',
                'file-format',
                'pipes',
                'resource-monitors',
                'roles',
                'privileges',
                'authentication',
                'network-policies',
                'encryption',
                'caching',
                'clustering',
                'materialized-views',
                'transactions',
                'query-syntax',
                'data-types',
            ]
    
    filtered_urls = []
    for url in all_urls:
        url_lower = url.strip().replace('\n','').lower()
        if any(topic in url_lower for topic in relevant_topics):
            if '/en/release-notes/' not in url_lower:
                filtered_urls.append(url)
    
    print(f"Found {len(all_urls)} total URLs, {len(filtered_urls)} relevant URLs")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='all_urls', value=filtered_urls)
    return len(filtered_urls)


def compute_delta_urls(**context):
    """
    Task 2: Compare with previously scraped URLs and compute delta
    """
    # Pull URLs from previous task
    all_urls = context['task_instance'].xcom_pull(
        task_ids='fetch_sitemap_urls',
        key='all_urls'
    )
    
    # Get previously scraped URLs from Airflow Variable
    try:
        scraped_metadata = Variable.get("snowflake_scraped_urls", deserialize_json=True)
        if not scraped_metadata:
            scraped_metadata = {}
    except:
        scraped_metadata = {}
    
    # Compute delta: new URLs and potentially changed URLs
    new_urls = []
    for url in all_urls:
        if url not in scraped_metadata:
            new_urls.append(url)
    
    print(f"Delta: {len(new_urls)} new/changed URLs to scrape")
    print(f"Previously scraped: {len(scraped_metadata)} URLs")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='delta_urls', value=new_urls)
    context['task_instance'].xcom_push(key='scraped_metadata', value=scraped_metadata)
    
    return len(new_urls)


def scrape_pages(**context):
    """
    Task 3: Scrape delta URLs sequentially
    """
    delta_urls = context['task_instance'].xcom_pull(
        task_ids='compute_delta_urls',
        key='delta_urls'
    )

    if not delta_urls:
        print("No new URLs to scrape")
        return []

    print(f"Scraping {len(delta_urls)} pages...")

    # Create a single session for all requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    def clean_text(element):
        for script in element(["script", "style", "nav", "footer"]):
            script.decompose()
        text = element.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return ' '.join(chunk for chunk in chunks if chunk)

    def scrape_single_url(url):
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            # Extract title
            title_tag = soup.find('h1') or soup.find('title')
            title = title_tag.get_text().strip() if title_tag else ''

            # Extract content
            main_content = soup.find('main') or soup.find('article') or soup.find('body')

            sections = []
            if main_content:
                current_section = {'heading': '', 'content': ''}

                for element in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'ul', 'ol', 'pre']):
                    if element.name in ['h1', 'h2', 'h3', 'h4']:
                        if current_section['content'].strip():
                            sections.append(current_section)
                        current_section = {
                            'heading': element.get_text().strip(),
                            'content': ''
                        }
                    else:
                        text = clean_text(element)
                        if text:
                            current_section['content'] += text + '\n\n'

                if current_section['content'].strip():
                    sections.append(current_section)

            return {
                'url': url,
                'title': title,
                'sections': sections,
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    # Sequential scraping with rate limiting
    scraped_pages = []

    for i, url in enumerate(delta_urls):
        print(f"Scraping {i+1}/{len(delta_urls)}: {url}")
        result = scrape_single_url(url)

        if result and result['sections']:
            scraped_pages.append(result)

        # Rate limiting: sleep between requests
        if i < len(delta_urls) - 1:
            time.sleep(0.5)

    print(f"Successfully scraped {len(scraped_pages)} pages")

    # Push to XCom
    context['task_instance'].xcom_push(key='scraped_pages', value=scraped_pages)
    return len(scraped_pages)


def update_pinecone(**context):
    """
    Task 4: Update Pinecone vector database with new documents
    """
    scraped_pages = context['task_instance'].xcom_pull(
        task_ids='scrape_pages',
        key='scraped_pages'
    )
    
    if not scraped_pages:
        print("No pages to update in Pinecone")
        return 0
    
    # Initialize Pinecone
    pc = get_pinecone_client()
    index_name = os.environ.get("PINECONE_INDEX_NAME", "snowflake-docs")
    
    # Check if index exists, create if not
    if index_name not in pc.list_indexes().names():
        print(f"Creating index {index_name}...")
        pc.create_index(
            name=index_name,
            dimension=1536,  # OpenAI ada-002 embedding dimension
            metric='cosine',
            spec=ServerlessSpec(cloud='aws', region='us-east-1')
        )
    
    index = pc.Index(index_name)
    
    # Initialize OpenAI for embeddings
    openai_client = get_openai_client()
    
    # Process documents into chunks
    chunks_to_upsert = []
    
    for page in scraped_pages:
        url = page['url']
        title = page['title']
        
        for i, section in enumerate(page['sections']):
            heading = section['heading']
            content = section['content']
            
            if not content.strip():
                continue
            
            # Create chunk text
            chunk_text = f"Title: {title}\n"
            if heading:
                chunk_text += f"Section: {heading}\n\n"
            chunk_text += content
            
            # Generate unique ID
            chunk_id = hashlib.md5(f"{url}_{i}".encode()).hexdigest()
            
            # Create embedding
            try:
                embedding_response = openai_client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=chunk_text[:8000]
                )
                embedding = embedding_response.data[0].embedding
                
                chunks_to_upsert.append({
                    'id': chunk_id,
                    'values': embedding,
                    'metadata': {
                        'url': url,
                        'title': title,
                        'section': heading,
                        'text': chunk_text[:1000],
                        'scraped_at': page['scraped_at']
                    }
                })
                
            except Exception as e:
                print(f"Error creating embedding for {url}: {e}")
                continue
    
    # Batch upsert to Pinecone
    batch_size = 100
    for i in range(0, len(chunks_to_upsert), batch_size):
        batch = chunks_to_upsert[i:i + batch_size]
        index.upsert(vectors=batch)
        print(f"Upserted batch {i//batch_size + 1}/{len(chunks_to_upsert)//batch_size + 1}")
        time.sleep(1) 
    
    print(f"Successfully upserted {len(chunks_to_upsert)} vectors to Pinecone")
    
    return len(chunks_to_upsert)


def update_scraped_urls_variable(**context):
    """
    Task 5: Update Airflow Variable with newly scraped URLs
    """
    scraped_pages = context['task_instance'].xcom_pull(
        task_ids='scrape_pages',
        key='scraped_pages'
    )

    scraped_metadata = context['task_instance'].xcom_pull(
        task_ids='compute_delta_urls',
        key='scraped_metadata'
    )

    # Handle case where no pages were scraped
    if not scraped_pages:
        print("No new pages scraped, variable unchanged")
        print(f"Current total: {len(scraped_metadata) if scraped_metadata else 0} URLs")
        return len(scraped_metadata) if scraped_metadata else 0

    # Update metadata with new URLs
    for page in scraped_pages:
        scraped_metadata[page['url']] = {
            'scraped_at': page['scraped_at'],
            'title': page['title']
        }

    # Save back to Airflow Variable
    Variable.set(
        "snowflake_scraped_urls",
        scraped_metadata,
        serialize_json=True
    )

    print(f"Updated scraped URLs variable. Total: {len(scraped_metadata)} URLs")

    return len(scraped_metadata)


# Task 1: Fetch all URLs from sitemap
task_fetch_urls = PythonOperator(
    task_id='fetch_sitemap_urls',
    python_callable=fetch_sitemap_urls,
    dag=dag
)

# Task 2: Compute delta (new URLs)
task_compute_delta = PythonOperator(
    task_id='compute_delta_urls',
    python_callable=compute_delta_urls,
    dag=dag
)

# Task 3: Scrape delta URLs
task_scrape = PythonOperator(
    task_id='scrape_pages',
    python_callable=scrape_pages,
    dag=dag
)

# Task 4: Update Pinecone
task_update_pinecone = PythonOperator(
    task_id='update_pinecone',
    python_callable=update_pinecone,
    dag=dag
)

# Task 5: Update scraped URLs variable
task_update_variable = PythonOperator(
    task_id='update_scraped_urls_variable',
    python_callable=update_scraped_urls_variable,
    dag=dag
)

# Define task dependencies
task_fetch_urls >> task_compute_delta >> task_scrape >> task_update_pinecone >> task_update_variable