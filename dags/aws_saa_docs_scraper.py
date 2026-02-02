"""
Airflow DAG: AWS SAA Documentation Scraper with Delta Mode
Runs weekly to update Pinecone vector database for AWS Solutions Architect Associate certification prep

Based on the SAA-C03 exam guide, this scraper focuses on in-scope AWS services:
- Domain 1: Design Secure Architectures (30%)
- Domain 2: Design Resilient Architectures (26%)
- Domain 3: Design High-Performing Architectures (24%)
- Domain 4: Design Cost-Optimized Architectures (20%)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import requests
import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
import hashlib
import re

# Default args for the DAG
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'aws_saa_docs_scraper',
    default_args=default_args,
    description='Scrape AWS documentation for SAA-C03 certification and update Pinecone (Delta mode)',
    schedule='0 */2 * * *',  # Evey 2 hours to initialize (then change to weekly)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['aws', 'saa', 'scraping', 'pinecone', 'certification']
)

# SAA-C03 In-Scope AWS Services and their documentation paths
# Based on the official SAA-C03 Exam Guide Appendix
SAA_SERVICE_SITEMAPS = {
    # Analytics
    'athena': 'https://docs.aws.amazon.com/athena/latest/ug/sitemap.xml',
    'emr': 'https://docs.aws.amazon.com/emr/latest/ManagementGuide/sitemap.xml',
    'glue': 'https://docs.aws.amazon.com/glue/latest/dg/sitemap.xml',
    'kinesis': 'https://docs.aws.amazon.com/streams/latest/dev/sitemap.xml',
    'kinesis-firehose': 'https://docs.aws.amazon.com/firehose/latest/dev/sitemap.xml',
    'lake-formation': 'https://docs.aws.amazon.com/lake-formation/latest/dg/sitemap.xml',
    'msk': 'https://docs.aws.amazon.com/msk/latest/developerguide/sitemap.xml',
    'opensearch': 'https://docs.aws.amazon.com/opensearch-service/latest/developerguide/sitemap.xml',
    'quicksight': 'https://docs.aws.amazon.com/quicksight/latest/user/sitemap.xml',
    'redshift': 'https://docs.aws.amazon.com/redshift/latest/mgmt/sitemap.xml',
    
    # Application Integration
    'eventbridge': 'https://docs.aws.amazon.com/eventbridge/latest/userguide/sitemap.xml',
    'sns': 'https://docs.aws.amazon.com/sns/latest/dg/sitemap.xml',
    'sqs': 'https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sitemap.xml',
    'step-functions': 'https://docs.aws.amazon.com/step-functions/latest/dg/sitemap.xml',
    'amazon-mq': 'https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/sitemap.xml',
    'appflow': 'https://docs.aws.amazon.com/appflow/latest/userguide/sitemap.xml',
    'appsync': 'https://docs.aws.amazon.com/appsync/latest/devguide/sitemap.xml',
    
    # Compute
    'ec2': 'https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/sitemap.xml',
    'ec2-autoscaling': 'https://docs.aws.amazon.com/autoscaling/ec2/userguide/sitemap.xml',
    'elastic-beanstalk': 'https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/sitemap.xml',
    'batch': 'https://docs.aws.amazon.com/batch/latest/userguide/sitemap.xml',
    'lambda': 'https://docs.aws.amazon.com/lambda/latest/dg/sitemap.xml',
    'outposts': 'https://docs.aws.amazon.com/outposts/latest/userguide/sitemap.xml',
    
    # Containers
    'ecs': 'https://docs.aws.amazon.com/AmazonECS/latest/developerguide/sitemap.xml',
    'eks': 'https://docs.aws.amazon.com/eks/latest/userguide/sitemap.xml',
    'ecr': 'https://docs.aws.amazon.com/AmazonECR/latest/userguide/sitemap.xml',
    'fargate': 'https://docs.aws.amazon.com/AmazonECS/latest/developerguide/sitemap.xml',  # Fargate docs are in ECS
    
    # Database
    'aurora': 'https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/sitemap.xml',
    'rds': 'https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/sitemap.xml',
    'dynamodb': 'https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/sitemap.xml',
    'elasticache': 'https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/sitemap.xml',
    'neptune': 'https://docs.aws.amazon.com/neptune/latest/userguide/sitemap.xml',
    'documentdb': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/sitemap.xml',
    'keyspaces': 'https://docs.aws.amazon.com/keyspaces/latest/devguide/sitemap.xml',
    'qldb': 'https://docs.aws.amazon.com/qldb/latest/developerguide/sitemap.xml',
    
    # Management and Governance
    'cloudformation': 'https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/sitemap.xml',
    'cloudtrail': 'https://docs.aws.amazon.com/awscloudtrail/latest/userguide/sitemap.xml',
    'cloudwatch': 'https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/sitemap.xml',
    'cloudwatch-logs': 'https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/sitemap.xml',
    'config': 'https://docs.aws.amazon.com/config/latest/developerguide/sitemap.xml',
    'control-tower': 'https://docs.aws.amazon.com/controltower/latest/userguide/sitemap.xml',
    'organizations': 'https://docs.aws.amazon.com/organizations/latest/userguide/sitemap.xml',
    'systems-manager': 'https://docs.aws.amazon.com/systems-manager/latest/userguide/sitemap.xml',
    'trusted-advisor': 'https://docs.aws.amazon.com/awssupport/latest/user/sitemap.xml',
    'compute-optimizer': 'https://docs.aws.amazon.com/compute-optimizer/latest/ug/sitemap.xml',
    'service-catalog': 'https://docs.aws.amazon.com/servicecatalog/latest/adminguide/sitemap.xml',
    'auto-scaling': 'https://docs.aws.amazon.com/autoscaling/plans/userguide/sitemap.xml',
    'license-manager': 'https://docs.aws.amazon.com/license-manager/latest/userguide/sitemap.xml',
    'health-dashboard': 'https://docs.aws.amazon.com/health/latest/ug/sitemap.xml',
    'well-architected': 'https://docs.aws.amazon.com/wellarchitected/latest/userguide/sitemap.xml',
    'proton': 'https://docs.aws.amazon.com/proton/latest/userguide/sitemap.xml',
    
    # Networking and Content Delivery
    'vpc': 'https://docs.aws.amazon.com/vpc/latest/userguide/sitemap.xml',
    'cloudfront': 'https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/sitemap.xml',
    'route53': 'https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/sitemap.xml',
    'api-gateway': 'https://docs.aws.amazon.com/apigateway/latest/developerguide/sitemap.xml',
    'direct-connect': 'https://docs.aws.amazon.com/directconnect/latest/UserGuide/sitemap.xml',
    'global-accelerator': 'https://docs.aws.amazon.com/global-accelerator/latest/dg/sitemap.xml',
    'elb': 'https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/sitemap.xml',
    'transit-gateway': 'https://docs.aws.amazon.com/vpc/latest/tgw/sitemap.xml',
    'vpn': 'https://docs.aws.amazon.com/vpn/latest/s2svpn/sitemap.xml',
    'privatelink': 'https://docs.aws.amazon.com/vpc/latest/privatelink/sitemap.xml',
    
    # Security, Identity, and Compliance
    'iam': 'https://docs.aws.amazon.com/IAM/latest/UserGuide/sitemap.xml',
    'cognito': 'https://docs.aws.amazon.com/cognito/latest/developerguide/sitemap.xml',
    'guardduty': 'https://docs.aws.amazon.com/guardduty/latest/ug/sitemap.xml',
    'inspector': 'https://docs.aws.amazon.com/inspector/latest/user/sitemap.xml',
    'macie': 'https://docs.aws.amazon.com/macie/latest/user/sitemap.xml',
    'kms': 'https://docs.aws.amazon.com/kms/latest/developerguide/sitemap.xml',
    'secrets-manager': 'https://docs.aws.amazon.com/secretsmanager/latest/userguide/sitemap.xml',
    'acm': 'https://docs.aws.amazon.com/acm/latest/userguide/sitemap.xml',
    'waf': 'https://docs.aws.amazon.com/waf/latest/developerguide/sitemap.xml',
    'shield': 'https://docs.aws.amazon.com/waf/latest/developerguide/sitemap.xml',  # Shield docs are with WAF
    'security-hub': 'https://docs.aws.amazon.com/securityhub/latest/userguide/sitemap.xml',
    'network-firewall': 'https://docs.aws.amazon.com/network-firewall/latest/developerguide/sitemap.xml',
    'firewall-manager': 'https://docs.aws.amazon.com/fms/latest/userguide/sitemap.xml',
    'directory-service': 'https://docs.aws.amazon.com/directoryservice/latest/admin-guide/sitemap.xml',
    'cloudhsm': 'https://docs.aws.amazon.com/cloudhsm/latest/userguide/sitemap.xml',
    'ram': 'https://docs.aws.amazon.com/ram/latest/userguide/sitemap.xml',
    'iam-identity-center': 'https://docs.aws.amazon.com/singlesignon/latest/userguide/sitemap.xml',
    'artifact': 'https://docs.aws.amazon.com/artifact/latest/ug/sitemap.xml',
    'audit-manager': 'https://docs.aws.amazon.com/audit-manager/latest/userguide/sitemap.xml',
    'detective': 'https://docs.aws.amazon.com/detective/latest/userguide/sitemap.xml',
    
    # Storage
    's3': 'https://docs.aws.amazon.com/AmazonS3/latest/userguide/sitemap.xml',
    's3-glacier': 'https://docs.aws.amazon.com/amazonglacier/latest/dev/sitemap.xml',
    'ebs': 'https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/sitemap.xml',  # EBS is in EC2 docs
    'efs': 'https://docs.aws.amazon.com/efs/latest/ug/sitemap.xml',
    'fsx': 'https://docs.aws.amazon.com/fsx/latest/WindowsGuide/sitemap.xml',
    'storage-gateway': 'https://docs.aws.amazon.com/storagegateway/latest/userguide/sitemap.xml',
    'backup': 'https://docs.aws.amazon.com/aws-backup/latest/devguide/sitemap.xml',
    
    # Migration and Transfer
    'dms': 'https://docs.aws.amazon.com/dms/latest/userguide/sitemap.xml',
    'datasync': 'https://docs.aws.amazon.com/datasync/latest/userguide/sitemap.xml',
    'migration-hub': 'https://docs.aws.amazon.com/migrationhub/latest/ug/sitemap.xml',
    'transfer-family': 'https://docs.aws.amazon.com/transfer/latest/userguide/sitemap.xml',
    'snow-family': 'https://docs.aws.amazon.com/snowball/latest/developer-guide/sitemap.xml',
    'application-migration': 'https://docs.aws.amazon.com/mgn/latest/ug/sitemap.xml',
    'application-discovery': 'https://docs.aws.amazon.com/application-discovery/latest/userguide/sitemap.xml',
    
    # Machine Learning (in-scope for SAA)
    'sagemaker': 'https://docs.aws.amazon.com/sagemaker/latest/dg/sitemap.xml',
    'rekognition': 'https://docs.aws.amazon.com/rekognition/latest/dg/sitemap.xml',
    'comprehend': 'https://docs.aws.amazon.com/comprehend/latest/dg/sitemap.xml',
    'polly': 'https://docs.aws.amazon.com/polly/latest/dg/sitemap.xml',
    'transcribe': 'https://docs.aws.amazon.com/transcribe/latest/dg/sitemap.xml',
    'translate': 'https://docs.aws.amazon.com/translate/latest/dg/sitemap.xml',
    'lex': 'https://docs.aws.amazon.com/lexv2/latest/dg/sitemap.xml',
    'kendra': 'https://docs.aws.amazon.com/kendra/latest/dg/sitemap.xml',
    'forecast': 'https://docs.aws.amazon.com/forecast/latest/dg/sitemap.xml',
    'fraud-detector': 'https://docs.aws.amazon.com/frauddetector/latest/ug/sitemap.xml',
    'textract': 'https://docs.aws.amazon.com/textract/latest/dg/sitemap.xml',
    
    # Front-End Web and Mobile
    'amplify': 'https://docs.aws.amazon.com/amplify/latest/userguide/sitemap.xml',
    'pinpoint': 'https://docs.aws.amazon.com/pinpoint/latest/userguide/sitemap.xml',
    'device-farm': 'https://docs.aws.amazon.com/devicefarm/latest/developerguide/sitemap.xml',
    
    # Cost Management
    'cost-explorer': 'https://docs.aws.amazon.com/cost-management/latest/userguide/sitemap.xml',
    'budgets': 'https://docs.aws.amazon.com/cost-management/latest/userguide/sitemap.xml',
    
    # Developer Tools (X-Ray is in scope)
    'xray': 'https://docs.aws.amazon.com/xray/latest/devguide/sitemap.xml',
    
    # Media Services
    'elastic-transcoder': 'https://docs.aws.amazon.com/elastictranscoder/latest/developerguide/sitemap.xml',
    'kinesis-video': 'https://docs.aws.amazon.com/kinesisvideostreams/latest/dg/sitemap.xml',
}

# SAA-relevant topics to filter URLs (keeps them focused on exam-relevant content)
SAA_RELEVANT_TOPICS = [
    # Architecture & Design
    'architecture', 'design', 'best-practice', 'well-architected',
    'high-availability', 'fault-tolerant', 'disaster-recovery', 'resilient',
    'scalable', 'scalability', 'elastic', 'auto-scaling',
    
    # Security
    'security', 'encryption', 'iam', 'policy', 'role', 'permission',
    'access-control', 'authentication', 'authorization', 'identity',
    'kms', 'key-management', 'ssl', 'tls', 'certificate',
    'vpc', 'subnet', 'security-group', 'network-acl', 'firewall',
    'mfa', 'multi-factor', 'compliance', 'audit',
    
    # Networking
    'networking', 'vpc', 'subnet', 'route', 'gateway', 'endpoint',
    'load-balancer', 'elb', 'alb', 'nlb', 'cloudfront', 'cdn',
    'direct-connect', 'vpn', 'transit-gateway', 'peering',
    'dns', 'route53', 'hosted-zone',
    
    # Storage
    'storage', 's3', 'bucket', 'object', 'ebs', 'volume', 'snapshot',
    'efs', 'fsx', 'glacier', 'lifecycle', 'replication', 'versioning',
    'storage-class', 'tiering', 'backup', 'archive',
    
    # Database
    'database', 'rds', 'aurora', 'dynamodb', 'elasticache',
    'read-replica', 'multi-az', 'failover', 'backup', 'restore',
    'performance', 'scaling', 'capacity', 'throughput',
    
    # Compute
    'compute', 'ec2', 'instance', 'ami', 'launch', 'auto-scaling',
    'lambda', 'serverless', 'container', 'ecs', 'eks', 'fargate',
    'spot', 'reserved', 'on-demand', 'savings-plan',
    
    # Application Integration
    'integration', 'sqs', 'sns', 'eventbridge', 'step-functions',
    'api-gateway', 'rest', 'websocket', 'microservices', 'decoupling',
    'message', 'queue', 'topic', 'event',
    
    # Monitoring & Management
    'monitoring', 'cloudwatch', 'metrics', 'alarm', 'log', 'trace',
    'cloudtrail', 'config', 'systems-manager', 'parameter-store',
    'secrets-manager', 'cloudformation', 'infrastructure-as-code',
    
    # Cost Optimization
    'cost', 'pricing', 'billing', 'budget', 'optimization',
    'reserved', 'spot', 'savings', 'cost-explorer',
    
    # Migration & Transfer
    'migration', 'transfer', 'datasync', 'dms', 'snowball',
    'hybrid', 'on-premises', 'lift-and-shift',
    
    # Data & Analytics
    'analytics', 'data-lake', 'etl', 'glue', 'athena', 'redshift',
    'kinesis', 'streaming', 'batch', 'emr', 'spark',
    
    # General concepts
    'getting-started', 'concepts', 'overview', 'how-it-works',
    'tutorial', 'use-case', 'scenario', 'example',
    'quotas', 'limits', 'pricing', 'regions', 'availability-zones',
]


def get_pinecone_client():
    """Initialize Pinecone client"""
    api_key = os.environ.get("PINECONE_API_KEY")
    return Pinecone(api_key=api_key)


def get_openai_client():
    """Initialize OpenAI client"""
    api_key = os.environ.get("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


def fetch_service_sitemaps(**context):
    """
    Task 1: Fetch URLs from all SAA-relevant service sitemaps
    """
    print(f"Fetching sitemaps for {len(SAA_SERVICE_SITEMAPS)} AWS services...")
    
    all_urls = []
    failed_sitemaps = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    for service_name, sitemap_url in SAA_SERVICE_SITEMAPS.items():
        try:
            print(f"Fetching sitemap for {service_name}...")
            response = session.get(sitemap_url, timeout=30)
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                service_urls = []
                for url in root.findall('.//ns:url/ns:loc', namespace):
                    clean_url = url.text.strip().replace('\n', '')
                    service_urls.append({
                        'url': clean_url,
                        'service': service_name
                    })
                
                all_urls.extend(service_urls)
                print(f"  Found {len(service_urls)} URLs for {service_name}")
            else:
                print(f"  Warning: Could not fetch sitemap for {service_name} (status: {response.status_code})")
                failed_sitemaps.append(service_name)
                
            # Rate limiting between sitemap requests
            time.sleep(0.3)
            
        except Exception as e:
            print(f"  Error fetching sitemap for {service_name}: {e}")
            failed_sitemaps.append(service_name)
    
    print(f"\nTotal URLs collected: {len(all_urls)}")
    print(f"Failed sitemaps: {len(failed_sitemaps)}")
    
    # Filter for SAA-relevant topics
    filtered_urls = []
    for url_info in all_urls:
        url_lower = url_info['url'].lower()
        
        # Skip release notes, API references, and CLI references
        if any(skip in url_lower for skip in ['/release-notes/', '/api-reference/', '/cli/', '/APIReference/']):
            continue
            
        # Check if URL contains relevant topics
        if any(topic in url_lower for topic in SAA_RELEVANT_TOPICS):
            filtered_urls.append(url_info)
    
    print(f"Filtered to {len(filtered_urls)} SAA-relevant URLs")
    
    context['task_instance'].xcom_push(key='all_urls', value=filtered_urls)
    context['task_instance'].xcom_push(key='failed_sitemaps', value=failed_sitemaps)
    
    return len(filtered_urls)


def compute_delta_urls(**context):
    """
    Task 2: Compare with previously scraped URLs and compute delta
    """
    all_urls = context['task_instance'].xcom_pull(
        task_ids='fetch_service_sitemaps',
        key='all_urls'
    )
    
    # Get previously scraped URLs from Airflow Variable
    try:
        scraped_metadata = Variable.get("aws_saa_scraped_urls", deserialize_json=True)
        if not scraped_metadata:
            scraped_metadata = {}
    except:
        scraped_metadata = {}
    
    # Compute delta: new URLs
    new_urls = []
    for url_info in all_urls:
        if url_info['url'] not in scraped_metadata:
            new_urls.append(url_info)
    
    print(f"Delta: {len(new_urls)} new URLs to scrape")
    print(f"Previously scraped: {len(scraped_metadata)} URLs")
    
    # Limit batch size to avoid overwhelming the system
    MAX_URLS_PER_RUN = 1000
    if len(new_urls) > MAX_URLS_PER_RUN:
        print(f"Limiting to {MAX_URLS_PER_RUN} URLs per run")
        new_urls = new_urls[:MAX_URLS_PER_RUN]
    
    context['task_instance'].xcom_push(key='delta_urls', value=new_urls)
    context['task_instance'].xcom_push(key='scraped_metadata', value=scraped_metadata)
    
    return len(new_urls)


def scrape_pages(**context):
    """
    Task 3: Scrape delta URLs sequentially with rate limiting
    """
    delta_urls = context['task_instance'].xcom_pull(
        task_ids='compute_delta_urls',
        key='delta_urls'
    )

    if not delta_urls:
        print("No new URLs to scrape")
        context['task_instance'].xcom_push(key='scraped_pages', value=[])
        return 0

    print(f"Scraping {len(delta_urls)} pages...")

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    def clean_text(element):
        """Clean HTML content and extract text"""
        for script in element(["script", "style", "nav", "footer", "header", "aside"]):
            script.decompose()
        text = element.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return ' '.join(chunk for chunk in chunks if chunk)

    def scrape_single_url(url_info):
        """Scrape a single URL and extract structured content"""
        url = url_info['url']
        service = url_info['service']
        
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            # Extract title
            title_tag = soup.find('h1') or soup.find('title')
            title = title_tag.get_text().strip() if title_tag else ''
            
            # Clean up title (remove " - Amazon Web Services" suffix)
            title = re.sub(r'\s*[-–]\s*Amazon Web Services.*$', '', title)
            title = re.sub(r'\s*[-–]\s*AWS Documentation.*$', '', title)

            # Extract main content
            main_content = (
                soup.find('main') or 
                soup.find('article') or 
                soup.find('div', {'id': 'main-content'}) or
                soup.find('div', {'class': 'awsdocs-content'}) or
                soup.find('body')
            )

            sections = []
            if main_content:
                current_section = {'heading': '', 'content': ''}

                for element in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'ul', 'ol', 'pre', 'table']):
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
                'service': service,
                'title': title,
                'sections': sections,
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    # Sequential scraping with rate limiting
    scraped_pages = []
    
    for i, url_info in enumerate(delta_urls):
        if (i + 1) % 50 == 0:
            print(f"Progress: {i + 1}/{len(delta_urls)}")
            
        result = scrape_single_url(url_info)
        
        if result and result['sections']:
            scraped_pages.append(result)

        # Rate limiting: 0.5 second between requests
        if i < len(delta_urls) - 1:
            time.sleep(0.5)

    print(f"Successfully scraped {len(scraped_pages)} pages")
    
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
    index_name = os.environ.get("PINECONE_AWS_INDEX_NAME", "aws-saa-docs")
    
    # Check if index exists, create if not
    existing_indexes = [idx.name for idx in pc.list_indexes()]
    if index_name not in existing_indexes:
        print(f"Creating index {index_name}...")
        pc.create_index(
            name=index_name,
            dimension=1536,  # OpenAI text-embedding-ada-002 dimension
            metric='cosine',
            spec=ServerlessSpec(cloud='aws', region='us-east-1')
        )
        # Wait for index to be ready
        time.sleep(10)
    
    index = pc.Index(index_name)
    
    # Initialize OpenAI for embeddings
    openai_client = get_openai_client()
    
    # Process documents into chunks
    chunks_to_upsert = []
    
    for page in scraped_pages:
        url = page['url']
        service = page['service']
        title = page['title']
        
        for i, section in enumerate(page['sections']):
            heading = section['heading']
            content = section['content']
            
            if not content.strip() or len(content) < 50:
                continue
            
            # Create chunk text with context
            chunk_text = f"AWS Service: {service.upper()}\n"
            chunk_text += f"Title: {title}\n"
            if heading:
                chunk_text += f"Section: {heading}\n\n"
            chunk_text += content
            
            # Generate unique ID
            chunk_id = hashlib.md5(f"{url}_{i}".encode()).hexdigest()
            
            # Create embedding
            try:
                embedding_response = openai_client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=chunk_text[:8000]  # OpenAI limit
                )
                embedding = embedding_response.data[0].embedding
                
                chunks_to_upsert.append({
                    'id': chunk_id,
                    'values': embedding,
                    'metadata': {
                        'url': url,
                        'service': service,
                        'title': title,
                        'section': heading,
                        'text': chunk_text[:1000],  # Store preview in metadata
                        'scraped_at': page['scraped_at'],
                        'exam': 'SAA-C03'
                    }
                })
                
            except Exception as e:
                print(f"Error creating embedding for {url}: {e}")
                continue
            
            # Rate limiting for OpenAI API
            time.sleep(0.1)
    
    # Batch upsert to Pinecone
    batch_size = 100
    for i in range(0, len(chunks_to_upsert), batch_size):
        batch = chunks_to_upsert[i:i + batch_size]
        index.upsert(vectors=batch)
        print(f"Upserted batch {i // batch_size + 1}/{(len(chunks_to_upsert) // batch_size) + 1}")
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

    if not scraped_pages:
        print("No new pages scraped, variable unchanged")
        return len(scraped_metadata) if scraped_metadata else 0

    # Update metadata with new URLs
    for page in scraped_pages:
        scraped_metadata[page['url']] = {
            'scraped_at': page['scraped_at'],
            'title': page['title'],
            'service': page['service']
        }

    # Save back to Airflow Variable
    Variable.set(
        "aws_saa_scraped_urls",
        scraped_metadata,
        serialize_json=True
    )

    print(f"Updated scraped URLs variable. Total: {len(scraped_metadata)} URLs")

    return len(scraped_metadata)


# Define tasks
task_fetch_sitemaps = PythonOperator(
    task_id='fetch_service_sitemaps',
    python_callable=fetch_service_sitemaps,
    dag=dag
)

task_compute_delta = PythonOperator(
    task_id='compute_delta_urls',
    python_callable=compute_delta_urls,
    dag=dag
)

task_scrape = PythonOperator(
    task_id='scrape_pages',
    python_callable=scrape_pages,
    dag=dag
)

task_update_pinecone = PythonOperator(
    task_id='update_pinecone',
    python_callable=update_pinecone,
    dag=dag
)

task_update_variable = PythonOperator(
    task_id='update_scraped_urls_variable',
    python_callable=update_scraped_urls_variable,
    dag=dag
)

# Define task dependencies
task_fetch_sitemaps >> task_compute_delta >> task_scrape >> task_update_pinecone >> task_update_variable