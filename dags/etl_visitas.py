import os
import sys
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import yaml

# Add modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from modules.transformation import DataTransformer
from modules.loading import MySQLLoader
from modules.metrics import ETLMetrics

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'configs', 'config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@task
def extract_task(metrics: ETLMetrics):
    """Extract data from local files (for development)"""
    import os
    from pathlib import Path

    start_time = time.time()
    local_dir = Path(__file__).parent.parent / 'data' / 'raw'
    files = [str(f) for f in local_dir.glob('*.txt') if f.is_file()]

    metrics.record_files_received(len(files))
    metrics.record_stage_time('extraction', start_time)

    return files

@task
def transform_task(files, metrics: ETLMetrics):
    """Transform extracted files"""
    start_time = time.time()
    transformer = DataTransformer()
    all_valid = []
    all_errors = []

    total_records = 0
    for file in files:
        result = transformer.transform_file(file)
        all_valid.extend(result['valid'])
        all_errors.extend(result['errors'])
        total_records += len(result['valid']) + len(result['errors'])

    metrics.record_files_processed(len(files))
    metrics.record_records_received(total_records)
    metrics.record_records_valid(len(all_valid))
    metrics.record_records_errors(len(all_errors))
    metrics.record_stage_time('transformation', start_time)

    return {'valid': all_valid, 'errors': all_errors}

@task
def load_task(data, files, metrics: ETLMetrics):
    """Load data to MySQL"""
    start_time = time.time()

    # For containerized environment
    loader = MySQLLoader(
        host='mysql',  # container name
        user='etl_user',
        password='etl_pass',
        database='visitas_db'
    )
    loader.load_data(data['valid'], data['errors'], files)

    metrics.record_stage_time('loading', start_time)
    metrics.end_execution()
    metrics.log_summary()

with DAG(
    'etl_visitas_diario',
    default_args=default_args,
    description='ETL diario para datos de visitas web',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Initialize metrics
    metrics = ETLMetrics()
    metrics.start_execution()

    extracted_files = extract_task(metrics)
    transformed_data = transform_task(extracted_files, metrics)
    load_task(transformed_data, extracted_files, metrics)

    # Note: end_execution is called in load_task after logging summary