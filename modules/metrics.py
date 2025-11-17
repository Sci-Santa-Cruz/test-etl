import logging
import time
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class ETLMetrics:
    """Collect and report ETL KPIs"""

    def __init__(self):
        self.metrics = {
            'execution_start': None,
            'execution_end': None,
            'files_received': 0,
            'files_processed': 0,
            'records_received': 0,
            'records_valid': 0,
            'records_errors': 0,
            'stage_times': {},
            'alerts_generated': 0,
            'alerts_resolved': 0
        }

    def start_execution(self):
        """Mark execution start"""
        self.metrics['execution_start'] = time.time()
        logger.info("ETL execution started")

    def end_execution(self):
        """Mark execution end and calculate total time"""
        self.metrics['execution_end'] = time.time()
        total_time = self.metrics['execution_end'] - self.metrics['execution_start']
        self.metrics['total_execution_time'] = total_time
        logger.info(f"ETL execution completed in {total_time:.2f} seconds")

    def record_stage_time(self, stage_name: str, start_time: float):
        """Record execution time for a stage"""
        end_time = time.time()
        duration = end_time - start_time
        self.metrics['stage_times'][stage_name] = duration
        logger.info(f"Stage '{stage_name}' completed in {duration:.2f} seconds")

    def record_files_received(self, count: int):
        """Record number of files received"""
        self.metrics['files_received'] = count
        logger.info(f"Files received: {count}")

    def record_files_processed(self, count: int):
        """Record number of files processed"""
        self.metrics['files_processed'] = count
        logger.info(f"Files processed: {count}")

    def record_records_received(self, count: int):
        """Record total records received"""
        self.metrics['records_received'] = count
        logger.info(f"Records received: {count}")

    def record_records_valid(self, count: int):
        """Record valid records"""
        self.metrics['records_valid'] = count
        logger.info(f"Valid records: {count}")

    def record_records_errors(self, count: int):
        """Record error records"""
        self.metrics['records_errors'] = count
        logger.info(f"Error records: {count}")

    def record_alert_generated(self):
        """Record alert generated"""
        self.metrics['alerts_generated'] += 1
        logger.warning(f"Alert generated (total: {self.metrics['alerts_generated']})")

    def record_alert_resolved(self):
        """Record alert resolved"""
        self.metrics['alerts_resolved'] += 1
        logger.info(f"Alert resolved (total: {self.metrics['alerts_resolved']})")

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        return {
            'files_processed_vs_received': f"{self.metrics['files_processed']}/{self.metrics['files_received']}",
            'records_valid_vs_errors': f"{self.metrics['records_valid']}/{self.metrics['records_errors']}",
            'total_records': self.metrics['records_received'],
            'execution_time_seconds': self.metrics.get('total_execution_time', 0),
            'stage_times': self.metrics['stage_times'],
            'alerts': f"{self.metrics['alerts_generated']} generated, {self.metrics['alerts_resolved']} resolved",
            'timestamp': datetime.now().isoformat()
        }

    def log_summary(self):
        """Log metrics summary"""
        summary = self.get_summary()
        logger.info("=== ETL METRICS SUMMARY ===")
        logger.info(f"Files processed: {summary['files_processed_vs_received']}")
        logger.info(f"Records (valid/errors): {summary['records_valid_vs_errors']}")
        logger.info(f"Total records: {summary['total_records']}")
        logger.info(f"Execution time: {summary['execution_time_seconds']:.2f}s")
        logger.info(f"Stage times: {summary['stage_times']}")
        logger.info(f"Alerts: {summary['alerts']}")
        logger.info("==========================")