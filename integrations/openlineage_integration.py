import requests
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class OpenLineageClient:
    def __init__(self, url: str, namespace: str):
        self.url = url
        self.namespace = namespace

    def send_event(self, event: Dict[str, Any]):
        """Send lineage event to OpenLineage"""
        try:
            response = requests.post(self.url, json=event)
            response.raise_for_status()
            logger.info("OpenLineage event sent successfully")
        except Exception as e:
            logger.error(f"Failed to send OpenLineage event: {e}")

    def create_job_event(self, job_name: str, inputs: list, outputs: list, status: str):
        """Create a job lineage event"""
        event = {
            "eventType": "COMPLETE" if status == "success" else "FAIL",
            "eventTime": "2023-01-01T00:00:00Z",  # Replace with actual
            "run": {
                "runId": "run-123",  # Generate unique
                "facets": {}
            },
            "job": {
                "namespace": self.namespace,
                "name": job_name,
                "facets": {}
            },
            "inputs": inputs,
            "outputs": outputs
        }
        self.send_event(event)