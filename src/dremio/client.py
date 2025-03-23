import requests
import yaml
from typing import Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DremioClient:
    def __init__(self, config_path: str):
        """Initialize Dremio client with configuration."""
        self.config = self._load_config(config_path)
        self.base_url = f"http://{self.config['dremio']['host']}:{self.config['dremio']['port']}"
        self.session = requests.Session()
        self._setup_authentication()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_authentication(self):
        """Setup authentication based on config."""
        auth_type = self.config['dremio']['auth_type']
        if auth_type == 'basic':
            self.session.auth = (
                self.config['dremio']['username'],
                self.config['dremio']['password']
            )
        elif auth_type == 'token':
            self.session.headers.update({
                'Authorization': f"Bearer {self.config['dremio']['token']}"
            })

    def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        endpoint = f"{self.base_url}/api/v3/sql"
        payload = {
            "sql": query,
            "context": [],
            "timeout": self.config['query_settings']['timeout']
        }
        
        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_catalog(self) -> Dict[str, Any]:
        """Get Dremio catalog information."""
        endpoint = f"{self.base_url}/api/v3/catalog"
        try:
            response = self.session.get(endpoint)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting catalog: {e}")
            raise 