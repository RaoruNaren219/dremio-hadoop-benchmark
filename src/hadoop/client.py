import yaml
from typing import Dict, Any
import logging
from hdfs import InsecureClient
from pyhive import hive

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HadoopClient:
    def __init__(self, config_path: str):
        """Initialize Hadoop client with configuration."""
        self.config = self._load_config(config_path)
        self.hdfs_client = self._setup_hdfs_client()
        self.hive_client = self._setup_hive_client()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_hdfs_client(self) -> InsecureClient:
        """Setup HDFS client."""
        try:
            return InsecureClient(
                self.config['hadoop']['namenode'],
                user=self.config['hadoop']['username']
            )
        except Exception as e:
            logger.error(f"Error setting up HDFS client: {e}")
            raise

    def _setup_hive_client(self) -> hive.Connection:
        """Setup Hive client."""
        try:
            return hive.Connection(
                host=self.config['hive']['host'],
                port=self.config['hive']['port'],
                username=self.config['hive']['username'],
                password=self.config['hive']['password'],
                database=self.config['hive']['database']
            )
        except Exception as e:
            logger.error(f"Error setting up Hive client: {e}")
            raise

    def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        try:
            cursor = self.hive_client.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            return {
                "columns": columns,
                "rows": results
            }
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()

    def list_hdfs_path(self, path: str) -> list:
        """List contents of an HDFS path."""
        try:
            return self.hdfs_client.list(path)
        except Exception as e:
            logger.error(f"Error listing HDFS path: {e}")
            raise

    def read_hdfs_file(self, path: str) -> bytes:
        """Read a file from HDFS."""
        try:
            with self.hdfs_client.read(path) as reader:
                return reader.read()
        except Exception as e:
            logger.error(f"Error reading HDFS file: {e}")
            raise 