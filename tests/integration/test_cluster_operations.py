import pytest
import logging
from pathlib import Path
from src.dremio.client import DremioClient
from datetime import datetime
import json
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def cluster_tester():
    """Create a ClusterOperationsTester instance for testing."""
    config_path = "config/dremio_config.yaml"
    return ClusterOperationsTester(config_path)

class ClusterOperationsTester:
    def __init__(self, config_path: str):
        """Initialize testers for both clusters."""
        self.source_client = DremioClient(config_path, "dremio_source")
        self.target_client = DremioClient(config_path, "dremio_target")
        self.test_results = []

    def test_read_operations(self, cluster_name: str) -> Dict[str, Any]:
        """Test reading operations on specified cluster."""
        client = self.source_client if cluster_name == "source" else self.target_client
        results = []
        
        try:
            # Test 1: Simple SELECT
            logger.info(f"Testing simple SELECT on {cluster_name} cluster...")
            result = client.execute_query("SELECT 1")
            results.append({
                "operation": "simple_select",
                "status": "success",
                "rows": len(result.get('rows', []))
            })
            
            # Test 2: Read from existing table
            logger.info(f"Testing table read on {cluster_name} cluster...")
            result = client.execute_query("SELECT * FROM Apollo.test_table LIMIT 10")
            results.append({
                "operation": "table_read",
                "status": "success",
                "rows": len(result.get('rows', []))
            })
            
        except Exception as e:
            logger.error(f"Read operations failed on {cluster_name} cluster: {str(e)}")
            results.append({
                "operation": "read_operations",
                "status": "failed",
                "error": str(e)
            })
        
        return {
            "cluster": cluster_name,
            "operations": results
        }

    def test_write_operations(self, cluster_name: str) -> Dict[str, Any]:
        """Test writing operations on specified cluster."""
        client = self.source_client if cluster_name == "source" else self.target_client
        results = []
        
        try:
            # Test 1: Create and write to test table
            logger.info(f"Testing table creation and write on {cluster_name} cluster...")
            table_name = f"test_write_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create test table
            columns = [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "value", "type": "DECIMAL(10,2)"}
            ]
            client.create_test_table(table_name, columns)
            
            # Insert test data
            test_data = [
                {"id": 1, "name": "test1", "value": 100.50},
                {"id": 2, "name": "test2", "value": 200.75}
            ]
            client.ingest_data_methodology(table_name, test_data, "direct")
            
            # Verify data
            result = client.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
            results.append({
                "operation": "create_and_write",
                "status": "success",
                "rows_written": result['rows'][0]['count']
            })
            
            # Cleanup
            client.execute_query(f"DROP TABLE IF EXISTS {table_name}")
            
        except Exception as e:
            logger.error(f"Write operations failed on {cluster_name} cluster: {str(e)}")
            results.append({
                "operation": "write_operations",
                "status": "failed",
                "error": str(e)
            })
        
        return {
            "cluster": cluster_name,
            "operations": results
        }

    def test_transfer_operations(self) -> Dict[str, Any]:
        """Test data transfer operations between clusters."""
        results = []
        
        try:
            # Test 1: Single table transfer
            logger.info("Testing single table transfer...")
            source_table = "test_transfer_source"
            target_table = "test_transfer_target"
            
            # Create test table in source
            columns = [
                {"name": "id", "type": "INTEGER"},
                {"name": "data", "type": "VARCHAR"},
                {"name": "timestamp", "type": "TIMESTAMP"}
            ]
            self.source_client.create_test_table(source_table, columns)
            
            # Insert test data
            test_data = [
                {"id": 1, "data": "test1", "timestamp": datetime.now()},
                {"id": 2, "data": "test2", "timestamp": datetime.now()}
            ]
            self.source_client.ingest_data_methodology(source_table, test_data, "direct")
            
            # Transfer table
            self.target_client.transfer_table(self.source_client, source_table)
            
            # Verify transfer
            result = self.target_client.execute_query(f"SELECT COUNT(*) as count FROM {target_table}")
            results.append({
                "operation": "single_table_transfer",
                "status": "success",
                "rows_transferred": result['rows'][0]['count']
            })
            
            # Cleanup
            self.source_client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
            self.target_client.execute_query(f"DROP TABLE IF EXISTS {target_table}")
            
        except Exception as e:
            logger.error(f"Transfer operations failed: {str(e)}")
            results.append({
                "operation": "transfer_operations",
                "status": "failed",
                "error": str(e)
            })
        
        return {
            "operation_type": "transfer",
            "results": results
        }

def test_source_read_operations(cluster_tester):
    """Test read operations on source cluster."""
    results = cluster_tester.test_read_operations("source")
    assert results["cluster"] == "source"
    assert len(results["operations"]) > 0
    assert all(op["status"] == "success" for op in results["operations"])

def test_target_read_operations(cluster_tester):
    """Test read operations on target cluster."""
    results = cluster_tester.test_read_operations("target")
    assert results["cluster"] == "target"
    assert len(results["operations"]) > 0
    assert all(op["status"] == "success" for op in results["operations"])

def test_source_write_operations(cluster_tester):
    """Test write operations on source cluster."""
    results = cluster_tester.test_write_operations("source")
    assert results["cluster"] == "source"
    assert len(results["operations"]) > 0
    assert all(op["status"] == "success" for op in results["operations"])

def test_target_write_operations(cluster_tester):
    """Test write operations on target cluster."""
    results = cluster_tester.test_write_operations("target")
    assert results["cluster"] == "target"
    assert len(results["operations"]) > 0
    assert all(op["status"] == "success" for op in results["operations"])

def test_transfer_operations(cluster_tester):
    """Test data transfer operations between clusters."""
    results = cluster_tester.test_transfer_operations()
    assert results["operation_type"] == "transfer"
    assert len(results["results"]) > 0
    assert all(op["status"] == "success" for op in results["results"]) 