import logging
from pathlib import Path
from client import DremioClient, QueryType
import pandas as pd
from datetime import datetime
import json
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            
            # Test 3: Complex query
            logger.info(f"Testing complex query on {cluster_name} cluster...")
            complex_query = """
            SELECT 
                i_item_id,
                i_item_desc,
                i_category,
                i_class,
                i_current_price,
                SUM(ss_ext_sales_price) as itemrevenue
            FROM store_sales, item, date_dim
            WHERE ss_item_sk = i_item_sk
            AND i_category IN ('Sports', 'Books', 'Home')
            AND ss_sold_date_sk = d_date_sk
            AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
            GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
            ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
            """
            result = client.execute_query(complex_query)
            results.append({
                "operation": "complex_query",
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
            
            # Test 2: Multi-table transfer
            logger.info("Testing multi-table transfer...")
            tables = ["table1", "table2", "table3"]
            
            for table in tables:
                self.source_client.create_test_table(table, columns)
                self.source_client.ingest_data_methodology(table, test_data, "direct")
                self.target_client.transfer_table(self.source_client, table)
            
            results.append({
                "operation": "multi_table_transfer",
                "status": "success",
                "tables_transferred": len(tables)
            })
            
            # Cleanup
            for table in tables:
                self.source_client.execute_query(f"DROP TABLE IF EXISTS {table}")
                self.target_client.execute_query(f"DROP TABLE IF EXISTS {table}")
            
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

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all test operations."""
        logger.info("Starting comprehensive cluster operations testing...")
        
        # Test read operations on both clusters
        source_read_results = self.test_read_operations("source")
        target_read_results = self.test_read_operations("target")
        
        # Test write operations on both clusters
        source_write_results = self.test_write_operations("source")
        target_write_results = self.test_write_operations("target")
        
        # Test transfer operations
        transfer_results = self.test_transfer_operations()
        
        # Compile all results
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "read_operations": {
                "source": source_read_results,
                "target": target_read_results
            },
            "write_operations": {
                "source": source_write_results,
                "target": target_write_results
            },
            "transfer_operations": transfer_results
        }
        
        # Save results
        self._save_results(all_results)
        
        return all_results

    def _save_results(self, results: Dict[str, Any]) -> None:
        """Save test results to file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = Path("test_results") / f"cluster_operations_{timestamp}.json"
        results_file.parent.mkdir(exist_ok=True)
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Test results saved to {results_file}")

def main():
    """Main function to run all tests."""
    config_path = "config/dremio_config.yaml"
    
    tester = ClusterOperationsTester(config_path)
    results = tester.run_all_tests()
    
    # Print summary
    logger.info("\nTest Summary:")
    logger.info(f"Total operations tested: {len(results['read_operations']) + len(results['write_operations']) + 1}")
    logger.info(f"Results saved to: test_results/cluster_operations_*.json")

if __name__ == "__main__":
    main() 