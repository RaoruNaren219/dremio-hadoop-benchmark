import pytest
import logging
from pathlib import Path
from src.dremio.client import DremioClient, QueryType
from datetime import datetime
import json
from typing import Dict, Any
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def benchmark_tester():
    """Create a BenchmarkTester instance for testing."""
    config_path = "config/dremio_config.yaml"
    return BenchmarkTester(config_path)

class BenchmarkTester:
    def __init__(self, config_path: str):
        """Initialize benchmark tester."""
        self.client = DremioClient(config_path, "dremio_source")
        self.results = []

    def benchmark_query_execution(self, query: str, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark query execution time."""
        execution_times = []
        
        for i in range(iterations):
            start_time = time.time()
            result = self.client.execute_query(query)
            end_time = time.time()
            
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            
            logger.info(f"Iteration {i+1}/{iterations}: {execution_time:.2f} seconds")
        
        return {
            "query": query,
            "iterations": iterations,
            "avg_time": sum(execution_times) / len(execution_times),
            "min_time": min(execution_times),
            "max_time": max(execution_times),
            "execution_times": execution_times
        }

    def benchmark_data_ingestion(self, table_name: str, data_size: int = 1000) -> Dict[str, Any]:
        """Benchmark data ingestion performance."""
        # Generate test data
        test_data = [
            {
                "id": i,
                "name": f"test{i}",
                "value": i * 100.50,
                "timestamp": datetime.now()
            }
            for i in range(data_size)
        ]
        
        # Create test table
        columns = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "value", "type": "DECIMAL(10,2)"},
            {"name": "timestamp", "type": "TIMESTAMP"}
        ]
        
        start_time = time.time()
        self.client.create_test_table(table_name, columns)
        self.client.ingest_data_methodology(table_name, test_data, "direct")
        end_time = time.time()
        
        execution_time = end_time - start_time
        rows_per_second = data_size / execution_time
        
        return {
            "operation": "data_ingestion",
            "rows": data_size,
            "execution_time": execution_time,
            "rows_per_second": rows_per_second
        }

    def benchmark_table_transfer(self, source_table: str, target_table: str) -> Dict[str, Any]:
        """Benchmark table transfer performance."""
        start_time = time.time()
        self.client.transfer_table(self.client, source_table)
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Get row count
        result = self.client.execute_query(f"SELECT COUNT(*) as count FROM {target_table}")
        row_count = result['rows'][0]['count']
        
        return {
            "operation": "table_transfer",
            "rows": row_count,
            "execution_time": execution_time,
            "rows_per_second": row_count / execution_time
        }

    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Run all benchmark tests."""
        logger.info("Starting comprehensive benchmark testing...")
        
        # Test 1: Query execution benchmark
        query = """
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
        query_results = self.benchmark_query_execution(query)
        
        # Test 2: Data ingestion benchmark
        table_name = f"benchmark_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        ingestion_results = self.benchmark_data_ingestion(table_name)
        
        # Test 3: Table transfer benchmark
        source_table = f"benchmark_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        target_table = f"benchmark_target_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create source table with test data
        self.benchmark_data_ingestion(source_table)
        transfer_results = self.benchmark_table_transfer(source_table, target_table)
        
        # Compile all results
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "query_execution": query_results,
            "data_ingestion": ingestion_results,
            "table_transfer": transfer_results
        }
        
        # Save results
        self._save_results(all_results)
        
        # Cleanup
        self.client.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        self.client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
        self.client.execute_query(f"DROP TABLE IF EXISTS {target_table}")
        
        return all_results

    def _save_results(self, results: Dict[str, Any]) -> None:
        """Save benchmark results to file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = Path("test_results") / f"benchmark_results_{timestamp}.json"
        results_file.parent.mkdir(exist_ok=True)
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Benchmark results saved to {results_file}")

def test_query_execution_benchmark(benchmark_tester):
    """Test query execution benchmark."""
    query = "SELECT 1"
    results = benchmark_tester.benchmark_query_execution(query, iterations=3)
    assert results["query"] == query
    assert results["iterations"] == 3
    assert results["avg_time"] > 0
    assert results["min_time"] > 0
    assert results["max_time"] > 0

def test_data_ingestion_benchmark(benchmark_tester):
    """Test data ingestion benchmark."""
    table_name = f"benchmark_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    results = benchmark_tester.benchmark_data_ingestion(table_name, data_size=100)
    assert results["operation"] == "data_ingestion"
    assert results["rows"] == 100
    assert results["execution_time"] > 0
    assert results["rows_per_second"] > 0
    
    # Cleanup
    benchmark_tester.client.execute_query(f"DROP TABLE IF EXISTS {table_name}")

def test_table_transfer_benchmark(benchmark_tester):
    """Test table transfer benchmark."""
    source_table = f"benchmark_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    target_table = f"benchmark_target_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Create source table with test data
    benchmark_tester.benchmark_data_ingestion(source_table, data_size=100)
    results = benchmark_tester.benchmark_table_transfer(source_table, target_table)
    
    assert results["operation"] == "table_transfer"
    assert results["rows"] > 0
    assert results["execution_time"] > 0
    assert results["rows_per_second"] > 0
    
    # Cleanup
    benchmark_tester.client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
    benchmark_tester.client.execute_query(f"DROP TABLE IF EXISTS {target_table}") 