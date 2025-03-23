import os
import logging
from typing import List, Dict, Any
from src.dremio.client import DremioClient
from src.hadoop.client import HadoopClient
from src.common.metrics import MetricsCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BenchmarkRunner:
    def __init__(self):
        """Initialize benchmark runner."""
        self.dremio_client = DremioClient("config/dremio_config.yaml")
        self.hadoop_client = HadoopClient("config/hadoop_config.yaml")
        self.metrics_collector = MetricsCollector()

    def load_queries(self, query_file: str) -> List[Dict[str, Any]]:
        """Load benchmark queries from file."""
        # TODO: Implement query loading from file
        return []

    def run_query(self, query: Dict[str, Any], system: str) -> Dict[str, Any]:
        """Run a single query on specified system."""
        client = self.dremio_client if system == "dremio" else self.hadoop_client
        
        try:
            query_info = self.metrics_collector.start_query(
                query_id=query["id"],
                query_type=query["type"],
                query=query["sql"]
            )
            
            results = client.execute_query(query["sql"])
            metrics = self.metrics_collector.end_query(query_info, results)
            
            logger.info(f"Successfully executed {query['id']} on {system}")
            return metrics
        except Exception as e:
            logger.error(f"Error executing {query['id']} on {system}: {e}")
            return None

    def run_benchmark(self, queries: List[Dict[str, Any]]):
        """Run benchmark suite."""
        systems = ["dremio", "hadoop"]
        
        for query in queries:
            for system in systems:
                self.run_query(query, system)
        
        # Save metrics
        self.metrics_collector.save_metrics()
        
        # Print summary
        summary = self.metrics_collector.get_summary()
        logger.info("Benchmark Summary:")
        for key, value in summary.items():
            logger.info(f"{key}: {value}")

def main():
    """Main entry point."""
    runner = BenchmarkRunner()
    
    # Load queries
    queries = runner.load_queries("queries/benchmark_queries.yaml")
    
    # Run benchmark
    runner.run_benchmark(queries)

if __name__ == "__main__":
    main() 