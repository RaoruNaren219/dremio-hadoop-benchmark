import pytest
import logging
from pathlib import Path
from src.dremio.benchmark import DremioBenchmark
from datetime import datetime
import json
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def benchmark_runner():
    """Create a DremioBenchmark instance for testing."""
    config_path = "config/benchmark_config.yaml"
    return DremioBenchmark(config_path)

def test_benchmark_initialization(benchmark_runner):
    """Test benchmark runner initialization."""
    assert benchmark_runner is not None
    assert benchmark_runner.config is not None
    assert benchmark_runner.source_client is not None
    assert benchmark_runner.target_client is not None
    assert benchmark_runner.results_dir.exists()

def test_query_execution_benchmark(benchmark_runner):
    """Test query execution benchmark."""
    # Run a simple query benchmark
    query_config = {
        "name": "test_query",
        "query": "SELECT 1"
    }
    
    results = benchmark_runner._execute_query(query_config)
    
    assert results["query_name"] == "test_query"
    assert len(results["execution_times"]) > 0
    assert results["avg_time"] > 0
    assert results["min_time"] > 0
    assert results["max_time"] > 0
    assert len(results["resource_usage"]) > 0

def test_workload_execution(benchmark_runner):
    """Test workload execution."""
    results = benchmark_runner.run_workload("bi_reporting")
    
    assert results["workload_type"] == "bi_reporting"
    assert len(results["queries"]) > 0
    assert "timestamp" in results

def test_parallel_query_execution(benchmark_runner):
    """Test parallel query execution."""
    queries = [
        {"name": "query1", "query": "SELECT 1"},
        {"name": "query2", "query": "SELECT 2"}
    ]
    
    results = benchmark_runner.run_parallel_queries(queries)
    
    assert len(results) == 2
    assert all(r["query_name"] in ["query1", "query2"] for r in results)

def test_results_saving(benchmark_runner, tmp_path):
    """Test saving benchmark results."""
    # Override results directory for testing
    benchmark_runner.results_dir = tmp_path
    
    test_results = {
        "timestamp": datetime.now().isoformat(),
        "queries": [
            {
                "query_name": "test_query",
                "execution_times": [1.0, 2.0, 3.0],
                "avg_time": 2.0,
                "min_time": 1.0,
                "max_time": 3.0,
                "resource_usage": [{"cpu_percent": 50, "memory_bytes": 1000}],
                "rows_processed": 100
            }
        ]
    }
    
    benchmark_runner.save_results(test_results)
    
    # Check JSON file
    json_files = list(tmp_path.glob("benchmark_results_*.json"))
    assert len(json_files) == 1
    
    with open(json_files[0], 'r') as f:
        saved_results = json.load(f)
        assert saved_results == test_results
    
    # Check CSV file
    csv_files = list(tmp_path.glob("benchmark_results_*.csv"))
    assert len(csv_files) == 1

def test_comprehensive_benchmark(benchmark_runner):
    """Test running comprehensive benchmark suite."""
    results = benchmark_runner.run_all_benchmarks()
    
    assert "timestamp" in results
    assert "objectives" in results
    assert "workloads" in results
    assert len(results["objectives"]) > 0
    assert len(results["workloads"]) > 0 