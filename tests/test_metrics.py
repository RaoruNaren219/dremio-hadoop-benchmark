import pytest
from src.common.metrics import MetricsCollector
import os
import shutil

@pytest.fixture
def metrics_collector():
    """Create a metrics collector instance for testing."""
    test_dir = "test_results"
    collector = MetricsCollector(results_dir=test_dir)
    yield collector
    # Cleanup after tests
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

def test_metrics_collector_initialization(metrics_collector):
    """Test if metrics collector initializes correctly."""
    assert metrics_collector is not None
    assert hasattr(metrics_collector, 'metrics')
    assert isinstance(metrics_collector.metrics, list)

def test_query_tracking(metrics_collector):
    """Test if we can track a query execution."""
    query_info = metrics_collector.start_query(
        query_id="test_query",
        query_type="SELECT",
        query="SELECT 1"
    )
    
    assert query_info is not None
    assert "start_time" in query_info
    assert "query_id" in query_info
    
    results = {"rows": [[1]], "columns": ["test"]}
    metrics = metrics_collector.end_query(query_info, results)
    
    assert metrics is not None
    assert "execution_time" in metrics
    assert "row_count" in metrics
    assert metrics["row_count"] == 1

def test_metrics_saving(metrics_collector):
    """Test if we can save metrics to file."""
    # Add some test metrics
    query_info = metrics_collector.start_query(
        query_id="test_query",
        query_type="SELECT",
        query="SELECT 1"
    )
    results = {"rows": [[1]], "columns": ["test"]}
    metrics_collector.end_query(query_info, results)
    
    # Save metrics
    metrics_collector.save_metrics("test_metrics.json")
    
    # Check if files were created
    assert os.path.exists(os.path.join(metrics_collector.results_dir, "test_metrics.json"))
    assert os.path.exists(os.path.join(metrics_collector.results_dir, "test_metrics.csv"))

def test_metrics_summary(metrics_collector):
    """Test if we can get metrics summary."""
    # Add some test metrics
    query_info = metrics_collector.start_query(
        query_id="test_query",
        query_type="SELECT",
        query="SELECT 1"
    )
    results = {"rows": [[1]], "columns": ["test"]}
    metrics_collector.end_query(query_info, results)
    
    summary = metrics_collector.get_summary()
    
    assert summary is not None
    assert "total_queries" in summary
    assert "avg_execution_time" in summary
    assert summary["total_queries"] == 1 