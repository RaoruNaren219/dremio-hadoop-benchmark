import pytest
from src.dremio.client import DremioClusterBenchmark
import os
import yaml

@pytest.fixture
def benchmark_config():
    """Create a test configuration."""
    return {
        'dremio_clusters': {
            'source': {
                'host': 'source-dremio-host',
                'port': 9047,
                'username': 'test_user',
                'password': 'test_password',
                'ssl': False,
                'auth_type': 'basic',
                'catalog': 'test_catalog',
                'schema': 'test_schema'
            },
            'target': {
                'host': 'target-dremio-host',
                'port': 9047,
                'username': 'test_user',
                'password': 'test_password',
                'ssl': False,
                'auth_type': 'basic',
                'catalog': 'test_catalog',
                'schema': 'test_schema'
            }
        },
        'query_settings': {
            'timeout': 60,
            'max_rows': 1000,
            'fetch_size': 100,
            'batch_size': 1000
        },
        'benchmark': {
            'test_tables': [
                {
                    'name': 'test_table_1',
                    'size_mb': 1,  # Small size for testing
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER'},
                        {'name': 'value', 'type': 'VARCHAR'}
                    ]
                }
            ],
            'iterations': 2,
            'warmup_runs': 1
        }
    }

@pytest.fixture
def benchmark(benchmark_config, tmp_path):
    """Create a benchmark instance with test configuration."""
    config_path = tmp_path / "test_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(benchmark_config, f)
    return DremioClusterBenchmark(str(config_path))

def test_benchmark_initialization(benchmark):
    """Test if benchmark initializes correctly."""
    assert benchmark is not None
    assert hasattr(benchmark, 'source_client')
    assert hasattr(benchmark, 'target_client')
    assert hasattr(benchmark, 'metrics')

def test_benchmark_run(benchmark):
    """Test if benchmark runs without errors."""
    try:
        benchmark.run_benchmark()
    except Exception as e:
        pytest.fail(f"Benchmark failed: {str(e)}")

def test_metrics_collection(benchmark):
    """Test if metrics are collected correctly."""
    benchmark.run_benchmark()
    summary = benchmark.get_summary()
    
    assert summary is not None
    assert "total_tables" in summary
    assert "total_iterations" in summary
    assert "avg_execution_time" in summary
    assert "avg_rows_per_second" in summary

def test_test_data_generation(benchmark):
    """Test if test data is generated correctly."""
    table_config = benchmark.config['benchmark']['test_tables'][0]
    test_data = benchmark._generate_test_data(table_config)
    
    assert len(test_data) > 0
    assert all(isinstance(row, dict) for row in test_data)
    assert all('id' in row and 'value' in row for row in test_data)

def test_table_operations(benchmark):
    """Test table creation and data insertion."""
    table_config = benchmark.config['benchmark']['test_tables'][0]
    table_name = table_config['name']
    
    try:
        # Test table creation
        benchmark.source_client.create_test_table(table_name, table_config['columns'])
        benchmark.target_client.create_test_table(table_name, table_config['columns'])
        
        # Test data insertion
        test_data = benchmark._generate_test_data(table_config)
        benchmark.source_client.insert_data(table_name, test_data)
        
        # Test data transfer
        metrics = benchmark.source_client.transfer_data(table_name, table_name)
        assert metrics['total_rows'] > 0
        assert metrics['execution_time'] > 0
        assert metrics['rows_per_second'] > 0
        
    finally:
        # Cleanup
        benchmark.source_client.drop_table(table_name)
        benchmark.target_client.drop_table(table_name) 