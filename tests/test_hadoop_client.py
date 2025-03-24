import pytest
from src.hadoop.client import HadoopClient
import os

@pytest.fixture
def hadoop_client():
    """Create a Hadoop client instance for testing."""
    config_path = "config/hadoop_config.yaml"
    if not os.path.exists(config_path):
        pytest.skip("Hadoop config file not found")
    return HadoopClient(config_path)

def test_hadoop_client_initialization(hadoop_client):
    """Test if Hadoop client initializes correctly."""
    assert hadoop_client is not None
    assert hasattr(hadoop_client, 'hdfs_client')
    assert hasattr(hadoop_client, 'hive_client')

def test_hadoop_query_execution(hadoop_client):
    """Test if we can execute a simple query."""
    test_query = "SELECT 1 as test"
    try:
        result = hadoop_client.execute_query(test_query)
        assert result is not None
        assert 'columns' in result
        assert 'rows' in result
        assert len(result['rows']) > 0
    except Exception as e:
        pytest.fail(f"Failed to execute query: {str(e)}")

def test_hdfs_operations(hadoop_client):
    """Test basic HDFS operations."""
    test_path = "/test"
    try:
        # Test listing directory
        hadoop_client.list_hdfs_path("/")
    except Exception as e:
        pytest.fail(f"Failed to list HDFS path: {str(e)}") 