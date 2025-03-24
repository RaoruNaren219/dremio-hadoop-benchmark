import pytest
from src.dremio.client import DremioClient
from datetime import datetime

@pytest.fixture
def dremio_client():
    """Create a DremioClient instance for testing."""
    config_path = "config/dremio_config.yaml"
    return DremioClient(config_path, "dremio_source")

def test_client_initialization(dremio_client):
    """Test DremioClient initialization."""
    assert dremio_client is not None
    assert dremio_client.config is not None
    assert dremio_client.session is not None

def test_simple_query(dremio_client):
    """Test executing a simple query."""
    result = dremio_client.execute_query("SELECT 1")
    assert result is not None
    assert 'rows' in result
    assert len(result['rows']) > 0

def test_create_test_table(dremio_client):
    """Test creating a test table."""
    table_name = f"test_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    columns = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "VARCHAR"}
    ]
    
    dremio_client.create_test_table(table_name, columns)
    
    # Verify table exists
    result = dremio_client.execute_query(f"SELECT * FROM {table_name} LIMIT 1")
    assert result is not None
    
    # Cleanup
    dremio_client.execute_query(f"DROP TABLE IF EXISTS {table_name}")

def test_ingest_data(dremio_client):
    """Test data ingestion."""
    table_name = f"test_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    columns = [
        {"name": "id", "type": "INTEGER"},
        {"name": "value", "type": "VARCHAR"}
    ]
    
    dremio_client.create_test_table(table_name, columns)
    
    test_data = [
        {"id": 1, "value": "test1"},
        {"id": 2, "value": "test2"}
    ]
    
    result = dremio_client.ingest_data_methodology(table_name, test_data, "direct")
    assert result is not None
    assert result.get('status') == 'success'
    
    # Verify data
    result = dremio_client.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
    assert result['rows'][0]['count'] == 2
    
    # Cleanup
    dremio_client.execute_query(f"DROP TABLE IF EXISTS {table_name}") 