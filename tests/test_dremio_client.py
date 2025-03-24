import pytest
from src.dremio.client import DremioClient
import os

@pytest.fixture
def dremio_client():
    """Create a Dremio client instance for testing."""
    config_path = "config/dremio_config.yaml"
    if not os.path.exists(config_path):
        pytest.skip("Dremio config file not found")
    return DremioClient(config_path)

def test_dremio_client_initialization(dremio_client):
    """Test if Dremio client initializes correctly."""
    assert dremio_client is not None
    assert hasattr(dremio_client, 'base_url')
    assert hasattr(dremio_client, 'session')

def test_dremio_catalog(dremio_client):
    """Test if we can get the Dremio catalog."""
    try:
        catalog = dremio_client.get_catalog()
        assert catalog is not None
    except Exception as e:
        pytest.fail(f"Failed to get catalog: {str(e)}")

def test_dremio_query_execution(dremio_client):
    """Test if we can execute a simple query."""
    test_query = "SELECT 1 as test"
    try:
        result = dremio_client.execute_query(test_query)
        assert result is not None
        assert 'rows' in result
        assert len(result['rows']) > 0
    except Exception as e:
        pytest.fail(f"Failed to execute query: {str(e)}") 