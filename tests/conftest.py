import pytest
from pathlib import Path
from src.dremio.client import DremioClient

@pytest.fixture(scope="session")
def config_path():
    """Provide the path to the Dremio configuration file."""
    return "config/dremio_config.yaml"

@pytest.fixture(scope="session")
def source_client(config_path):
    """Create a DremioClient instance for the source cluster."""
    return DremioClient(config_path, "dremio_source")

@pytest.fixture(scope="session")
def target_client(config_path):
    """Create a DremioClient instance for the target cluster."""
    return DremioClient(config_path, "dremio_target")

@pytest.fixture(scope="session")
def test_results_dir():
    """Create and provide the path to the test results directory."""
    results_dir = Path("test_results")
    results_dir.mkdir(exist_ok=True)
    return results_dir 