import logging
from pathlib import Path
from client import DremioClient, QueryType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_cluster_connection(client: DremioClient, cluster_name: str) -> bool:
    """Test connection to a Dremio cluster."""
    try:
        # Try a simple query to verify connection
        result = client.execute_query("SELECT 1")
        logger.info(f"Successfully connected to {cluster_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to {cluster_name}: {str(e)}")
        return False

def test_data_transfer(source_client: DremioClient, target_client: DremioClient) -> None:
    """Test data transfer between clusters."""
    # Test single table transfer
    logger.info("Testing single table transfer...")
    try:
        # Create test table in source
        source_client.create_test_table("test_table", 1000)
        
        # Transfer table to target
        target_client.transfer_table(source_client, "test_table")
        
        # Verify data in target
        result = target_client.execute_query("SELECT COUNT(*) as count FROM test_table")
        logger.info(f"Transferred {result['rows'][0]['count']} rows to target")
        
        # Clean up
        source_client.execute_query("DROP TABLE IF EXISTS test_table")
        target_client.execute_query("DROP TABLE IF EXISTS test_table")
        
    except Exception as e:
        logger.error(f"Single table transfer failed: {str(e)}")
    
    # Test different file formats
    formats = ["parquet", "orc", "csv"]
    for fmt in formats:
        logger.info(f"Testing {fmt} format transfer...")
        try:
            # Create test table with specific format
            source_client.create_test_table(f"test_table_{fmt}", 1000, file_format=fmt)
            
            # Transfer table to target
            target_client.transfer_table(source_client, f"test_table_{fmt}")
            
            # Verify data in target
            result = target_client.execute_query(f"SELECT COUNT(*) as count FROM test_table_{fmt}")
            logger.info(f"Transferred {result['rows'][0]['count']} rows in {fmt} format")
            
            # Clean up
            source_client.execute_query(f"DROP TABLE IF EXISTS test_table_{fmt}")
            target_client.execute_query(f"DROP TABLE IF EXISTS test_table_{fmt}")
            
        except Exception as e:
            logger.error(f"{fmt} format transfer failed: {str(e)}")

def main():
    """Main function to run tests."""
    config_path = "config/dremio_config.yaml"
    
    # Initialize clients
    source_client = DremioClient(config_path, "dremio_source")
    target_client = DremioClient(config_path, "dremio_target")
    
    # Test connections
    logger.info("Testing cluster connections...")
    source_connected = test_cluster_connection(source_client, "source")
    target_connected = test_cluster_connection(target_client, "target")
    
    if source_connected and target_connected:
        logger.info("Both clusters connected successfully")
        
        # Test data transfer
        logger.info("Starting data transfer tests...")
        test_data_transfer(source_client, target_client)
        
        # Run benchmark scenarios
        logger.info("Running benchmark scenarios...")
        for scenario in ["single_table", "multi_table", "data_mart", "format_conversion"]:
            logger.info(f"Testing {scenario} scenario...")
            try:
                if scenario == "single_table":
                    source_client.run_benchmark(scale_factor=100, query_type=QueryType.BI_REPORTING)
                elif scenario == "multi_table":
                    source_client.run_benchmark(scale_factor=100, query_type=QueryType.ANALYTICAL)
                elif scenario == "data_mart":
                    source_client.run_benchmark(scale_factor=100, query_type=QueryType.AD_HOC)
                elif scenario == "format_conversion":
                    source_client.run_benchmark(scale_factor=100, query_type=QueryType.BATCH)
            except Exception as e:
                logger.error(f"{scenario} scenario failed: {str(e)}")
    else:
        logger.error("Failed to connect to one or both clusters")

if __name__ == "__main__":
    main() 