import logging
from client import DremioClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_space(client: DremioClient, space_name: str) -> bool:
    """Create a new space in Dremio."""
    try:
        # First, check if the space already exists
        check_query = f"SELECT 1 FROM sys.spaces WHERE name = '{space_name}'"
        result = client.execute_query(check_query)
        
        if result.get('rows'):
            logger.info(f"Space '{space_name}' already exists")
            return True
            
        # Create the space
        create_query = f"CREATE SPACE {space_name}"
        client.execute_query(create_query)
        logger.info(f"Successfully created space '{space_name}'")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create space '{space_name}': {str(e)}")
        return False

def main():
    """Main function to create space in target cluster."""
    config_path = "config/dremio_config.yaml"
    
    # Initialize target client
    target_client = DremioClient(config_path, "dremio_target")
    
    # Create space
    space_name = "Apollo_Target"
    if create_space(target_client, space_name):
        logger.info("Space creation completed successfully")
    else:
        logger.error("Failed to create space")

if __name__ == "__main__":
    main() 