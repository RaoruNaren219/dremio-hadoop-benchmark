import argparse
import logging
import subprocess
import time
from pathlib import Path
from typing import List, Optional
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSPartitionDeleter:
    def __init__(self, hdfs_path: str, batch_size: int = 50, delay: int = 2):
        """
        Initialize the HDFS partition deleter.
        
        Args:
            hdfs_path: The HDFS path containing partitions
            batch_size: Number of partitions to delete in each batch
            delay: Delay between batches in seconds
        """
        self.hdfs_path = hdfs_path
        self.batch_size = batch_size
        self.delay = delay
        self.deleted_partitions = []
        self.failed_partitions = []
        self.results_file = f"partition_deletion_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    def list_partitions(self) -> List[str]:
        """List all partitions in the HDFS path."""
        try:
            cmd = f"hdfs dfs -ls {self.hdfs_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed to list partitions: {result.stderr}")
                return []
            
            # Parse the output to get partition paths
            partitions = []
            for line in result.stdout.split('\n'):
                if line.strip() and 'Found' not in line:
                    # Extract the partition path from the ls output
                    parts = line.split()
                    if len(parts) >= 8:
                        partition_path = parts[-1]
                        if partition_path.startswith(self.hdfs_path):
                            partitions.append(partition_path)
            
            logger.info(f"Found {len(partitions)} partitions")
            return partitions
        except Exception as e:
            logger.error(f"Error listing partitions: {str(e)}")
            return []

    def delete_partition(self, partition_path: str) -> bool:
        """Delete a single partition."""
        try:
            cmd = f"hdfs dfs -rm -r {partition_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully deleted: {partition_path}")
                return True
            else:
                logger.error(f"Failed to delete {partition_path}: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Error deleting partition {partition_path}: {str(e)}")
            return False

    def delete_batch(self, partitions: List[str]) -> None:
        """Delete a batch of partitions."""
        for partition in partitions:
            success = self.delete_partition(partition)
            if success:
                self.deleted_partitions.append(partition)
            else:
                self.failed_partitions.append(partition)
        
        # Save progress after each batch
        self.save_results()

    def save_results(self) -> None:
        """Save deletion results to a JSON file."""
        results = {
            "timestamp": datetime.now().isoformat(),
            "hdfs_path": self.hdfs_path,
            "total_deleted": len(self.deleted_partitions),
            "total_failed": len(self.failed_partitions),
            "deleted_partitions": self.deleted_partitions,
            "failed_partitions": self.failed_partitions
        }
        
        with open(self.results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {self.results_file}")

    def run(self) -> None:
        """Run the partition deletion process."""
        logger.info(f"Starting partition deletion for {self.hdfs_path}")
        
        # Get all partitions
        partitions = self.list_partitions()
        if not partitions:
            logger.error("No partitions found or error listing partitions")
            return
        
        total_partitions = len(partitions)
        logger.info(f"Total partitions to process: {total_partitions}")
        
        # Process partitions in batches
        for i in range(0, total_partitions, self.batch_size):
            batch = partitions[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1} of {(total_partitions + self.batch_size - 1)//self.batch_size}")
            logger.info(f"Batch size: {len(batch)} partitions")
            
            self.delete_batch(batch)
            
            if i + self.batch_size < total_partitions:
                logger.info(f"Waiting {self.delay} seconds before next batch...")
                time.sleep(self.delay)
        
        # Final summary
        logger.info("\nDeletion Summary:")
        logger.info(f"Total partitions processed: {total_partitions}")
        logger.info(f"Successfully deleted: {len(self.deleted_partitions)}")
        logger.info(f"Failed to delete: {len(self.failed_partitions)}")
        
        if self.failed_partitions:
            logger.warning("\nFailed partitions:")
            for partition in self.failed_partitions:
                logger.warning(f"- {partition}")

def main():
    parser = argparse.ArgumentParser(description='Delete HDFS partitions in batches')
    parser.add_argument('hdfs_path', help='HDFS path containing partitions')
    parser.add_argument('--batch-size', type=int, default=50,
                      help='Number of partitions to delete in each batch')
    parser.add_argument('--delay', type=int, default=2,
                      help='Delay between batches in seconds')
    args = parser.parse_args()

    try:
        deleter = HDFSPartitionDeleter(
            hdfs_path=args.hdfs_path,
            batch_size=args.batch_size,
            delay=args.delay
        )
        deleter.run()
    except Exception as e:
        logger.error(f"Error during partition deletion: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main() 