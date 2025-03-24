import argparse
import logging
from dremio.client import DremioClusterBenchmark
import json
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Run Dremio cluster benchmark')
    parser.add_argument('--config', default='config/dremio_config.yaml',
                      help='Path to configuration file')
    parser.add_argument('--output', default='results',
                      help='Directory to store results')
    args = parser.parse_args()

    # Create results directory
    os.makedirs(args.output, exist_ok=True)

    try:
        # Initialize and run benchmark
        logger.info("Initializing benchmark...")
        benchmark = DremioClusterBenchmark(args.config)
        
        logger.info("Starting benchmark...")
        benchmark.run_benchmark()
        
        # Get and save results
        summary = benchmark.get_summary()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save summary
        summary_file = os.path.join(args.output, f'summary_{timestamp}.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Save detailed metrics
        metrics_file = os.path.join(args.output, f'metrics_{timestamp}.json')
        with open(metrics_file, 'w') as f:
            json.dump(benchmark.metrics, f, indent=2)
        
        logger.info(f"Benchmark completed successfully")
        logger.info(f"Summary saved to: {summary_file}")
        logger.info(f"Detailed metrics saved to: {metrics_file}")
        
        # Print summary
        logger.info("\nBenchmark Summary:")
        for key, value in summary.items():
            logger.info(f"{key}: {value}")
            
    except Exception as e:
        logger.error(f"Benchmark failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 