import argparse
import logging
from pathlib import Path
from benchmark import DremioBenchmark
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Run DremioMetrics performance analysis')
    parser.add_argument('--config', default='config/benchmark_config.yaml',
                      help='Path to metrics configuration file')
    parser.add_argument('--scale-factor', type=float, default=1.0,
                      help='Scale factor for TPC-DS data generation')
    parser.add_argument('--workload', choices=['bi_reporting', 'analytical', 'all'],
                      default='all', help='Workload type to analyze')
    parser.add_argument('--output-dir', default='benchmark_results',
                      help='Directory to save metrics results')
    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    try:
        # Initialize benchmark runner
        benchmark = DremioBenchmark(args.config)
        benchmark.results_dir = output_dir

        # Prepare environment
        logger.info(f"Preparing environment with scale factor {args.scale_factor}")
        benchmark.prepare_environment(args.scale_factor)

        # Run benchmarks based on workload type
        if args.workload == 'all':
            logger.info("Running all workload analyses")
            results = benchmark.run_all_benchmarks()
        else:
            logger.info(f"Running {args.workload} workload analysis")
            results = benchmark.run_workload(args.workload)

        # Save results
        benchmark.save_results(results)
        logger.info(f"Metrics results saved to {output_dir}")

        # Print summary
        print("\nMetrics Analysis Summary:")
        print(f"Timestamp: {results['timestamp']}")
        print(f"Total Queries: {len(results.get('queries', []))}")
        print(f"Successful Queries: {sum(1 for q in results.get('queries', []) if q.get('status') == 'success')}")
        print(f"Failed Queries: {sum(1 for q in results.get('queries', []) if q.get('status') == 'failed')}")
        
        if 'objectives' in results:
            print("\nObjectives Results:")
            for obj in results['objectives']:
                print(f"- {obj['name']}: {obj['status']}")

    except Exception as e:
        logger.error(f"Error running metrics analysis: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main() 