# DremioMetrics

A comprehensive metrics collection and analysis suite for evaluating Dremio's performance across different workloads and data sizes. DremioMetrics provides a standardized way to measure, collect, and analyze Dremio's query performance, resource utilization, and scalability metrics.

## Features

- TPC-DS data generation and preparation
- Multiple workload types (BI Reporting, Analytical)
- Resource usage monitoring
- Parallel query execution
- Detailed performance metrics collection
- Results export in JSON and CSV formats

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/dremiometrics.git
cd dremiometrics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
```

## Configuration

1. Copy the example configuration:
```bash
cp config/metrics_config.yaml.example config/metrics_config.yaml
```

2. Update the configuration file with your Dremio cluster details and metrics settings.

## Usage

Run the metrics collection tool:

```bash
# Run all workloads
dremiometrics

# Run specific workload
dremiometrics --workload bi_reporting

# Run with custom scale factor
dremiometrics --scale-factor 10.0

# Run with custom config and output directory
dremiometrics --config custom_config.yaml --output-dir my_results
```

## Metrics Results

Results are saved in the specified output directory (default: `benchmark_results/`) in both JSON and CSV formats. The results include:

- Query execution times
- Resource utilization
- Throughput metrics
- Success/failure statistics

## Development

### Running Tests

```bash
pytest tests/
```

### Project Structure

```
dremiometrics/
├── config/
│   └── metrics_config.yaml
├── src/
│   └── dremio/
│       ├── client.py
│       ├── benchmark.py
│       └── run_benchmarks.py
├── tests/
│   └── benchmark/
│       └── test_benchmark.py
├── requirements.txt
└── setup.py
```

## License

MIT License 