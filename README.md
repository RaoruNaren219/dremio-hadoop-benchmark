# Dremio Hadoop Benchmark

This project provides tools and scripts for benchmarking Dremio against Hadoop-based data processing systems.

## Project Structure

```
dremio-hadoop-benchmark/
├── config/                 # Configuration files
│   ├── dremio_config.yaml  # Dremio connection settings
│   └── hadoop_config.yaml  # Hadoop cluster settings
├── data/                   # Sample data and test datasets
│   ├── raw/               # Raw data files
│   └── processed/         # Processed data files
├── src/                    # Source code
│   ├── dremio/            # Dremio-specific code
│   │   ├── client.py      # Dremio client implementation
│   │   └── queries.py     # Dremio query definitions
│   ├── hadoop/            # Hadoop-specific code
│   │   ├── client.py      # Hadoop client implementation
│   │   └── queries.py     # Hadoop query definitions
│   └── common/            # Shared utilities
│       ├── metrics.py     # Performance metrics collection
│       └── utils.py       # Common utility functions
├── tests/                 # Test files
│   ├── test_dremio.py     # Dremio-specific tests
│   └── test_hadoop.py     # Hadoop-specific tests
├── results/               # Benchmark results
│   ├── raw/              # Raw benchmark data
│   └── reports/          # Generated reports
├── notebooks/            # Jupyter notebooks for analysis
├── requirements.txt      # Python dependencies
└── README.md            # Project documentation
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your environment:
- Copy `config/dremio_config.yaml.example` to `config/dremio_config.yaml`
- Copy `config/hadoop_config.yaml.example` to `config/hadoop_config.yaml`
- Update the configuration files with your settings

## Usage

1. Run benchmarks:
```bash
python src/run_benchmarks.py
```

2. Generate reports:
```bash
python src/generate_reports.py
```

3. View results:
- Check the `results/reports` directory for generated reports
- Use Jupyter notebooks in the `notebooks` directory for detailed analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 