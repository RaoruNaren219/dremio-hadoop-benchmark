import requests
import yaml
from typing import Dict, Any, Optional, List
import logging
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import random
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import csv
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryType(Enum):
    BI_REPORTING = "bi_reporting"
    AD_HOC = "ad_hoc"
    ANALYTICAL = "analytical"
    STREAMING = "streaming"
    BATCH = "batch"

class DremioClient:
    def __init__(self, config_path: str, cluster_name: str):
        """Initialize Dremio client with configuration."""
        self.config = self._load_config(config_path)
        self.cluster_name = cluster_name
        self.cluster_config = self.config['dremio_clusters'][cluster_name]
        self.base_url = f"http://{self.cluster_config['host']}:{self.cluster_config['port']}"
        self.session = requests.Session()
        self._setup_authentication()
        self.metrics_dir = Path("metrics")
        self.metrics_dir.mkdir(exist_ok=True)
        self.tpcds_dir = Path("tpcds")
        self.tpcds_dir.mkdir(exist_ok=True)
        self.jmeter_dir = Path("jmeter")
        self.jmeter_dir.mkdir(exist_ok=True)
        self.query_cache = {}

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_authentication(self):
        """Setup authentication based on config."""
        auth_type = self.cluster_config['auth_type']
        if auth_type == 'basic':
            self.session.auth = (
                self.cluster_config['username'],
                self.cluster_config['password']
            )
        elif auth_type == 'token':
            self.session.headers.update({
                'Authorization': f"Bearer {self.cluster_config['token']}"
            })

    def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        endpoint = f"{self.base_url}/api/v3/sql"
        payload = {
            "sql": query,
            "context": [],
            "timeout": self.config['query_settings']['timeout']
        }
        
        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing query on {self.cluster_name}: {e}")
            raise

    def get_catalog(self) -> Dict[str, Any]:
        """Get Dremio catalog information."""
        endpoint = f"{self.base_url}/api/v3/catalog"
        try:
            response = self.session.get(endpoint)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting catalog from {self.cluster_name}: {e}")
            raise

    def create_test_table(self, table_name: str, columns: List[Dict[str, str]], file_format: Dict[str, str] = None) -> None:
        """Create a test table with specified columns and file format."""
        columns_def = ", ".join([f"{col['name']} {col['type']}" for col in columns])
        
        # Add file format specification if provided
        format_clause = ""
        if file_format:
            format_clause = f"STORED AS {file_format['type'].upper()}"
            if 'compression' in file_format:
                format_clause += f" WITH COMPRESSION '{file_format['compression']}'"

        query = f"""
        CREATE TABLE {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{table_name} (
            {columns_def}
        )
        {format_clause}
        """
        self.execute_query(query)

    def drop_table(self, table_name: str) -> None:
        """Drop a table if it exists."""
        query = f"""
        DROP TABLE IF EXISTS {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{table_name}
        """
        self.execute_query(query)

    def ingest_data_methodology(self, table_name: str, data: List[Dict[str, Any]], methodology: str = "direct") -> Dict[str, Any]:
        """Ingest data using specified methodology."""
        start_time = time.time()
        metrics = {
            "methodology": methodology,
            "start_time": start_time,
            "rows_ingested": len(data)
        }

        try:
            if methodology == "direct":
                self._direct_insert(table_name, data)
            elif methodology == "batch":
                self._batch_insert(table_name, data)
            elif methodology == "stream":
                self._stream_insert(table_name, data)
            elif methodology == "bulk":
                self._bulk_insert(table_name, data)
            elif methodology == "parallel":
                self._parallel_insert(table_name, data)
            elif methodology == "partitioned":
                self._partitioned_insert(table_name, data)
            else:
                raise ValueError(f"Unsupported ingestion methodology: {methodology}")

            end_time = time.time()
            metrics.update({
                "end_time": end_time,
                "execution_time": end_time - start_time,
                "rows_per_second": len(data) / (end_time - start_time),
                "status": "success"
            })
            
            self._save_metrics(f"ingest_{methodology}_{table_name}", metrics)
            return metrics

        except Exception as e:
            end_time = time.time()
            metrics.update({
                "end_time": end_time,
                "execution_time": end_time - start_time,
                "error": str(e),
                "status": "failed"
            })
            self._save_metrics(f"ingest_{methodology}_{table_name}", metrics)
            raise

    def _direct_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Direct INSERT methodology."""
        columns = list(data[0].keys())
        values_str = ", ".join([f"({', '.join(str(v) for v in row)})" for row in data])
        query = f"""
        INSERT INTO {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{table_name}
        ({', '.join(columns)})
        VALUES {values_str}
        """
        self.execute_query(query)

    def _batch_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Batch INSERT methodology."""
        batch_size = self.config['query_settings']['batch_size']
        df = pd.DataFrame(data)
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            self._direct_insert(table_name, batch.to_dict('records'))

    def _stream_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Stream INSERT methodology using smaller batches."""
        stream_batch_size = 1000  # Smaller batch size for streaming
        df = pd.DataFrame(data)
        
        for i in range(0, len(df), stream_batch_size):
            batch = df.iloc[i:i + stream_batch_size]
            self._direct_insert(table_name, batch.to_dict('records'))
            time.sleep(0.1)  # Small delay between batches

    def _bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Bulk INSERT methodology using temporary staging table."""
        staging_table = f"{table_name}_staging"
        
        # Create staging table
        self.create_test_table(staging_table, [{"name": k, "type": "VARCHAR(MAX)"} for k in data[0].keys()])
        
        # Insert into staging
        self._direct_insert(staging_table, data)
        
        # Bulk transfer to target
        query = f"""
        INSERT INTO {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{table_name}
        SELECT * FROM {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{staging_table}
        """
        self.execute_query(query)
        
        # Cleanup
        self.drop_table(staging_table)

    def transfer_data(self, source_table: str, target_table: str, batch_size: int = None) -> Dict[str, Any]:
        """Transfer data from source table to target table."""
        if batch_size is None:
            batch_size = self.config['query_settings']['batch_size']

        # Get total count
        count_query = f"""
        SELECT COUNT(*) as total
        FROM {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{source_table}
        """
        total_count = self.execute_query(count_query)['rows'][0][0]

        # Transfer in batches
        start_time = time.time()
        transferred_rows = 0

        while transferred_rows < total_count:
            query = f"""
            INSERT INTO {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{target_table}
            SELECT *
            FROM {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{source_table}
            ORDER BY 1
            LIMIT {batch_size}
            OFFSET {transferred_rows}
            """
            self.execute_query(query)
            transferred_rows += batch_size

        end_time = time.time()
        return {
            "total_rows": total_count,
            "execution_time": end_time - start_time,
            "rows_per_second": total_count / (end_time - start_time)
        }

    def transfer_data_with_format(self, source_table: str, target_table: str, 
                                source_format: Dict[str, str], target_format: Dict[str, str]) -> Dict[str, Any]:
        """Transfer data with format conversion."""
        start_time = time.time()
        
        # Create target table with specified format
        self.create_test_table(target_table, self._get_table_columns(source_table), target_format)
        
        # Transfer with format conversion
        query = f"""
        INSERT INTO {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{target_table}
        SELECT *
        FROM {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{source_table}
        """
        self.execute_query(query)
        
        end_time = time.time()
        return {
            "execution_time": end_time - start_time,
            "source_format": source_format,
            "target_format": target_format
        }

    def _get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get column definitions for a table."""
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        result = self.execute_query(query)
        return [{"name": row[0], "type": row[1]} for row in result['rows']]

    def prepare_tpcds_data(self, scale_factor: int = 1000) -> None:
        """Prepare TPC-DS test data with specified scale factor."""
        logger.info(f"Preparing TPC-DS data with scale factor {scale_factor}")
        
        # Generate TPC-DS data
        self._generate_tpcds_data(scale_factor)
        
        # Convert to Parquet format
        self._convert_to_parquet()
        
        # Load into Dremio
        self._load_tpcds_data()

    def _generate_tpcds_data(self, scale_factor: int) -> None:
        """Generate TPC-DS test data using dsdgen."""
        output_dir = self.tpcds_dir / f"sf{scale_factor}"
        output_dir.mkdir(exist_ok=True)
        
        # Generate data using dsdgen
        cmd = f"dsdgen -scale {scale_factor} -dir {output_dir}"
        subprocess.run(cmd, shell=True, check=True)
        
        # Process and combine files
        self._process_tpcds_files(output_dir)

    def _process_tpcds_files(self, output_dir: Path) -> None:
        """Process and combine TPC-DS generated files."""
        for i in range(1, 25):  # TPC-DS has 24 tables
            filename = f"{i:02d}.dat"
            csvfile = output_dir / f"{filename}.csv"
            
            if (output_dir / filename).exists():
                # Add headers if needed
                if not csvfile.exists():
                    self._add_tpcds_headers(i, csvfile)
                
                # Append data
                with open(output_dir / filename, 'r') as src, open(csvfile, 'a') as dst:
                    dst.write(src.read())

    def _add_tpcds_headers(self, table_num: int, output_file: Path) -> None:
        """Add headers to TPC-DS data files."""
        headers = self._get_tpcds_headers(table_num)
        with open(output_file, 'w') as f:
            f.write("|".join(headers) + "\n")

    def _get_tpcds_headers(self, table_num: int) -> List[str]:
        """Get headers for TPC-DS tables."""
        # This is a simplified version - you should implement the full TPC-DS schema
        headers = {
            1: ["ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk"],
            2: ["cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk"],
            # Add more table headers as needed
        }
        return headers.get(table_num, [])

    def _convert_to_parquet(self) -> None:
        """Convert TPC-DS CSV files to Parquet format."""
        for scale_dir in self.tpcds_dir.glob("sf*"):
            parquet_dir = scale_dir / "parquet"
            parquet_dir.mkdir(exist_ok=True)
            
            for csv_file in scale_dir.glob("*.csv"):
                table_name = csv_file.stem.replace(".dat", "")
                parquet_file = parquet_dir / f"{table_name}.parquet"
                
                # Convert to Parquet using Dremio CTAS
                self._convert_csv_to_parquet(csv_file, parquet_file, table_name)

    def _convert_csv_to_parquet(self, csv_file: Path, parquet_file: Path, table_name: str) -> None:
        """Convert a single CSV file to Parquet using Dremio CTAS."""
        query = f"""
        CREATE TABLE {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{table_name}
        STORED AS PARQUET
        AS SELECT *
        FROM {self.cluster_config['catalog']}.{self.cluster_config['schema']}.{csv_file.name}
        """
        self.execute_query(query)

    def _get_benchmark_queries(self, query_type: Optional[QueryType] = None) -> List[Dict[str, Any]]:
        """Get benchmark queries based on type."""
        if query_type in self.query_cache:
            return self.query_cache[query_type]

        queries = {
            QueryType.BI_REPORTING: [
                {
                    "id": "q1",
                    "type": "bi_reporting",
                    "sql": """
                    SELECT 
                        i_item_id,
                        i_item_desc,
                        i_category,
                        i_class,
                        i_current_price,
                        SUM(ss_ext_sales_price) as itemrevenue,
                        SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) as revenueratio
                    FROM store_sales, item, date_dim
                    WHERE ss_item_sk = i_item_sk
                    AND i_category IN ('Sports', 'Books', 'Home')
                    AND ss_sold_date_sk = d_date_sk
                    AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
                    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
                    ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
                    """
                },
                {
                    "id": "q2",
                    "type": "bi_reporting",
                    "sql": """
                    SELECT 
                        i_item_id,
                        i_item_desc,
                        i_category,
                        i_class,
                        i_current_price,
                        SUM(ss_ext_sales_price) as itemrevenue,
                        SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) as revenueratio
                    FROM store_sales, item, date_dim
                    WHERE ss_item_sk = i_item_sk
                    AND i_category IN ('Sports', 'Books', 'Home')
                    AND ss_sold_date_sk = d_date_sk
                    AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
                    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
                    ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
                    """
                }
            ],
            QueryType.AD_HOC: [
                {
                    "id": "q3",
                    "type": "ad_hoc",
                    "sql": """
                    SELECT 
                        d_year,
                        i_brand_id,
                        i_brand,
                        SUM(ss_ext_sales_price) ext_price
                    FROM date_dim, store_sales, item
                    WHERE d_date_sk = ss_sold_date_sk
                    AND ss_item_sk = i_item_sk
                    AND i_manager_id = 1
                    AND d_moy = 12
                    AND d_year = 2000
                    GROUP BY d_year, i_brand, i_brand_id
                    ORDER BY d_year, ext_price DESC, i_brand_id
                    """
                }
            ],
            QueryType.ANALYTICAL: [
                {
                    "id": "q4",
                    "type": "analytical",
                    "sql": """
                    SELECT 
                        d_year,
                        i_category_id,
                        i_category,
                        SUM(ss_ext_sales_price) as total_sales
                    FROM date_dim, store_sales, item
                    WHERE d_date_sk = ss_sold_date_sk
                    AND ss_item_sk = i_item_sk
                    AND i_category IN ('Electronics', 'Sports', 'Toys')
                    AND d_year IN (1999, 2000, 2001)
                    GROUP BY d_year, i_category_id, i_category
                    ORDER BY d_year, total_sales DESC
                    """
                }
            ],
            QueryType.STREAMING: [
                {
                    "id": "q5",
                    "type": "streaming",
                    "sql": """
                    SELECT 
                        i_item_id,
                        i_item_desc,
                        i_category,
                        i_class,
                        i_current_price,
                        SUM(ss_ext_sales_price) as itemrevenue,
                        SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) as revenueratio
                    FROM store_sales, item, date_dim
                    WHERE ss_item_sk = i_item_sk
                    AND i_category IN ('Sports', 'Books', 'Home')
                    AND ss_sold_date_sk = d_date_sk
                    AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
                    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
                    ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
                    """
                }
            ],
            QueryType.BATCH: [
                {
                    "id": "q6",
                    "type": "batch",
                    "sql": """
                    SELECT 
                        i_item_id,
                        i_item_desc,
                        i_category,
                        i_class,
                        i_current_price,
                        SUM(ss_ext_sales_price) as itemrevenue,
                        SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) as revenueratio
                    FROM store_sales, item, date_dim
                    WHERE ss_item_sk = i_item_sk
                    AND i_category IN ('Sports', 'Books', 'Home')
                    AND ss_sold_date_sk = d_date_sk
                    AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
                    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
                    ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
                    """
                }
            ]
        }

        if query_type:
            self.query_cache[query_type] = queries.get(query_type, [])
            return self.query_cache[query_type]
        else:
            all_queries = [q for qs in queries.values() for q in qs]
            self.query_cache[None] = all_queries
            return all_queries

    def _execute_benchmark_queries(self, scale_factor: int, query_type: Optional[QueryType]) -> List[Dict[str, Any]]:
        """Execute benchmark queries and collect results."""
        results = []
        queries = self._get_benchmark_queries(query_type)
        
        for query in queries:
            start_time = time.time()
            try:
                # Execute query
                result = self.execute_query(query['sql'])
                
                # Get query plan
                plan = self._get_query_plan(query['sql'])
                
                end_time = time.time()
                
                # Collect detailed metrics
                metrics = {
                    "query_id": query['id'],
                    "query_type": query['type'],
                    "execution_time": end_time - start_time,
                    "rows": len(result.get('rows', [])),
                    "status": "success",
                    "plan": plan,
                    "memory_usage": self._get_memory_usage(),
                    "cpu_usage": self._get_cpu_usage(),
                    "io_stats": self._get_io_stats()
                }
                
                results.append(metrics)
                
            except Exception as e:
                end_time = time.time()
                results.append({
                    "query_id": query['id'],
                    "query_type": query['type'],
                    "execution_time": end_time - start_time,
                    "error": str(e),
                    "status": "failed"
                })
        
        return results

    def _get_query_plan(self, query: str) -> Dict[str, Any]:
        """Get query execution plan."""
        try:
            plan_query = f"EXPLAIN PLAN FOR {query}"
            result = self.execute_query(plan_query)
            return result
        except Exception as e:
            logger.error(f"Error getting query plan: {e}")
            return {}

    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics."""
        try:
            query = "SELECT * FROM sys.memory"
            result = self.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Error getting memory usage: {e}")
            return {}

    def _get_cpu_usage(self) -> Dict[str, Any]:
        """Get CPU usage statistics."""
        try:
            query = "SELECT * FROM sys.cpu"
            result = self.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Error getting CPU usage: {e}")
            return {}

    def _get_io_stats(self) -> Dict[str, Any]:
        """Get I/O statistics."""
        try:
            query = "SELECT * FROM sys.io"
            result = self.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Error getting I/O stats: {e}")
            return {}

    def _calculate_benchmark_metrics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate benchmark metrics from results."""
        df = pd.DataFrame(results)
        
        # Basic metrics
        metrics = {
            "total_queries": len(results),
            "successful_queries": len(df[df['status'] == 'success']),
            "failed_queries": len(df[df['status'] == 'failed']),
            "avg_execution_time": df['execution_time'].mean(),
            "min_execution_time": df['execution_time'].min(),
            "max_execution_time": df['execution_time'].max(),
            "total_rows": df['rows'].sum() if 'rows' in df else 0,
            "query_types": df.groupby('query_type')['execution_time'].agg(['mean', 'min', 'max']).to_dict()
        }
        
        # Performance metrics by query type
        for query_type in df['query_type'].unique():
            type_df = df[df['query_type'] == query_type]
            metrics[f"{query_type}_metrics"] = {
                "avg_execution_time": type_df['execution_time'].mean(),
                "min_execution_time": type_df['execution_time'].min(),
                "max_execution_time": type_df['execution_time'].max(),
                "success_rate": len(type_df[type_df['status'] == 'success']) / len(type_df),
                "avg_rows": type_df['rows'].mean() if 'rows' in type_df else 0
            }
        
        # Resource utilization metrics
        if 'memory_usage' in df.columns:
            metrics['memory_metrics'] = df['memory_usage'].apply(pd.Series).mean().to_dict()
        if 'cpu_usage' in df.columns:
            metrics['cpu_metrics'] = df['cpu_usage'].apply(pd.Series).mean().to_dict()
        if 'io_stats' in df.columns:
            metrics['io_metrics'] = df['io_stats'].apply(pd.Series).mean().to_dict()
        
        return metrics

    def run_benchmark(self, scale_factor: int = 1000, query_type: Optional[QueryType] = None, 
                     iterations: int = 3, warmup_runs: int = 1) -> Dict[str, Any]:
        """Run TPC-DS benchmark suite with multiple iterations."""
        logger.info(f"Starting TPC-DS benchmark with scale factor {scale_factor}")
        
        all_results = []
        
        # Prepare test data if needed
        if not (self.tpcds_dir / f"sf{scale_factor}").exists():
            self.prepare_tpcds_data(scale_factor)
        
        # Run warmup iterations
        logger.info("Running warmup iterations...")
        for _ in range(warmup_runs):
            self._refresh_metadata()
            self._execute_benchmark_queries(scale_factor, query_type)
        
        # Run actual benchmark iterations
        logger.info("Running benchmark iterations...")
        for iteration in range(iterations):
            logger.info(f"Starting iteration {iteration + 1}/{iterations}")
            
            # Refresh metadata before each iteration
            self._refresh_metadata()
            
            # Execute queries
            results = self._execute_benchmark_queries(scale_factor, query_type)
            all_results.extend(results)
            
            # Calculate and save metrics for this iteration
            iteration_metrics = self._calculate_benchmark_metrics(results)
            self._save_benchmark_results(iteration_metrics, scale_factor, iteration + 1)
        
        # Calculate overall metrics
        overall_metrics = self._calculate_benchmark_metrics(all_results)
        self._save_benchmark_results(overall_metrics, scale_factor, "overall")
        
        return overall_metrics

    def _save_benchmark_results(self, metrics: Dict[str, Any], scale_factor: int, iteration: Any) -> None:
        """Save benchmark results to file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_file = self.metrics_dir / f"benchmark_sf{scale_factor}_{iteration}_{timestamp}.json"
        
        with open(result_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info(f"Benchmark results saved to {result_file}")

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of all benchmark results."""
        results = []
        for result_file in self.metrics_dir.glob("benchmark_*.json"):
            with open(result_file, 'r') as f:
                results.append(json.load(f))
        
        if not results:
            return {}
        
        df = pd.DataFrame(results)
        return {
            "total_runs": len(results),
            "avg_success_rate": df['successful_queries'].mean() / df['total_queries'].mean(),
            "avg_execution_time": df['avg_execution_time'].mean(),
            "best_execution_time": df['avg_execution_time'].min(),
            "worst_execution_time": df['avg_execution_time'].max(),
            "total_rows_processed": df['total_rows'].sum()
        }

class DremioClusterBenchmark:
    def __init__(self, config_path: str):
        """Initialize benchmark runner for two Dremio clusters."""
        self.config = self._load_config(config_path)
        self.source_client = DremioClient(config_path, 'source')
        self.target_client = DremioClient(config_path, 'target')
        self.metrics = []

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def run_benchmark(self):
        """Run the complete benchmark suite."""
        for scenario in self.config['benchmark']['test_scenarios']:
            logger.info(f"Running scenario: {scenario['name']}")
            self._run_scenario(scenario)

    def _run_scenario(self, scenario: Dict[str, Any]):
        """Run a specific benchmark scenario."""
        if scenario['name'] == 'single_table_transfer':
            self._run_single_table_transfer(scenario)
        elif scenario['name'] == 'multi_table_transfer':
            self._run_multi_table_transfer(scenario)
        elif scenario['name'] == 'data_mart_transfer':
            self._run_data_mart_transfer(scenario)
        elif scenario['name'] == 'format_conversion':
            self._run_format_conversion(scenario)

    def _run_single_table_transfer(self, scenario: Dict[str, Any]):
        """Run single table transfer benchmark."""
        for data_mart in self.config['data_marts']:
            for table in data_mart['tables']:
                self._benchmark_table_transfer(table, scenario)

    def _run_multi_table_transfer(self, scenario: Dict[str, Any]):
        """Run multi-table transfer benchmark."""
        for data_mart in self.config['data_marts']:
            start_time = time.time()
            
            # Transfer all tables in the mart
            for table in data_mart['tables']:
                self._benchmark_table_transfer(table, scenario)
            
            end_time = time.time()
            self.metrics.append({
                "scenario": "multi_table_transfer",
                "data_mart": data_mart['name'],
                "execution_time": end_time - start_time,
                "tables_transferred": len(data_mart['tables'])
            })

    def _run_data_mart_transfer(self, scenario: Dict[str, Any]):
        """Run entire data mart transfer benchmark."""
        for data_mart in self.config['data_marts']:
            start_time = time.time()
            
            # Create mart schema
            self._create_mart_schema(data_mart)
            
            # Transfer all tables
            for table in data_mart['tables']:
                self._benchmark_table_transfer(table, scenario)
            
            end_time = time.time()
            self.metrics.append({
                "scenario": "data_mart_transfer",
                "data_mart": data_mart['name'],
                "execution_time": end_time - start_time,
                "tables_transferred": len(data_mart['tables'])
            })
            
            # Cleanup
            self._drop_mart_schema(data_mart)

    def _run_format_conversion(self, scenario: Dict[str, Any]):
        """Run format conversion benchmark."""
        for source_format in self.config['benchmark']['file_formats']:
            for target_format in self.config['benchmark']['file_formats']:
                if source_format != target_format:
                    for data_mart in self.config['data_marts']:
                        for table in data_mart['tables']:
                            self._benchmark_format_conversion(table, source_format, target_format, scenario)

    def _benchmark_table_transfer(self, table_config: Dict[str, Any], scenario: Dict[str, Any]):
        """Run benchmark for a single table transfer."""
        table_name = table_config['name']
        
        # Create tables
        self.source_client.create_test_table(table_name, table_config['columns'])
        self.target_client.create_test_table(table_name, table_config['columns'])
        
        # Generate and insert test data
        test_data = self._generate_test_data(table_config)
        self.source_client.ingest_data_methodology(table_name, test_data, "batch")
        
        # Run warmup iterations
        for _ in range(scenario['warmup_runs']):
            self.source_client.transfer_data(table_name, table_name)
        
        # Run actual benchmark iterations
        for iteration in range(scenario['iterations']):
            start_time = time.time()
            metrics = self.source_client.transfer_data(table_name, table_name)
            end_time = time.time()
            
            self.metrics.append({
                "scenario": scenario['name'],
                "table_name": table_name,
                "iteration": iteration + 1,
                "start_time": start_time,
                "end_time": end_time,
                "execution_time": end_time - start_time,
                "rows_transferred": metrics["total_rows"],
                "rows_per_second": metrics["rows_per_second"]
            })
        
        # Cleanup
        self.source_client.drop_table(table_name)
        self.target_client.drop_table(table_name)

    def _benchmark_format_conversion(self, table_config: Dict[str, Any], 
                                  source_format: Dict[str, str], target_format: Dict[str, str],
                                  scenario: Dict[str, Any]):
        """Run benchmark for format conversion."""
        table_name = table_config['name']
        source_table = f"{table_name}_source"
        target_table = f"{table_name}_target"
        
        try:
            # Create source table with source format
            self.source_client.create_test_table(source_table, table_config['columns'], source_format)
            
            # Generate and insert test data
            test_data = self._generate_test_data(table_config)
            self.source_client.ingest_data_methodology(source_table, test_data, "batch")
            
            # Run warmup iterations
            for _ in range(scenario['warmup_runs']):
                self.source_client.transfer_data_with_format(source_table, target_table, source_format, target_format)
            
            # Run actual benchmark iterations
            for iteration in range(scenario['iterations']):
                metrics = self.source_client.transfer_data_with_format(source_table, target_table, source_format, target_format)
                
                self.metrics.append({
                    "scenario": "format_conversion",
                    "table_name": table_name,
                    "iteration": iteration + 1,
                    "source_format": source_format['type'],
                    "target_format": target_format['type'],
                    "execution_time": metrics["execution_time"]
                })
        
        finally:
            # Cleanup
            self.source_client.drop_table(source_table)
            self.target_client.drop_table(target_table)

    def _create_mart_schema(self, data_mart: Dict[str, Any]):
        """Create schema for a data mart."""
        query = f"""
        CREATE SCHEMA IF NOT EXISTS {self.target_client.cluster_config['catalog']}.{data_mart['name']}
        """
        self.target_client.execute_query(query)

    def _drop_mart_schema(self, data_mart: Dict[str, Any]):
        """Drop schema for a data mart."""
        query = f"""
        DROP SCHEMA IF EXISTS {self.target_client.cluster_config['catalog']}.{data_mart['name']} CASCADE
        """
        self.target_client.execute_query(query)

    def _generate_test_data(self, table_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate test data based on table configuration."""
        data = []
        num_rows = (table_config['size_mb'] * 1024 * 1024) // 100  # Rough estimate of rows per MB
        
        for i in range(num_rows):
            row = {}
            for col in table_config['columns']:
                if col['type'] == 'INTEGER':
                    row[col['name']] = i
                elif col['type'] == 'VARCHAR':
                    row[col['name']] = f"value_{i}"
                elif col['type'] == 'DECIMAL':
                    row[col['name']] = round(random.uniform(0, 1000), 2)
                elif col['type'] == 'DATE':
                    row[col['name']] = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                elif col['type'] == 'TIMESTAMP':
                    row[col['name']] = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d %H:%M:%S')
            data.append(row)
        
        return data 