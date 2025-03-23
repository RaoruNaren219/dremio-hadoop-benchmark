import time
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
import json
import os

class MetricsCollector:
    def __init__(self, results_dir: str = "results/raw"):
        """Initialize metrics collector."""
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)
        self.metrics = []

    def start_query(self, query_id: str, query_type: str, query: str) -> Dict[str, Any]:
        """Start timing a query execution."""
        return {
            "query_id": query_id,
            "query_type": query_type,
            "query": query,
            "start_time": time.time(),
            "start_datetime": datetime.now().isoformat()
        }

    def end_query(self, query_info: Dict[str, Any], results: Dict[str, Any]) -> Dict[str, Any]:
        """End timing a query execution and collect metrics."""
        end_time = time.time()
        execution_time = end_time - query_info["start_time"]
        
        metrics = {
            **query_info,
            "end_time": end_time,
            "end_datetime": datetime.now().isoformat(),
            "execution_time": execution_time,
            "row_count": len(results.get("rows", [])),
            "column_count": len(results.get("columns", [])),
            "success": True
        }
        
        self.metrics.append(metrics)
        return metrics

    def save_metrics(self, filename: str = None):
        """Save collected metrics to a file."""
        if not filename:
            filename = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = os.path.join(self.results_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        
        # Also save as CSV for easier analysis
        csv_filepath = os.path.join(self.results_dir, filename.replace('.json', '.csv'))
        df = pd.DataFrame(self.metrics)
        df.to_csv(csv_filepath, index=False)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of collected metrics."""
        if not self.metrics:
            return {}
        
        df = pd.DataFrame(self.metrics)
        return {
            "total_queries": len(df),
            "avg_execution_time": df["execution_time"].mean(),
            "min_execution_time": df["execution_time"].min(),
            "max_execution_time": df["execution_time"].max(),
            "total_rows_processed": df["row_count"].sum(),
            "success_rate": (df["success"].sum() / len(df)) * 100
        } 