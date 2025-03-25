#!/bin/bash

# Check if HDFS path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <hdfs_path>"
    echo "Example: $0 /path/to/hdfs/directory"
    exit 1
fi

HDFS_PATH=$1
BATCH_SIZE=50
BATCH_DIR="partition_batches"

# Create directory for batch files
mkdir -p "$BATCH_DIR"

# Get list of partitions and split into files
echo "Listing partitions in $HDFS_PATH..."
hdfs dfs -ls "$HDFS_PATH" | grep -v "Found" | awk '{print $8}' | split -l $BATCH_SIZE - "$BATCH_DIR/batch_"

TOTAL_BATCHES=$(ls "$BATCH_DIR/batch_"* | wc -l)
echo "Split into $TOTAL_BATCHES batches of $BATCH_SIZE partitions each"

# Process each batch file
for batch_file in "$BATCH_DIR/batch_"*; do
    echo "Processing batch file: $batch_file"
    echo "Press Enter to start deleting this batch (or Ctrl+C to stop)..."
    read
    
    while read -r partition; do
        echo "Deleting: $partition"
        hdfs dfs -rm -r "$partition"
    done < "$batch_file"
    
    echo "Batch completed. Press Enter to continue with next batch..."
    read
done

# Cleanup
rm -rf "$BATCH_DIR"
echo "All partitions deleted!" 