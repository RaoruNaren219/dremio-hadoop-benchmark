#!/bin/bash

# Check if base path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <hdfs_base_path>"
    echo "Example: $0 /user/test"
    exit 1
fi

BASE_PATH=$1
NUM_DIRS=5
PARTITIONS_PER_DIR=100

echo "Creating test directories in $BASE_PATH"

# Create base directory if it doesn't exist
hdfs dfs -mkdir -p "$BASE_PATH"

# Create multiple test directories with partitions
for dir_num in $(seq 1 $NUM_DIRS); do
    DIR_PATH="$BASE_PATH/test_dir_$dir_num"
    echo "Creating directory: $DIR_PATH"
    
    # Create the directory
    hdfs dfs -mkdir -p "$DIR_PATH"
    
    # Create partitions
    for part_num in $(seq 1 $PARTITIONS_PER_DIR); do
        PARTITION_PATH="$DIR_PATH/partition_$part_num"
        echo "Creating partition: $PARTITION_PATH"
        
        # Create a dummy file in each partition
        echo "dummy data" | hdfs dfs -put - "$PARTITION_PATH/dummy.txt"
    done
done

echo "Test directories created successfully!"
echo "Created $NUM_DIRS directories with $PARTITIONS_PER_DIR partitions each"
echo "Total partitions: $((NUM_DIRS * PARTITIONS_PER_DIR))" 