#!/bin/bash

# Check if HDFS path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <hdfs_path>"
    echo "Example: $0 /path/to/hdfs/directory"
    exit 1
fi

HDFS_PATH=$1
BATCH_DIR="partition_batches"

# Create directory for batch files
mkdir -p "$BATCH_DIR"

# Get list of partitions and count total splits
echo "Listing partitions in $HDFS_PATH..."
TOTAL_SPLITS=$(hdfs dfs -ls "$HDFS_PATH" | grep -v "Found" | wc -l)

if [ $TOTAL_SPLITS -eq 0 ]; then
    echo "No partitions found"
    rm -rf "$BATCH_DIR"
    exit 0
fi

# Calculate batch size (aim for 10-20 batches)
if [ $TOTAL_SPLITS -le 100 ]; then
    BATCH_SIZE=10
elif [ $TOTAL_SPLITS -le 500 ]; then
    BATCH_SIZE=25
elif [ $TOTAL_SPLITS -le 1000 ]; then
    BATCH_SIZE=50
else
    BATCH_SIZE=100
fi

echo "Found $TOTAL_SPLITS total splits"
echo "Using batch size of $BATCH_SIZE splits per batch"

# Save all partitions to a file for review
echo "Saving partition list for review..."
hdfs dfs -ls "$HDFS_PATH" | grep -v "Found" | awk '{print $8}' > "$BATCH_DIR/all_partitions.txt"

# Show first few partitions as preview
echo -e "\nPreview of first 5 partitions:"
head -n 5 "$BATCH_DIR/all_partitions.txt"
echo -e "\n... and last 5 partitions:"
tail -n 5 "$BATCH_DIR/all_partitions.txt"

# Ask for confirmation
echo -e "\nWARNING: This will permanently delete $TOTAL_SPLITS partitions from:"
echo "$HDFS_PATH"
echo -e "\nAre you sure you want to proceed? (yes/no)"
read CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Operation cancelled."
    rm -rf "$BATCH_DIR"
    exit 1
fi

# Split partitions into files
echo "Splitting partitions into batches..."
split -l $BATCH_SIZE "$BATCH_DIR/all_partitions.txt" "$BATCH_DIR/batch_"

TOTAL_BATCHES=$(ls "$BATCH_DIR/batch_"* | wc -l)
echo "Split into $TOTAL_BATCHES batches"

# Process each batch file
for batch_file in "$BATCH_DIR/batch_"*; do
    echo -e "\nProcessing batch file: $batch_file"
    echo "Partitions in this batch:"
    cat "$batch_file"
    echo -e "\nPress Enter to start deleting this batch (or Ctrl+C to stop)..."
    read
    
    while read -r partition; do
        echo "Deleting: $partition"
        hdfs dfs -rm -r -skipTrash "$partition"
    done < "$batch_file"
    
    echo "Batch completed. Press Enter to continue with next batch..."
    read
done

# Cleanup
rm -rf "$BATCH_DIR"
echo "All partitions deleted!" 