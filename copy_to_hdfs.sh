#!/bin/bash

# Define the S3 source paths
S3_ORDERS_PATH="s3a://amittrendytech/orders/orders.csv"
S3_CUSTOMERS_PATH="s3a://amittrendytech/customers/customers.csv"

# Define the HDFS destinations
HDFS_ORDERS_PATH="/user/hadoop/orders/"
HDFS_CUSTOMERS_PATH="/user/hadoop/customers/"

# Copy files directly from S3 to HDFS
hdfs dfs -cp $S3_ORDERS_PATH $HDFS_ORDERS_PATH
hdfs dfs -cp $S3_CUSTOMERS_PATH $HDFS_CUSTOMERS_PATH

echo "Data copy from S3 to HDFS completed."

