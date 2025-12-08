#!/bin/bash
set -e

echo "Starting Spark Thrift Server in LOCAL mode..."

sleep 10

/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --master local[*] \
  --conf hive.server2.thrift.bind.host=${HIVE_SERVER2_THRIFT_BIND_HOST} \
  --conf hive.server2.thrift.port=${HIVE_SERVER2_THRIFT_PORT} \
  --conf hive.server2.authentication=${HIVE_SERVER2_THRIFT_AUTHENTICATION} \
  --conf hive.metastore.uris=${THRIFT_HIVE_METASTORE} \
  --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT_URL} \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=true