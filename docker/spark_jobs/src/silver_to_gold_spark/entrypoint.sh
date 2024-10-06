#!/bin/bash


PYSPARK_FILEPATH="/app/silver_to_gold_spark/app.py"
MASTER="spark://spark:7077"

TOTAL_EXEC_CORES=1
EXEC_MEMORY=512M

echo "spark-submit                                                 "
echo "    --deploy-mode client                                     "
echo "    --executor-memory ${EXEC_MEMORY}                         "
echo "    --total-executor-cores ${TOTAL_EXEC_CORES}               "

spark-submit \
--deploy-mode client \
--total-executor-cores $TOTAL_EXEC_CORES \
--executor-memory $EXEC_MEMORY \
${PYSPARK_FILEPATH}