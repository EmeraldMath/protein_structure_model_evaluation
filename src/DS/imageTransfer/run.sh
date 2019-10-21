#!/bin/sh
spark-submit --master spark://ip-10-0-0-12:7077 --driver-memory 30g --executor-memory 30g  --jars postgresql-42.2.8.jar --conf spark.network.timeout=10000000 bi2pic.py