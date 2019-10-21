#!/bin/sh
spark-submit  --jars postgresql-42.2.8.jar --files ./LIC --master spark://ip-10-0-0-12:7077  --driver-memory 30g --executor-memory 30g etl.py
