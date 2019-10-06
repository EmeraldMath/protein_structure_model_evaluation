import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import numpy as np
from operator import add
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
from io import BytesIO
import time
import datetime


def str_to_record(obj):
    args = ("./LIC", "protein-structures", obj)
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
                           env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                           'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
    output = popen.stdout.read()
    pro_contains_resArry = output.decode()
    row = pro_contains_resArry[:-1].splitlines()
    protein = [[float(j) for j in i.split()] for i in pro_contains_resArry[:-1].splitlines()]

    protein = bytearray(np.array(protein))
    obj = obj.strip().split('/')
    str_id = obj[-1]
    return Row(str_id, obj[-2], protein, obj[-3])


def native_to_record(obj):
    args = ("./LIC", "protein-structures", obj)
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
                           env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                           'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
    output = popen.stdout.read()
    pro_contains_resArry = output.decode()
    row = pro_contains_resArry[:-1].splitlines()
    protein = [[float(j) for j in i.split()] for i in pro_contains_resArry[:-1].splitlines()]
    protein = bytearray(np.array(protein))

    obj = obj.strip().split('/')
    return Row(obj[-1], protein, obj[-2], 100.0)
    

def score_to_record(x):
    id = x[0].split('/')
    id = id[-1].split('.')
    str_id = id[-2]
    body = x[1]
    start = body.find("SUMMARY(GDT)")
    if (start == -1):
        protein_record = Row(str_id, -1.0)
    else:
        score = body[start:start+75].split()
        score = score[-3]
        protein_record = Row(str_id, float(score))
    
    return protein_record


def set_test_dir():
    struct_dir = "s3a://protein-structures/test_data/test_str.txt"
    score_dir = 's3a://protein-structures/test_data/test_scores/'
    native_dir = 's3a://protein-structures/protein_raw_data/native.txt'
    return struct_dir, score_dir, native_dir


def set_dir():
    struct_dir = 's3a://protein-structures/protein_raw_data/structures.txt'
    score_dir = 's3a://protein-structures/protein_raw_data/casp_protein_scores/'
    native_dir = 's3a://protein-structures/protein_raw_data/native.txt'
    return struct_dir, score_dir, native_dir


def main():
    #struct_dir, score_dir, native_dir = set_test_dir()
    struct_dir, score_dir, native_dir = set_dir()
    
    url ='postgresql://10.0.0.8:5432/structure_evaluation'
    properties = {'user':db_user,'password':db_password,'driver':'org.postgresql.Driver'}
    conf = SparkConf().setAll([('spark.executor.memory', '5g'),('spark.driver.memory', '5g')])
    spark_session = SparkSession.builder.master("spark://ip-10-0-0-12:7077").appName("s3ToPostgre").config(conf=conf).getOrCreate()
    sc = spark_session.sparkContext
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    
    '''
    extract and computing contact_map distributedly for predicted structures
    '''
    key_str = sc.textFile(struct_dir)
    df_str = key_str.map(str_to_record).toDF()
    df_str = df_str.withColumnRenamed("_1","candidate_id").withColumnRenamed("_2","protein_id")\
        .withColumnRenamed("_3","contact_map").withColumnRenamed("_4","candidate_source")
    
    '''
    read score distributely
    '''
    df_score = sc.wholeTextFiles(score_dir + '*/*', minPartitions=250).map(score_to_record).toDF()
    df_score = df_score.withColumnRenamed('_1', 'candidate_id').withColumnRenamed('_2', 'score')
    df_score = df_score.filter('score!=-1')
    
    '''
    inner join two df to make sure the traning data is good. But still keep the raw data
    as df_str and df_score
    '''
    df_str.createOrReplaceTempView("candidates")
    df_score.createOrReplaceTempView("scores")
    df_join = spark_session.sql('select candidates.candidate_id, candidates.contact_map, candidates.candidate_source, scores.score\
        from candidates inner join scores on candidates.candidate_id = scores.candidate_id')
    
    #df_str.show()
    #df_score.show()
    #df_join.show()
    
    '''
    extract scores and computing contact_map distributedly for predicted structures
    '''
    key_native = sc.textFile(native_dir)
    df_native = key_native.map(native_to_record).toDF()
    df_native = df_native.withColumnRenamed("_1","candidate_id").withColumnRenamed("_2","contact_map")\
        .withColumnRenamed("_3","candidate_source").withColumnRenamed("_4","score")    
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df = df_join.unionAll(df_native).withColumn('timestamp', unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    
    #df.show()
    df.write.jdbc(url='jdbc:%s' % url, table='candidates', mode='overwrite',  properties=properties)
    

if __name__ == "__main__":
    main()