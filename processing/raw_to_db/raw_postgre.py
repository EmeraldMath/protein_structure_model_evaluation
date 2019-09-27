import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import boto3
import botocore
import io
import numpy as np
from operator import add
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import smart_open

def get_a_fold_keys(bucket_name, dir):
    """List files in specific S3 URL"""
    client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,\
                      aws_secret_access_key=AWS_SECRET_KEY)
    keys = []
    response = client.list_objects(Bucket=bucket_name, Prefix=dir)
    for content in response.get('Contents', []):
        keys.append((bucket_name, content.get('Key')))
    return keys

def get_all_s3_keys(bucket_name):
    """Get a list of all keys in an S3 bucket."""
    clinet = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,\
                      aws_secret_access_key=AWS_SECRET_KEY)
    keys = []

    kwargs = {'Bucket': bucket_name, 'Prefix': dir}
    while True:
        resp = clinet.list_objects_v2(Bucket=bucket_name)
        for obj in resp['Contents']:
            keys.append((bucket_name, obj['Key']))

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

#write the contact map to the database
def test_struct_id(obj):
    str_id = obj[-1]
    return str_id

def struct_id(obj):
    str_id = obj[-3] + "/" + obj[-1]
    return str_id

def test_score_id(obj):
    obj = obj.strip().split('/')
    obj = obj[-1].split('.')
    str_id = obj[-2]
    return str_id

def score_id(obj):
    obj = obj.strip().split('/')
    name = obj[-1].split('.')
    score_id = obj[-3] + "/" + name[0]
    return score_id

def str_to_record(x):
    bucket_name = x[0]
    obj = x[1]
    args = ("./LIC", "protein-structures", "test_data/test_structures/T0950/T0950TS470_2")
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
                           env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                           'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
    output = popen.stdout.read()
    pro_contains_resArry = output.decode()
    protein = np.array((float(j) for j in i.split()) for i in pro_contains_resArry[:-1].splitlines())
    #print(protein)
    protein = bytearray(protein)
    #print(protein)
    obj = obj.strip().split('/')
    str_id = test_struct_id(obj)
    protein_record = Row(str_id, obj[-2], protein)

    return protein_record


def score_to_record(x):
    session = boto3.Session(
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
    )
    bucket_name = x[0]
    obj = x[1]
    flag = False
    for line in smart_open.open('s3://'+bucket_name+'/'+obj, transport_params=dict(session=session)):
        if (line.startswith("SUMMARY(GDT)")):
            line = line.strip().split()
            str_id = test_score_id(obj)
            protein_record = Row(str_id, float(line[6]))
            break

    return protein_record


def main():
    bucket_name = 'protein-structures'
    struct_dir = 'test_data/test_structures/T0950/'
    score_dir = 'test_data/test_scores/T0950/'

    schema1 = StructType([
        StructField("structure_id", StringType(), False),
        StructField("casp_No", StringType(), False),
        StructField("contact_map", BinaryType(), False),
        ])

    schema2 = StructType([
        StructField("score_id", StringType(), False),
        StructField("score", FloatType(), False)
        ])


    url ='postgresql://10.0.0.8:5432/structure_evaluation'
    properties = {'user':db_user,'password':db_password,'driver':'org.postgresql.Driver'}
    spark_session =SparkSession.builder.master("spark://ip-10-0-0-12:7077").appName("s3ToLIC").getOrCreate()
    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    '''
    extract and computing contact_map distributedly
    '''
    structs = get_a_fold_keys(bucket_name, struct_dir)
    rdd = sc.parallelize(structs)
    row_rdd = rdd.map(str_to_record)
   
    '''
    extract scores distributedly
    '''
    scores = get_a_fold_keys(bucket_name, score_dir)
    rdd = sc.parallelize(scores)
    row_rdd = rdd.map(score_to_record)
    df_score = spark_session.createDataFrame(row_rdd, schema2)
    '''
    inner join two df to make sure the traning data is good
    '''
    df_join = df_str.join(df_score, df_str.structure_id == df_score.score_id)
    df_join.createOrReplaceTempView("table1")
    df = spark_session.sql("SELECT structure_id, casp_No, contact_map, score from table1")
    df.write.jdbc(url='jdbc:%s' % url, table='structures', mode='append',  properties=properties)


if __name__ == "__main__":
    main()
