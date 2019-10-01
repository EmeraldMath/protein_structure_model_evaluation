import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import boto3
import botocore
import io
import numpy as np
from operator import add
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import smart_open
from PIL import Image
from io import BytesIO

def get_a_fold_keys(bucket_name, dir):
    """List files in specific S3 URL"""
    client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,\
                      aws_secret_access_key=AWS_SECRET_KEY)
    keys = []
    response = client.list_objects(Bucket=bucket_name, Prefix=dir)
    for content in response.get('Contents', []):
        i = content.get('Key').split('/')
        if (i[1] != "casp_native_structures"):
            if (len(i) != 5):
                continue
        keys.append((bucket_name, content.get('Key')))
    return keys

def get_all_s3_keys(bucket_name, dir):
    """Get a list of all keys in an S3 bucket."""
    clinet = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,\
                      aws_secret_access_key=AWS_SECRET_KEY)
    keys = []
       
    kwargs = {'Bucket': bucket_name, 'Prefix': dir}
    while True:
        resp = clinet.list_objects_v2(Bucket=bucket_name, Prefix=dir)
        for obj in resp['Contents']:
            keys.append((bucket_name, obj['Key']))

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

#write the contact map to the database
def struct_id(obj):
    str_id = obj[-1]
    return str_id
    
def score_id(obj):
    obj = obj.split('.')
    score_id = obj[-2]
    return score_id

def str_to_record(x):
    bucket_name = x[0]
    obj = x[1]
    args = ("./LIC", x[0], x[1])
    #args = ("./LIC", 'protein-structures', 'test_data/test_structures/T0950/T0950TS164_4')
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
                           env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                           'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
    output = popen.stdout.read()
    pro_contains_resArry = output.decode()
    #print(pro_contains_resArry)
    row = pro_contains_resArry[:-1].splitlines()
    protein = [[float(j) for j in i.split()] for i in pro_contains_resArry[:-1].splitlines()]
    '''
    if (np.shape(np.array(protein))[0] == 0):
        print(obj)
        print("print protein ", protein)
    '''
    img = Image.fromarray(np.array(protein)*10)
    img = img.convert("L")
    with BytesIO() as output:
        img.save(output, "png")
        protein = output.getvalue()
    protein = bytearray(np.array(protein))
    obj = obj.strip().split('/')
    str_id = struct_id(obj)
    protein_record = Row(str_id, obj[-2], protein, obj[-3])
    
    return protein_record


def native_to_record(x):
    bucket_name = x[0]
    obj = x[1]
    args = ("./LIC", x[0], x[1])
    #args = ("./LIC", 'protein-structures', 'test_data/test_structures/T0950/T0950TS164_4')
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
                           env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                           'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
    output = popen.stdout.read()
    pro_contains_resArry = output.decode()
    row = pro_contains_resArry[:-1].splitlines()
    protein = [[float(j) for j in i.split()] for i in pro_contains_resArry[:-1].splitlines()]
     
    img = Image.fromarray(np.array(protein)*10)
    img = img.convert("L")
    with BytesIO() as output:
        img.save(output, "png")
        protein = output.getvalue()
    
    protein = bytearray(np.array(protein))
    obj = obj.strip().split('/')
    protein_record = Row(obj[-1], protein, obj[-2], 100.0)
    
    return protein_record

def score_to_record(x):
    session = boto3.Session(
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
    )
    bucket_name = x[0]
    obj = x[1]
    flag = False
    print('print x', x)
    for line in smart_open.open('s3://'+bucket_name+'/'+obj, transport_params=dict(session=session)):
        if (line.startswith("SUMMARY(GDT)")):
            line = line.strip().split()
            obj = obj.strip().split('/')
            str_id = score_id(obj[-1])
            protein_record = Row(str_id, float(line[6]))
            break
    
    return protein_record

def set_test_dir():
    struct_dir = 'test_data/test_structures/T0950/'
    score_dir = 'test_data/test_scores/T0950/'
    return struct_dir, score_dir

def set_dir():
    struct_dir = 'protein_raw_data/casp_protein_structures/'
    score_dir = 'protein_raw_data/casp_protein_scores/'
    return struct_dir, score_dir

def main():
    bucket_name = 'protein-structures'
    #struct_dir, score_dir = set_test_dir()
    struct_dir, score_dir = set_dir()
    
    schema1 = StructType([
        StructField("candidate_id", StringType(), False),
        StructField("protein_id", StringType(), False),
        StructField("contact_map", BinaryType(), False),
        StructField("candidate_source", StringType(), False)
        ])
    
    schema2 = StructType([
        StructField("score_id", StringType(), False),
        StructField("score", FloatType(), False)
        ])
    
    schema3 = StructType([
        StructField("protein_id", StringType(), False),
        StructField("contact_map", BinaryType(), False),
        StructField("protein_source", StringType(), False),
        StructField("score", FloatType(), False)
    ])

    url ='postgresql://10.0.0.8:5432/structure_evaluation'
    properties = {'user':db_user,'password':db_password,'driver':'org.postgresql.Driver'}
    spark_session = SparkSession.builder.master("spark://ip-10-0-0-12:7077").appName("s3ToLIC").getOrCreate()
    sc = spark_session.sparkContext
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    '''
    extract and computing contact_map distributedly
    '''
    #structs = get_a_fold_keys(bucket_name, struct_dir)
    struct_dir = 'protein_raw_data/casp_protein_structures/casp8/'
    structs = get_all_s3_keys(bucket_name, struct_dir)
    print(len(structs))

    '''
    rdd = sc.parallelize(structs)
    row_rdd = rdd.map(str_to_record)
    df_str = spark_session.createDataFrame(row_rdd, schema1)
    df_str.write.jdbc(url='jdbc:%s' % url, table='structures', mode='append',  properties=properties)
    #df_str.show()
    '''

    '''
    extract scores distributedly
    '''

    '''
    scores = get_a_fold_keys(bucket_name, score_dir)
    rdd = sc.parallelize(scores)
    row_rdd = rdd.map(score_to_record)
    df_score = spark_session.createDataFrame(row_rdd, schema2)
    df_score.write.jdbc(url='jdbc:%s' % url, table='scores', mode='append',  properties=properties)
    '''   
    '''
    inner join two df to make sure the traning data is good. But still keep the raw data
    as df_str and df_score
    '''
    '''
    df_join = df_str.join(df_score, df_str.candidate_id == df_score.score_id)
    df_join.createOrReplaceTempView("table1")
    df = spark_session.sql("SELECT candidate_id, protein_id, contact_map, candidate_source, score from table1")
    df.write.jdbc(url='jdbc:%s' % url, table='candidates', mode='append',  properties=properties)
    df_str.show()
    df_score.show()
    df.show()

    native_dir = "protein_raw_data/casp_native_structures/casp10"
    native = get_a_fold_keys(bucket_name, native_dir)
    rdd = sc.parallelize(native)
    row_rdd = rdd.map(native_to_record)
    df_native = spark_session.createDataFrame(row_rdd, schema3)
    df_native.write.jdbc(url='jdbc:%s' % url, table='natives', mode='append',  properties=properties)
    df_native.show()
    '''

if __name__ == "__main__":
    main()