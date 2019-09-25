import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import boto3
import botocore
import pandas as pd
import io
import psycopg2
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
def send_CM_to_DB(str_id, pro_contains_resArry, csr):
    pro_contains_resArry = pro_contains_resArry[:-1].split('\n')
    cmd = """INSERT INTO structures VALUES (%s, %s)"""
    data = (str_id, (pro_contains_resArry))
    csr.execute(cmd, (data))

def send_score_to_DB(str_id, score, csr):
    #print(str_id)
    cmd = """UPDATE structures SET score = %s WHERE structure_id = %s """
    data = (score, str_id)
    csr.execute(cmd, data)

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

def insert_struct(rdd):
    connection = psycopg2.connect(host = db_host, database = db, user = db_user, password = db_password)
    cursor = connection.cursor()
    #_list = []
    for bucket_name, obj in rdd:
        args = ("./LIC", "protein-structures", "test_data/test_structures/T0950/T0950TS470_2")
        popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={'AWS_ACCESS_KEY_ID':AWS_ACCESS_KEY,\
                               'AWS_SECRET_ACCESS_KEY':AWS_SECRET_KEY})
        output = popen.stdout.read()
        pro_contains_resArry = output.decode()
        #for each structure, save all its residue-residue contact map to database
        obj = obj.strip().split('/')
        str_id = test_struct_id(obj)
        send_CM_to_DB(str_id, pro_contains_resArry, cursor)

    connection.commit()
    cursor.close()
    connection.close()

def insert_score(rdd):
    #print("enter insert_score(rdd)")
    connection = psycopg2.connect(host = db_host, database = db, user = db_user, password = db_password)
    cursor = connection.cursor()
    #print("database connected !")
    session = boto3.Session(
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
    )
    for bucket_name, obj in rdd:
        flag = False
        for line in smart_open.open('s3://'+bucket_name+'/'+obj, transport_params=dict(session=session)):
            #print("s3 file opened !")
            if (line.startswith("SUMMARY(GDT)")):
                line = line.strip().split()
                str_id = test_score_id(obj)
                #print("before send_score_to_DB")
                send_score_to_DB(str_id, float(line[6]), cursor)
                #print("after send_score_to_DB")
                break
    connection.commit()
    cursor.close()
    connection.close()


def main():
    bucket_name = 'protein-structures'
    struct_dir = 'test_data/test_structures/T0950/'
    score_dir = 'test_data/test_scores/T0950/'

    #conf = SparkConf().setMaster("spark://ip-10-0-0-12:7077").setAppName("s3ToLIC")
    #sc = SparkContext(conf=conf)
    spark_session =SparkSession.builder.master("spark://ip-10-0-0-12:7077").appName("s3ToLIC").getOrCreate()
    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    structs = get_a_fold_keys(bucket_name, struct_dir)
    sc.parallelize(structs).foreachPartition(insert_struct)
    #UPDATE should happen AFTER INSERT IS COMPLETED! spark is deadlock
    scores = get_a_fold_keys(bucket_name, score_dir)
    sc.parallelize(scores).foreachPartition(insert_score)




if __name__ == "__main__":
    main()
