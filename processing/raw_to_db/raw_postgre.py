import subprocess
from pyspark import SparkContext, SparkConf
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
        keys.append(content.get('Key'))
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
            keys.append(obj['Key'])

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
    score_id = obj[-1].split('.')
    score_id = score_id[-2]
    return score_id

def score_id(obj):
    score_id = obj[-3] + "/" + obj[-1]
    return score_id

def main():
    connection = psycopg2.connect(host = db_host, database = db, user = db_user, password = db_password)
    cursor = connection.cursor()
    bucket_name = 'protein-structures'
    struct_dir = 'test_data/test_structures/T0950/'
    score_dir = 'test_data/test_scores/T0950/'

    conf = SparkConf().setMaster("spark://ip-10-0-0-12:7077").setAppName("s3ToLIC")
    sc = SparkContext(conf=conf)
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    #keys = get_all_s3_keys(bucket_name)
    structs = get_a_fold_keys(bucket_name, struct_dir)
    #print(type(structs))
    pstructs = sc.parallelize(structs)
    scores = get_a_fold_keys(bucket_name, score_dir)
    pscores = sc.parallelize(scores)
    #print(pscores.take(5)) 

    for obj in pstructs.toLocalIterator():
        args = ("./LIC", bucket_name, obj)
        popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = popen.stdout.read()
        pro_contains_resArry = output.decode()
        #for each structure, save all its residue-residue contact map to database
        obj = obj.strip().split('/')
        str_id = test_struct_id(obj)
        send_CM_to_DB(str_id, pro_contains_resArry, cursor)
        #break


    for obj in pscores.toLocalIterator():
        flag = False
        for line in smart_open.open('s3://'+bucket_name+'/'+obj):
            if (line.startswith("SUMMARY(GDT)")):
                line = line.strip().split()
                obj = obj.strip().split('/')
                str_id = test_score_id(obj)
                send_score_to_DB(str_id, float(line[6]), cursor)
                break



    connection.commit()
    #to read the first row in the contact map
    #cursor.execute("""SELECT contact_map[1] FROM structures""" )
    #bad idea because of memory issue: print(cursor.fetchall())
    cursor.close()
    connection.close()

    #s3 = boto3.resource('s3')
    #bucket = s3.Bucket(bucket_name)



if __name__ == "__main__":
    main()
