import subprocess
from pyspark import SparkContext, SparkConf
import boto3
import botocore
import pandas as pd
import io
import psycopg2
from operator import add
from mySecret import db_host, db, db_user, db_password, table

#def get_all_s3_keys(bucket_name, dir):
def get_all_s3_keys(bucket_name):
    """Get a list of all keys in an S3 bucket."""
    clinet = boto3.client('s3')
    keys = []

    kwargs = {'Bucket': bucket_name}
    while True:
        #resp = clinet.list_objects_v2(Bucket=bucket_name,Prefix=dir)
        resp = clinet.list_objects_v2(Bucket=bucket_name)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

#write the contact map to the database
def send_CM_to_DB(str_id, pro_contains_resArry, csr, tb):
    pro_contains_resArry = pro_contains_resArry[:-1].split('\n')
    cmd = """INSERT INTO structures VALUES (%s, %s)"""
    data = (str_id, (pro_contains_resArry))
    csr.execute(cmd, (data))

def main():
    connection = psycopg2.connect(host = db_host, database = db, user = db_user, password = db_password)
    cursor = connection.cursor()
    #bucket_name = 'protein-structures'
    #dir = 'test_data/test_structures/T0949/'
    bucket_name = 'namehard'
    
    conf = SparkConf().setMaster("spark://ip-10-0-0-12:7077").setAppName("s3ToLIC")
    sc = SparkContext(conf=conf)
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    #keys = get_all_s3_keys(bucket_name, dir)
    keys = get_all_s3_keys(bucket_name)
    pkeys = sc.parallelize(keys)
    #i = 0
    #sc.parallelize(i)
    print(pkeys.take(10))

    for obj in pkeys.toLocalIterator():
        #print(obj)
        '''
        if i > 0:
            break;
        i = i + 1;
        '''
        args = ("./LIC", bucket_name, obj)
        popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = popen.stdout.read()
        pro_contains_resArry = output.decode()
        #for each structure, save all its residue-residue contact map to database
        send_CM_to_DB(obj, pro_contains_resArry, cursor, table)

    connection.commit()
    #to read the first row in the contact map
    #cursor.execute("""SELECT contact_map[1] FROM structures""" )
    #bad idea because of memory issue: print(cursor.fetchall())
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
