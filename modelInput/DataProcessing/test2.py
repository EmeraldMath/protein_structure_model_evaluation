import subprocess
#import pyspark
import boto3
import botocore
import pandas as pd
import io
import psycopg2

'''
#connect database and create a table
def connect_and_create_a_table(ip, db, usr, pw)
    cmd = (
        """
        CREATE TABLE structures(
            structure_id STRING PRIMARY KEY,
            contact_map  DOUBLE ARRAY[][]
        )
        """
    )
    connection = psycopg2.connect(host = ip, database = db, user = usr, password = pw)
    cursor = connection.cursor(cmd)
    return [cursor, table]


#write the contact map to the database
def send_CM_to_DB(str_id, protein, csr, tb):
    contact_arry = (lambda x: x.strip().split())(for x in y)
    contact_arry(for res_array in protein)
    contact_surf = (lambda surf: float[surf])(for surf in res)
    contact_surf(for i in res_array)
    cmd = (
        """
        INSERT INTO %s
        VALUES ()
        """,
        (AsIs(tb), AsIs(myStr.strip().split()))
    )
'''
#data processing
bucket_name = 'protein-structures'
dir = 'test_data/test_structures/T0949/'

clinet = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
i = 0
resp = clinet.list_objects_v2(Bucket=bucket_name,Prefix=dir)

for obj in resp['Contents']:
    #print(obj['Key'])
    if i > 0:
        break;
    i = i + 1;

    args = ("./LIC", bucket_name, obj['Key'])
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#    popen.wait()
    output = popen.stdout.read()
    myStr = output.decode()
    myStr = myStr[:-1].split('\n')
    #print(len(myStr))
    print(float(myStr.strip().split()))
    #for each structure, save all its residue-residue contact map to database
'''
    [cursor, table] = connect_and_create_a_table(ip, db, usr, pw)
    send_CM_to_DB(obj['Key'], myStr, cursor, table)
'''
