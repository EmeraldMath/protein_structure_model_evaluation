import subprocess
#import pyspark
import boto3
import botocore
import pandas as pd
import io
import psycopg2


#write the contact map to the database
def send_CM_to_DB(str_id, pro_contains_resArry, csr, tb):
    pro_contains_resArry = pro_contains_resArry[:-1].split('\n')
    cmd = """INSERT INTO structures VALUES (%s, %s)"""
    data = (str_id, (pro_contains_resArry))
    csr.execute(cmd, (data))

def main():
    table = 'structures'
    connection = psycopg2.connect(host = ip, database = db, user = usr, password = pw)
    cursor = connection.cursor()

    #data processing
    bucket_name = 'protein-structures'
    dir = 'test_data/test_structures/T0949/'
    
    clinet = boto3.client('s3')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    i = 0
    resp = clinet.list_objects_v2(Bucket=bucket_name,Prefix=dir)

    for obj in resp['Contents']:
        print(obj['Key'])
        '''
        if i > 0:
            break;
        i = i + 1;
        '''
        args = ("./LIC", bucket_name, obj['Key'])
        popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = popen.stdout.read()
        pro_contains_resArry = output.decode()
        #for each structure, save all its residue-residue contact map to database
        send_CM_to_DB(obj['Key'], pro_contains_resArry, cursor, table)

    connection.commit()
    #to read the first row in the contact map
    #cursor.execute("""SELECT contact_map[1] FROM structures""" )
    #bad idea because of memory issue: print(cursor.fetchall())
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
