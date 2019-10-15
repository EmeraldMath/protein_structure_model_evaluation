from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import numpy as np
from PIL import Image
from io import BytesIO
import boto3
import psycopg2
import pandas as pd

def uploadImage(row):
    
    as_bytes = row['contact_map']
    score = row['score']
    try:
        str_id = row['candidate_id']
    except:
        str_id = row['protein_id']
    '''
    from bytearray to np.array
    '''
    tmp = np.frombuffer(as_bytes, np.float)
    lenth = int(np.sqrt(len(tmp)))
    protein = tmp.reshape((lenth, lenth))
    '''
    from np.array to image
    '''
    img = Image.fromarray(np.array(protein)*10)
    img = img.convert("L")
    in_mem_file = BytesIO()
    img.save(in_mem_file, format="png")
    in_mem_file.seek(0)
    
    client_s3 = boto3.client('s3',\
        aws_access_key_id=AWS_ACCESS_KEY, \
        aws_secret_access_key=AWS_SECRET_KEY,\
    )
    
    if (score <= 25 and score > -0.1):
        client_s3.upload_fileobj(
            in_mem_file,
            'excellentimage',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 50 and score > 25):
        client_s3.upload_fileobj(
            in_mem_file,
            'mygoodimage',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 75 and score > 50):
        client_s3.upload_fileobj(
            in_mem_file,
            'notgoodimage',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 100 and score > 75):
        client_s3.upload_fileobj(
            in_mem_file,
            'badimage',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )

def uploadtestImage(row):
    as_bytes = row['contact_map']
    score = row['score']
    try:
        str_id = row['candidate_id']
    except:
        str_id = row['protein_id']
    '''
    from bytearray to np.array
    '''
    tmp = np.frombuffer(as_bytes, np.float)
    lenth = int(np.sqrt(len(tmp)))
    protein = tmp.reshape((lenth, lenth))
    '''
    from np.array to image
    '''
    img = Image.fromarray(np.array(protein)*10)
    img = img.convert("L")
    in_mem_file = BytesIO()
    img.save(in_mem_file, format="png")
    in_mem_file.seek(0)
    
    client_s3 = boto3.client('s3',\
        aws_access_key_id=AWS_ACCESS_KEY, \
        aws_secret_access_key=AWS_SECRET_KEY,\
    )
    
    if (score <= 25 and score > -0.1):
        client_s3.upload_fileobj(
            in_mem_file,
            'casp13excel',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 50 and score > 25):
        client_s3.upload_fileobj(
            in_mem_file,
            'casp13good',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 75 and score > 50):
        client_s3.upload_fileobj(
            in_mem_file,
            'casp13notgood',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )
    elif (score <= 100 and score > 75):
        client_s3.upload_fileobj(
            in_mem_file,
            'casp13bad',
            str_id,
            ExtraArgs = {
                'ACL': 'public-read'
            }
        )

        
def main():   
    url ='jdbc:postgresql://10.0.0.8:5432/structure_evaluation'
    spark_session = SparkSession.builder.getOrCreate()
    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    
    #upload images for training data
    for name in ["'T038'", "'T039%'", "'T040%'", "'T041%'", "'T042%'", "'T043%'",  "'T044%'", "'T046%'","'T047%'", "'T048%'", "'T049%'",\
         "'T050%'", "'T051%'", "'T055%'", "'T056%'", "'T057%'","'T058%'"]:
        query = "select * from candidates where candidate_id like {}".format(name)
        df = spark_session.read.format("jdbc").option("url", url).option("driver", 'org.postgresql.Driver')\
            .option("user", db_user).option("password", db_password).option("query", query).load()
        df.foreach(uploadImage)
    
    #upload images for test data
    query = "select * from candidates where candidate_id like 'T095%'"
    df = spark_session.read.format("jdbc").option("url", url).option("driver", 'org.postgresql.Driver')\
        .option("user", db_user).option("password", db_password).option("query", query).load() 
    df.foreach(uploadtestImage)


if __name__ == "__main__":
    main()