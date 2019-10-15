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

def uploadImage(row):
    '''
    score = df.select('score').collect()['score']  will return many rows of scores, for example"
    Row(score=14.180999755859375), Row(score=12.281000137329102), Row(score=8.772000312805176) ...
    '''
    as_bytes = row['contact_map']
    score = row['score']
    str_id = row['candidate_id']
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

    client_s3 = boto3.client('s3')
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

    '''
    from image to new bytearray (byte array with magic bytes)
    and read image from memory
    with BytesIO() as output:
        img.save(output, "png")
        protein = output.getvalue()
    img = Image.open(BytesIO(img_set))
    '''

def main():   
    url ='postgresql://10.0.0.8:5432/structure_evaluation'
    properties = {'user':db_user,'password':db_password,'driver':'org.postgresql.Driver'}
    spark_session = SparkSession.builder.getOrCreate()
    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)

    schema = StructType([
        StructField("candidate_id", StringType(), False),
        StructField("protein_id", StringType(), False),
        StructField("contact_map", BinaryType(), False),
        StructField("candidate_source", StringType(), False),
        StructField("score", FloatType(), False),
        StructField("str_time", TimestampType(), False),
        StructField("score_time", TimestampType(), False),
        StructField("join_time", TimestampType(), False),
    ])
    '''
    for traning set, table = 'casp8', for test set, table = 'casp*'
    '''
    df = spark_session.read.jdbc('jdbc:%s' % url, table = 'casp8', properties = properties)
    
    df.foreach(uploadImage)


if __name__ == "__main__":
    main()