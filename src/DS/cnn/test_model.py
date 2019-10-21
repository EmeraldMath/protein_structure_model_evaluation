from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from sparkdl import DeepImageFeaturizer
from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY

def genDataFrames(spark_session, excel13, good13, ng13, bad13):
    excel13_df = spark_session.read.format("image").load(excel13).withColumn("label", lit(4).cast(IntegerType()))
    good13_df = spark_session.read.format("image").load(good13).withColumn("label", lit(3).cast(IntegerType()))
    not_good13_df = spark_session.read.format("image").load(not_good13).withColumn("label", lit(2).cast(IntegerType()))
    bad13_df = spark_session.read.format("image").load(bad13).withColumn("label", lit(1).cast(IntegerType()))
    test_set = excel13_df.unionAll(good13_df).unionAll(not_good13_df).unionAll(bad13_df)

    return test_set

def load_model():
    lr_model = LogisticRegressionModel.load('s3a://trainingmodel/lr')
    featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
    p_test = PipelineModel(stages=[featurizer, lr_model])

    return p_test

def toDB(x):
    image = x['image'][0]
    name = image.split('/')
    name = name[-1]
    score = x['label']
    return Row(name, score)


def write_db(test_df):
    output = test_df.rdd.map(toDB).toDF()
    output = output.withColumnRenamed("_1","candidate_id").withColumnRenamed("_2","prediction")
    url ='postgresql://10.0.0.8:5432/structure_evaluation'
    properties = {'user':db_user,'password':db_password,'driver':'org.postgresql.Driver'}
    output.write.jdbc(url='jdbc:%s' % url, table='prediction', mode='overwrite',  properties=properties)


def main():
    spark_session = SparkSession.builder.getOrCreate()
    sc = spark_session.sparkContext
    #sc = SparkContext(conf=conf).setLogLevel("ERROR")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)   
    
    excel13 = 's3a://casp13excel/'
    good13 = 's3a://casp13good/'
    not_good13 = 's3a://casp13notgood/'
    bad13 = 's3a://casp13bad/'
    
    test_set = genDataFrames(spark_session, excel13, good13, ng13, bad13)
    model = load_model()
    predictions = model.transform(test_set)
    write_db(predictions)

if __name__ == "__main__":
    main()