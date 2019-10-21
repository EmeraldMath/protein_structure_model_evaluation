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
import time
import datetime

def genDataFrames(spark_session, excellent, good, not_good, bad):
    excellent_df = spark_session.read.format("image").load(excellent).withColumn("label", lit(4).cast(IntegerType()))
    good_df = spark_session.read.format("image").load(good).withColumn("label", lit(3).cast(IntegerType()))
    not_good_df = spark_session.read.format("image").load(not_good).withColumn("label", lit(2).cast(IntegerType()))
    bad_df = spark_session.read.format("image").load(bad).withColumn("label", lit(1).cast(IntegerType()))
    
    train_set = excellent_df.unionAll(good_df).unionAll(not_good_df).unionAll(bad_df)

    return train_set


def Pretrain_Model(train_df, max_iter, reg_param, elastic_net_param):
    
    featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
    lr = LogisticRegression(maxIter=max_iter, 
                            regParam=reg_param,
                            elasticNetParam=elastic_net_param, 
                            labelCol="label")
    p = Pipeline(stages=[featurizer, lr])
    model = p.fit(train_df)
    model.stages[1].write().overwrite().save('s3a://trainingmodel/lr')

    print("Coefficients: \n" + str(model.coefficientMatrix))
    print("Intercept: " + str(model.interceptVector))

    trainingSummary = model.summary

    # Obtain the objective per iteration
    objectiveHistory = trainingSummary.objectiveHistory
    print("objectiveHistory:")
    for objective in objectiveHistory:
        print(objective)

    # for multiclass, we can inspect metrics on a per-label basis
    print("False positive rate by label:")
    for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("True positive rate by label:")
    for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("Precision by label:")
    for i, prec in enumerate(trainingSummary.precisionByLabel):
        print("label %d: %s" % (i, prec))

    print("Recall by label:")
    for i, rec in enumerate(trainingSummary.recallByLabel):
        print("label %d: %s" % (i, rec))

    print("F-measure by label:")
    for i, f in enumerate(trainingSummary.fMeasureByLabel()):
        print("label %d: %s" % (i, f))

    accuracy = trainingSummary.accuracy
    falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    truePositiveRate = trainingSummary.weightedTruePositiveRate
    fMeasure = trainingSummary.weightedFMeasure()
    precision = trainingSummary.weightedPrecision
    recall = trainingSummary.weightedRecall
    print("Training Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
          % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))
    
    return model


def predictWithModel(test_df, model):

    df = model.transform(test_df)
    
    return(df)


def validate(df):
    predictionAndLabels = df.select("score", "label")
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    
    return(evaluator.evaluate(predictionAndLabels))

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
    
    excellent = 's3a://excellentimage/'
    good = 's3a://mygoodimage/'
    not_good = 's3a://notgoodimage/'
    bad = 's3a://badimage/'
    train_df = genDataFrames(spark_session, excellent, good, not_good, bad)
    model = Pretrain_Model(train_df, 20, 0.05, 0.3)

if __name__ == "__main__":
    main()