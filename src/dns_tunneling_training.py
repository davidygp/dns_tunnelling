# # Imports # #
# System Packages
import argparse
import datetime
import logging

# Other Packages
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    # LogisticRegression,
    # DecisionTreeClassifier,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler

# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import split
# Own Functions
from dns_tunneling_features import generate_features
from dns_tunneling_features import clean_str_udf

# # Imports # #


# # Config # #
logger_format = "%(asctime)s, %(levelname)-8s %(filename)s:%(lineno)d - \
                %(message)s"
logger_datefmt = "%Y-%m-%d %H:%M:%S"
logger_level = "INFO"

global_seed = 42

model_numTrees = 50
model_featureSubsetStrategy = "auto"
model_impurity = "entropy"
model_maxDepth = 9

model_name = "RandomForestClassifier_v1.0"
time_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
label = "DnsTunnel"
ans_mapping = {"normal": 0, "tunnel": 1}
# # Config # #


# # Functions # #
def register_udfs(spark):
    spark.udf.registerJavaFunction(
        "top_private_dn", "cscoe.domain.TopPrivateDomainUDF", "string"
    )


# # Functions # #


def main():
    # Logging purposes
    logging.basicConfig(
        format=logger_format, datefmt=logger_datefmt, level=logger_level
    )
    logging.info(
        "Train the dataset from the prepared clean (legit) and malicious \
        examples"
    )

    # Parse the arguments from the shellscript
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_train_filepath",
        help="Please provide the HDFS filepath to the training CSV file",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--model_save_root_filepath",
        help="Please provide the HDFS filepath for where the model should be \
             saved (Note: model type and datetime is added from within the py \
             script)",
        required=True,
    )

    flags = parser.parse_args()
    logging.info("EVENT=These are the flags submitted %s" % (flags))

    input_train_filepath = flags.input_train_filepath
    model_save_root_filepath = flags.model_save_root_filepath

    spark = SparkSession.builder.getOrCreate()
    register_udfs(spark)

    #schema = t.StructType(
    #    [
    #        t.StructField("QNAME", t.StringType(), True),
    #        t.StructField("RDATA", t.StringType(), True),
    #        t.StructField("DnsTunnel", t.StringType(), True),
    #    ]
    #)
    # Read from csv with delimiter ~ and remove trailing commas from DnsTunnel column
    #train_df = spark.read.csv(input_train_filepath, header=True, schema=schema, sep="~")
    train_df = spark.read.csv(input_train_filepath, header=True, sep="~")
    train_df = train_df.cache()
    logging.info("EVENT=%s rows are in the train/test dataset" % (train_df.count()))

    #train_df = train_df.select(split(train_df.a,"~").getItem(0),
    #                          split(train_df.a,"~").getItem(1),
    #                          split(train_df.a,"~").getItem(2))
    #train_df = train_df.toDF("QNAME","DnsTunnel","RDATA")
    #train_df = train_df.select("QNAME","RDATA","DnsTunnel")
    train_df = train_df.withColumn("DnsTunnel",clean_str_udf(train_df.DnsTunnel))
    train_df = train_df.filter("QNAME is not null and RDATA is not null and DnsTunnel is not null")
    train_df = train_df.filter("DnsTunnel == 'tunnel' or DnsTunnel == 'normal'")
    train_df = train_df.selectExpr("*", "top_private_dn(QNAME) as lld")
    features_df = generate_features(train_df)
    features_df = features_df.withColumn(
        "label", f.udf(lambda x: ans_mapping[x], t.IntegerType())(f.col(label))
    )
    features_df = features_df.cache()
    logging.info("EVENT=%s rows were left after generating features" % (features_df.count()))

    train, test = features_df.randomSplit([0.8, 0.2], seed=global_seed)

    # Target Column #

    # Feature Columns #
    feature_cols = [
        "subdomain_length",
        "subdomain_entropy",
        "average_subdomain_length",
        "min_subdomain_length",
        "max_subdomain_length",
        "label_count",
        "subdomain_uppercase_char",
        "subdomain_numeric_char",
        #"subdomain_english_word",
        "average_answer_length",
        "max_answer_length",
        "average_answer_uppercase_char",
        "average_answer_numeric_char",
        "average_answer_entropy",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid='skip')

    # Model(s) and their Parameters #
    # lr = LogisticRegression(regParam=0.1)
    # dt = DecisionTreeClassifier(maxDepth=5, seed=global_seed)
    rfc = RandomForestClassifier(
        numTrees=model_numTrees,
        featureSubsetStrategy=model_featureSubsetStrategy,
        impurity=model_impurity,
        maxDepth=model_maxDepth,
        seed=global_seed,
    )

    # Create/ Combine the Stages #
    stages = []
    stages.append(assembler)
    stages.append(rfc)
    pipe = Pipeline(stages=stages)
    logging.info("EVENT=Pipeline Created")
    # Train the Model #
    model = pipe.fit(train)
    logging.info("EVENT=Model Trained")
    logging.info("EVENT=Model is %s" % (model))
    logging.info("EVENT=Model Stages %s" % (model.stages))

    # make prediction
    pred = model.transform(test)
    
    # For Binary Classification:
    # evaluate. note only 2 metrics are supported out of the box by Spark ML.
    bce = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    au_roc = bce.setMetricName("areaUnderROC").evaluate(pred)
    au_prc = bce.setMetricName("areaUnderPR").evaluate(pred)
    logging.info("METRIC=Area under ROC: %f" % (au_roc))
    logging.info("METRIC=Area under PR: %f" % (au_prc))
 
    # For Multiclass Classification:
    # evaluate. there is no AUROC/ AUPRC for multiclass classification out of
    # the box by Spark ML.
    mce = MulticlassClassificationEvaluator(
        predictionCol="prediction", labelCol="label"
    )
    accuracy = mce.setMetricName("accuracy").evaluate(pred)
    recall = mce.setMetricName("weightedRecall").evaluate(pred)
    precision = mce.setMetricName("weightedPrecision").evaluate(pred)
    f1_score = mce.setMetricName("f1").evaluate(pred)

    logging.info("METRIC=Accuracy is: %f" % (accuracy))
    logging.info("METRIC=Recall is: %f" % (recall))
    logging.info("METRIC=Precision is: %f" % (precision))
    logging.info("METRIC=F1 Score is: %f" % (f1_score))

    logging.info("FEATURE=Feature Importances %s" % (model.stages[-1].featureImportances))
    logging.info("FEATURE=Feature Columns %s" % (feature_cols))

    model_save_filepath = (
        model_save_root_filepath + "_" + model_name + "_" + time_str + ".mml"
    )
    model.write().overwrite().save(model_save_filepath)
    logging.info("EVENT=Saved model to %s" % (model_save_filepath))

    logging.info("EVENT=It's finished!")


if __name__ == "__main__":
    main()
