#! /bin/bash

[ -f ./env.sh ] && source ./env.sh

export SPARK_HOME="$HOME/spark-2.4.0-bin-hadoop2.6"
export PYSPARK_PYTHON=/opt/anaconda3/bin/python

#INPUT_TRAIN_FILEPATH="/home/defaultuser/spark-2.4.5-bin-hadoop2.7/bin/#training_set.csv"
#MODEL_SAVE_FILEPATH="/home/defaultuser/spark-2.4.5-bin-hadoop2.7/bin/model/"
INPUT_TRAIN_FILEPATH="hdfs:///user/david_yam/input/dns_tunnelling/training_dataset.csv"
MODEL_SAVE_FILEPATH="hdfs:///user/david_yam/output/dns_tunnelling/test"

set -ex
echo "Dns tunneling train"
PY_FILES=$(find ./lib -type f -name '*.zip' | paste -sd,)
JAR_FILES=$(find ./lib -type f -name '*.jar' | paste -sd,)
FILES="hdfs:///user/david_yam/input/nltk_data.zip"
#FILES="./nltk_data"

"$SPARK_HOME/bin/spark-submit" --master yarn \
    --py-files "$PY_FILES" \
    --jars "$JAR_FILES" \
    --archives "$FILES" \
    ./src/dns_tunneling_training.py \
    -i "$INPUT_TRAIN_FILEPATH" \
    -s "$MODEL_SAVE_FILEPATH"
    #"$@"
    #--input_train_filepath $1 \
    #--model_save_root_filepath $2
    

echo "Finished"
