#! /bin/bash

[ -f ./env.sh ] && source ./env.sh

MODEL="hdfs:///user/david_yam/output/dns_tunnelling/test_RandomForestClassifier_v1.0_20200312-095536.mml"
#MODEL="hdfs:///user/david_yam/DNS_Tunnelling_RandomForestClassifier_v1.0_20191210-100230.mml"
CONFIG_FILEPATH="./dns_tunnelling_config.json"

set -ex

PY_FILES=$(find ./lib -type f -name '*.zip' | paste -sd,)
JAR_FILES=$(find ./lib -type f -name '*jar' | paste -sd,)
"$SPARK_HOME"/bin/spark-submit --master yarn \
    --name DNS_Tunnelling_Scoring \
    --py-files "$PY_FILES" \
    --jars "$JAR_FILES" \
    ./src/dns_tunnelling_scoring.py \
    --config_filepath "$CONFIG_FILEPATH" \
    --model_save_filepath "$MODEL" \
    "$@"
