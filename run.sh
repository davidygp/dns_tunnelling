#! /etc/bash

INPUT_FILEPATH="hdfs:///user/hive/ext_warehouse/cti.db/dns/datestr=20200303/hour=18/part-*"
OUTPUT_FILEPATH="hdfs:///user/david_yam/output/dns_tunnelling/20200303_18_test6_fil/"

#KAFKA_TOPIC="test"
#KAFKA_BROKERS="kfk02:9092,kfk03:9092,kfk05:9092"
#METRIC="metric.json"

SCRIPT="./dns_tunnelling_scoring.sh"

exec "$SCRIPT" \
    --input_filepath "$INPUT_FILEPATH" \
    --output_filepath "$OUTPUT_FILEPATH"
    #--topic "$KAFKA_TOPIC" \
    #--kafka_brokers "$KAFKA_BROKERS" \
    #--metric "$METRIC"
