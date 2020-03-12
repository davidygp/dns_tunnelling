# # Imports # #
# System Packages
import argparse
import json
import logging
import sys
import time

# Other Packages
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

# Own Functions
#from dns_tunnelling_features import generate_features, concat_RDATA
from dns_tunneling_features import generate_features
from alert_info import init, enrich, write_alert_topic

# # Imports # #

# # Config # #
logger_format = "%(asctime)s %(levelname)-8s %(filename)s:%(lineno)d - \
                %(message)s"
logger_datefmt = "%Y-%m-%d %H:%M:%S"
logger_level = "INFO"

# Alert Format Names
l7_application_name = "DNS"
engine_type = "ML"
engine_name = "DNS_TUNNELLING"
alert_type = "TUNNELLING"
alert_name = "TUNNELLING"
threat_name = "-"
telemetry_source = "DNS"
# Exclude the following
"""
EXCLUDED_RTYPE_LIST = [
    "RRSIG",
    "DNSKEY",
    "DS",
    "eNSEC",
    "NSEC3",
    "CDNSKEY",
    "CDS",
    "TSIG",
    "TKEY",
]
EXCLUDED_RDATA_LIST = ["v=spf1", "docusign=", "verification="]

MIN_IP_IP_2LD_COUNT = 100
MIN_IP_IP_2LD_UNIQUE_QNAME_PERC = 0.33
MIN_IP_IP_2LD_TUNNEL_PERC = 0.33
"""
ans_mapping = {"normal": 0.0, "tunnel": 1.0, "tunnel_keepalive": 2.0}
inv_ans_mapping = {v: k for k, v in ans_mapping.items()}
# # Config # #


# # Functions # #
# Note: Helper functions are defined in <sample_model>_helper_functions.py
def register_udfs(spark):
    spark.udf.registerJavaFunction(
        "top_private_dn", "cscoe.domain.TopPrivateDomainUDF", "string"
    )


def filter_dataframe(dataframe, min_ip_ip_2ld_count):
    # Filter in ONLY the responses from a DNS server
    responses_df = dataframe.filter(
        "payload.header.QR == True and \
        payload.header.responseCode == 'NOERROR'"
    )
    # For the QR flag, 0 is for query & 1 is for response, so True is response

    exploded_df = responses_df.select(
        "*", f.explode("payload.questions.name").alias("QNAME")
    )

    w_2ld_df = exploded_df.selectExpr(
        "*",
        "payload.pcap.ts_sec",
        "payload.ip.srcIP as srcIP",
        "payload.ip.destIP as destIP",
        "top_private_dn(QNAME) as 2ld",
    )

    w_2ld_filtered_df = w_2ld_df.filter(~f.col("2ld").endswith("in-addr.arpa"))

    grpby_ip_ip_2ld_cnt_df = w_2ld_filtered_df.groupby(
        ["srcIP", "destIP", "2ld"]
    ).count()

    grpby_ip_ip_2ld_cnt_filtered_df = grpby_ip_ip_2ld_cnt_df.filter(
        (f.col("count") >= min_ip_ip_2ld_count)
    )

    final_filtered_dataframe = w_2ld_df.join(
        grpby_ip_ip_2ld_cnt_filtered_df,
        ["srcIP", "destIP", "2ld"],
        how="inner",
    )

    return final_filtered_dataframe


# Functions # #


def main():
    # Logging purposes
    logging.basicConfig(
        format=logger_format, datefmt=logger_datefmt, level=logger_level
    )
    # Parse the arguments from the shellscript
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_filepath",
        help="Please provide the HDFS filepath to the ... dataset to score",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_filepath",
        help="Please provide the HDFS filepath for the output",
    )
    parser.add_argument(
        "-c",
        "--config_filepath",
        help="Please provide the filepath for the config file",
    )
    parser.add_argument(
        "-m",
        "--model_save_filepath",
        help="Please provide the HDFS filepath to the scoring model",
        required=True,
    )
    parser.add_argument("--kafka_topic", help="output kafka topic")
    parser.add_argument(
        "--kafka_brokers", help="output kafka broker(s) comma separated"
    )
    flags = parser.parse_args()

    logging.info("EVENT=START flags=%s" % (flags))
    start_s = time.time()

    input_filepath = flags.input_filepath
    output_filepath = flags.output_filepath
    config_filepath = flags.config_filepath
    model_save_filepath = flags.model_save_filepath

    kafka_topic = flags.kafka_topic
    kafka_brokers = flags.kafka_brokers

    if any([kafka_topic, kafka_brokers]) and (
        not all([kafka_topic, kafka_brokers])
    ):
        logging.error("Must provide both topic and kafka_brokers or none.")
        sys.exit(2)

    if not any([kafka_topic, output_filepath]):
        logging.error("No output/topic specified!")
        sys.exit(2)

    spark = SparkSession.builder.getOrCreate()
    # Register any JAVA functions that are required here #
    register_udfs(spark)
    init(spark)

    # Extract the config values from the config_fp
    with open(config_filepath, "r") as fp:
        cfg = json.load(fp)

    EXCLUDED_RTYPE_LIST = cfg["EXCLUDED_RTYPE_LIST"]
    EXCLUDED_RDATA_LIST = cfg["EXCLUDED_RDATA_LIST"]
    MIN_IP_IP_2LD_COUNT = cfg["MIN_IP_IP_2LD_COUNT"]
    MIN_IP_IP_2LD_UNIQUE_QNAME_PERC = cfg["MIN_IP_IP_2LD_UNIQUE_QNAME_PERC"]
    MIN_IP_IP_2LD_TUNNEL_PERC = cfg["MIN_IP_IP_2LD_TUNNEL_PERC"]

    # Seems that I can't put this above, it has to be after the SparkSession is
    # created
    EXCLUDED_RTYPE_ARRAY = f.array([f.lit(x) for x in EXCLUDED_RTYPE_LIST])
    EXCLUDED_RDATA_ARRAY = f.array([f.lit(x) for x in EXCLUDED_RDATA_LIST])

    # Read in the dataset from the filepath
    df = spark.read.parquet(input_filepath)

    filtered_df = filter_dataframe(df, MIN_IP_IP_2LD_COUNT)
    filtered_df = filtered_df.withColumn("lld", f.col("2ld"))
    filtered_df = filtered_df.withColumn("RDATA", f.col("payload.answers.rdata"))
 
    #concat_RDATA_df = concat_RDATA(
    #    filtered_df, EXCLUDED_RTYPE_ARRAY, EXCLUDED_RDATA_ARRAY
    #)

    #features_df = generate_features(concat_RDATA_df)
    features_df = generate_features(filtered_df)
    features_df = features_df.fillna(0)

    logging.info("EVENT=Features have been generated")

    # Load the Model #
    model = PipelineModel.load(model_save_filepath)
    logging.info("EVENT=Model loaded %s" % (model))

    # make prediction
    logging.info("EVENT=Scoring on dataset")
    pred = model.transform(features_df)
    pred = pred.cache()
    # pred_cnt = pred.count()
    # logging.info("COUNT=%d rows were scored" % (pred_cnt))

    pred = pred.withColumn(
        "label_str",
        f.udf(lambda x: inv_ans_mapping[x], t.StringType())(
            f.col("prediction")
        ),
    )

    # Get srcIP-destIP-2ld which have any non-normal traffic
    possible_tunnel_ip_ip_2ld_df = (
        pred.filter("label_str != 'normal'")
        .select("srcIP", "destIP", "2ld")
        .distinct()
    )
    # Collect all the data from the srcIP-destIP-2ld which have any non-normal
    # traffic
    possible_tunnel_data_df = pred.join(
        possible_tunnel_ip_ip_2ld_df, ["srcIP", "destIP", "2ld"], how="inner"
    )
    possible_tunnel_data_df = possible_tunnel_data_df.withColumn(
        "normal", f.when(f.col("label_str") == "normal", 1).otherwise(0)
    )
    possible_tunnel_data_df = possible_tunnel_data_df.withColumn(
        "tunnel", f.when(f.col("label_str") != "normal", 1).otherwise(0)
    )

    # Groupby srcIP-destIP-2ld and aggregate the following information:
    # Count of normal traffic
    # Count of tunnel traffic
    # Count of unique QNAME
    # Percentage of tunnel traffic
    # Percentage of unique QNAME
    grpby_pos_tun_ip_ip_2ld_df = possible_tunnel_data_df.groupby(
        ["srcIP", "destIP", "2ld"]
    ).agg(
        #f.max("interval_time").alias("max_interval_time"),
        #f.avg("interval_time").alias("average_interval_time"),
        f.countDistinct("QNAME").alias("unique_QNAME_count"),
        f.count("2ld").alias("2ld_count"),
        f.sum("normal").alias("normal_count"),
        f.sum("tunnel").alias("tunnel_count"),
    )
    grpby_pos_tun_ip_ip_2ld_df = grpby_pos_tun_ip_ip_2ld_df.withColumn(
        "unique_QNAME_perc", f.col("unique_QNAME_count") / f.col("2ld_count")
    )
    grpby_pos_tun_ip_ip_2ld_df = grpby_pos_tun_ip_ip_2ld_df.withColumn(
        "tunnel_perc", f.col("tunnel_count") / f.col("2ld_count")
    )
    grpby_pos_tun_ip_ip_2ld_df = grpby_pos_tun_ip_ip_2ld_df.cache()
    gptii_cnt = grpby_pos_tun_ip_ip_2ld_df.count()
    logging.info(
        "EVENT=COUNT %d ip-ip-2ld(s) are possibly \
                 tunnelling"
        % (gptii_cnt)
    )

    # Filter based on Percentage of tunnel traffic & Percentage of unique QNAME
    tunnel_ip_ip_2ld_df = grpby_pos_tun_ip_ip_2ld_df.filter(
        (f.col("unique_QNAME_perc") >= MIN_IP_IP_2LD_UNIQUE_QNAME_PERC)
        & (f.col("tunnel_perc") >= MIN_IP_IP_2LD_TUNNEL_PERC)
    )
    tunnel_ip_ip_2ld_df = tunnel_ip_ip_2ld_df.cache()
    tii2_cnt = tunnel_ip_ip_2ld_df.count()
    logging.info(
        "EVENT=COUNT %d ip-ip-2ld(s) are detected as tunnelling \
                 based on the additional thresholds"
        % (tii2_cnt)
    )

    # Add in the alert_id for each ip-ip-2ld tuple
    tunnel_ip_ip_2ld_df = tunnel_ip_ip_2ld_df.selectExpr(
        "*", "-1 as alert_id" #"generateId() as alert_id"
    )

    tunnel_traffic_df = possible_tunnel_data_df.join(
        tunnel_ip_ip_2ld_df, ["srcIP", "destIP", "2ld"], how="inner"
    )

    #TODO
    final_output_traffic = tunnel_traffic_df.select("key", "payload")
    final_output_traffic.write.parquet(output_filepath)
    """
    # Formatting the data into the required alert format
    logging.info("EVENT=Formatting the data into the required alert format")
    output_traffic = tunnel_traffic_df
    output_traffic = output_traffic.withColumn(
        "record_first_seen_timestamp_ms",
        f.col("payload.pcap.ts_sec") * 1000
        + f.floor(f.col("payload.pcap.ts_usec") / 1000),
    )
    output_traffic = output_traffic.withColumn(
        "l7_application_name", f.lit(l7_application_name)
    )
    output_traffic = output_traffic.withColumn(
        "src_ip", f.col("payload.ip.srcIP")
    )
    output_traffic = output_traffic.withColumn(
        "src_ip_type",
        f.when(f.col("payload.ip.isIPv6") == "true", "IPv6").otherwise("IPv4"),
    )
    output_traffic = output_traffic.withColumn(
        "src_port", f.col("payload.ip.srcPort")
    )
    output_traffic = output_traffic.withColumn(
        "src_is_malicious", f.lit(False)
    )
    output_traffic = output_traffic.withColumn(
        "dest_ip", f.col("payload.ip.destIP")
    )
    output_traffic = output_traffic.withColumn(
        "dest_ip_type",
        f.when(f.col("payload.ip.isIPv6") == "true", "IPv6").otherwise("IPv4"),
    )
    output_traffic = output_traffic.withColumn(
        "dest_port", f.col("payload.ip.destPort")
    )
    output_traffic = output_traffic.withColumn(
        "dest_is_malicious", f.lit(False)
    )
    # alert_timestamp_sec, src_ip, dest_ip, org_ip_enrich
    output_traffic = enrich(output_traffic)
    output_traffic = output_traffic.withColumn(
        "engine_type", f.lit(engine_type)
    )
    output_traffic = output_traffic.withColumn(
        "engine_name", f.lit(engine_name)
    )
    output_traffic = output_traffic.withColumn("alert_type", f.lit(alert_type))
    output_traffic = output_traffic.withColumn("alert_name", f.lit(alert_name))
    output_traffic = output_traffic.withColumn(
        "threat_name", f.lit(threat_name)
    )
    output_traffic = output_traffic.withColumn("threat_hostname", f.col("2ld"))
    output_traffic = output_traffic.withColumn(
        "confidence", f.floor(f.col("tunnel_perc") * 100).cast(t.IntegerType())
    )
    output_traffic = output_traffic.withColumn("severity", f.lit(-1))
    output_traffic = output_traffic.withColumn(
        "details",
        f.to_json(
            f.struct(
                #f.col("interval_time"),
                #f.col("QNAME_similarity"),
                #f.col("QNAME_length"),
                #f.col("QNAME_entropy"),
                #f.col("QNAME_num_count"),
                #f.col("QNAME_dots_count"),
                #f.col("QNAME_uppercase_count"),
                #f.col("QNAME_max_label_len"),
                #f.col("QNAME_avg_label_len"),
                #f.col("RDATA_similarity"),
                #f.col("RDATA_length"),
                #f.col("RDATA_entropy"),
                #f.col("RDATA_num_count"),
                #f.col("RDATA_uppercase_count"),
                #f.col("label_str"),
                f.col("2ld"),
                #f.col("2ld_count"),
                #f.col("unique_QNAME_count"),
                #f.col("normal_count"),
                #f.col("tunnel_count"),
                #f.col("unique_QNAME_perc"),
                #f.col("tunnel_perc"),
            )
        ),
    )

    output_traffic = output_traffic.withColumn("ioc", f.col("2ld"))
    output_traffic = output_traffic.withColumn("flow_count", f.expr("1L"))
    output_traffic = output_traffic.withColumn(
        "sent_packets", f.expr("cast(1 as decimal(38, 0))")
    )
    output_traffic = output_traffic.withColumn(
        "sent_bytes", f.expr("cast(payload.pcap.wirelen as decimal(38, 0))")
    )
    output_traffic = output_traffic.withColumn(
        "telemetry_source", f.lit(telemetry_source)
    )
    output_traffic = output_traffic.withColumn(
        "telemetry_details", f.to_json(f.struct("payload.*"))
    )

    final_output_traffic = output_traffic.selectExpr(
        "alert_id",
        "alert_timestamp_sec",
        "record_first_seen_timestamp_ms",
        "l7_application_name",
        "org_ip_enrich",
        "org_ip",
        "org_is_malicious",
        "src_ip_enrich",
        "src_ip",
        "src_ip_type",
        "src_port",
        "src_is_malicious",
        "dest_ip_enrich",
        "dest_ip",
        "dest_ip_type",
        "dest_port",
        "dest_is_malicious",
        "engine_type",
        "engine_name",
        "alert_type",
        "alert_name",
        "threat_name",
        "threat_hostname",
        "confidence",
        "severity",
        "details",
        "ioc",
        "flow_count",
        "sent_packets",
        "cast(null as decimal(38,0)) as recv_packets",
        "sent_bytes",
        "cast(null as decimal(38,0)) as recv_bytes",
        "telemetry_source",
        "telemetry_details",
        "date_format(from_unixtime(alert_timestamp_sec), 'yyyyMMdd') as \
        datestr",
        "date_format(from_unixtime(alert_timestamp_sec), 'HH') as hour",
    ).cache()

    if kafka_topic:
        logging.info("EVENT=write_kafka  TOPIC=%s", kafka_topic)
        write_alert_topic(
            df=final_output_traffic,
            topic=kafka_topic,
            brokers=kafka_brokers,
            assign_id=False,
        )

    elif output_filepath:
        logging.info("EVENT=Writing out the data now")
        # Write out the formatted output data
        final_output_traffic.write.parquet(output_filepath)
        logging.info("EVENT=Data written out to %s" % (output_filepath))

    else:
        logging.warn("not saving output anywhere!")

    duration_s = time.time() - start_s
    logging.info(
        "EVENT=END  DURATION_S=%fs, COUNT=%d, flags=%s"
        % (duration_s, final_output_traffic.count(), flags)
    )
    """

if __name__ == "__main__":
    main()
