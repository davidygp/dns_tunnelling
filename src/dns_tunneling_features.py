# # Imports # #
# System Packages

# Other Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import lit

# Own Functions
from dns_tunneling_helper_functions import (
    get_subdomain_length,
    get_subdomain_entropy,
    get_aggregation_subdomain_length,
    get_label_count,
    get_char_percentage,
    get_avg_answer_entropy,
    get_aggregation_answer_length,
    get_answer_percentage,
    clean_str,
)

# # Imports # #

# # Functions # #

get_subdomain_length_udf = f.udf(
    lambda x, y: get_subdomain_length(x, y), t.IntegerType()
)

get_subdomain_entropy_udf = f.udf(
    lambda x, y: get_subdomain_entropy(x, y), t.DoubleType()
)

get_aggregation_subdomain_length_udf = f.udf(
    lambda x, y, z: get_aggregation_subdomain_length(x, y, z), t.DoubleType()
)

get_label_count_udf = f.udf(lambda x, y: get_label_count(x, y), t.IntegerType())

get_char_percentage_udf = f.udf(
    lambda x, y, z: get_char_percentage(x, y, z), t.DoubleType()
)

get_avg_answer_entropy_udf = f.udf(lambda x: get_avg_answer_entropy(x), t.DoubleType())

get_aggregation_answer_length_udf = f.udf(
    lambda x, y: get_aggregation_answer_length(x, y), t.DoubleType()
)

get_answer_percentage_udf = f.udf(
    lambda x, y: get_answer_percentage(x, y), t.DoubleType()
)

clean_str_udf = f.udf(lambda x: clean_str(x), t.StringType())


def generate_features(dataframe):
    """
    Input: dataframe: A Pyspark dataframe with the QNAME(FQDN) & RDATA 
    Output: output: A Pyspark dataframe with features required
    """

    output = dataframe
    """
    Functions takes the FQDN and the top private domain
    """
    output = output.withColumn(
        "subdomain_length", get_subdomain_length_udf(output.QNAME, output.lld)
    )

    output = output.withColumn(
        "subdomain_entropy", get_subdomain_entropy_udf(output.QNAME, output.lld)
    )

    output = output.withColumn(
        "average_subdomain_length",
        get_aggregation_subdomain_length_udf(output.QNAME, output.lld, lit("average")),
    )

    output = output.withColumn(
        "min_subdomain_length",
        get_aggregation_subdomain_length_udf(output.QNAME, output.lld, lit("min")),
    )

    output = output.withColumn(
        "max_subdomain_length",
        get_aggregation_subdomain_length_udf(output.QNAME, output.lld, lit("max")),
    )

    output = output.withColumn(
        "label_count", get_label_count_udf(output.QNAME, output.lld)
    )

    output = output.withColumn(
        "subdomain_uppercase_char",
        get_char_percentage_udf(output.QNAME, output.lld, lit("uppercase")),
    )

    output = output.withColumn(
        "subdomain_numeric_char",
        get_char_percentage_udf(output.QNAME, output.lld, lit("numeric")),
    )

    """
    Functions takes in only RDATA 
    """
    output = output.withColumn(
        "average_answer_length",
        get_aggregation_answer_length_udf(output.RDATA, lit("average")),
    )

    output = output.withColumn(
        "max_answer_length", get_aggregation_answer_length_udf(output.RDATA, lit("max"))
    )
    output = output.withColumn(
        "average_answer_uppercase_char",
        get_answer_percentage_udf(output.RDATA, lit("uppercase")),
    )
    output = output.withColumn(
        "average_answer_numeric_char",
        get_answer_percentage_udf(output.RDATA, lit("numeric")),
    )

    output = output.withColumn(
        "average_answer_entropy", get_avg_answer_entropy_udf(output.RDATA)
    )

    return output


# # Functions # #