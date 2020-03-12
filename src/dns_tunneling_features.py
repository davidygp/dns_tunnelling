# # Imports # #
# System Packages

# Other Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t

# Own Functions
from dns_tunneling_helper_functions import (
    get_subdomain_length,
    get_entropy,
    get_avg_subdomain_length,
    get_min_subdomain_length,
    get_max_subdomain_length,
    get_label_count,
    get_uppercase_char,
    get_numeric_char,
    get_english_word,
    excluded_rtype_rdata,
    get_avg_answer_length,
    get_avg_answer_entropy,
    get_max_answer_length,
    get_avg_answer_uppercase_char,
    get_avg_answer_numeric_char,
    clean_str
)

# # Imports # #

# # Functions # #

get_subdomain_length_udf = f.udf(
    lambda x, y: get_subdomain_length(x, y), t.IntegerType()
)

get_entropy_udf = f.udf(lambda x, y: get_entropy(x, y), t.DoubleType())

get_avg_subdomain_length_udf = f.udf(
    lambda x, y: get_avg_subdomain_length(x, y), t.DoubleType()
)

get_min_subdomain_length_udf = f.udf(
    lambda x, y: get_min_subdomain_length(x, y), t.IntegerType()
)

get_max_subdomain_length_udf = f.udf(
    lambda x, y: get_max_subdomain_length(x, y), t.IntegerType()
)

get_label_count_udf = f.udf(lambda x, y: get_label_count(x, y), t.IntegerType())

get_uppercase_char_udf = f.udf(lambda x, y: get_uppercase_char(x, y), t.DoubleType())

get_numeric_char_udf = f.udf(lambda x, y: get_numeric_char(x, y), t.DoubleType())

get_english_word_udf = f.udf(lambda x, y: get_english_word(x, y), t.DoubleType())

get_avg_answer_length_udf = f.udf(lambda x: get_avg_answer_length(x), t.DoubleType())

get_avg_answer_entropy_udf = f.udf(lambda x: get_avg_answer_entropy(x), t.DoubleType())

get_max_answer_length_udf = f.udf(lambda x: get_max_answer_length(x), t.IntegerType())

get_avg_answer_uppercase_char_udf = f.udf(
    lambda x: get_avg_answer_uppercase_char(x), t.DoubleType()
)

get_avg_answer_numeric_char_udf = f.udf(
    lambda x: get_avg_answer_numeric_char(x), t.DoubleType()
)

get_excluded_rtype_rdata_udf = f.udf(lambda x: excluded_rtype_rdata(x), t.BooleanType())

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
        "subdomain_entropy", get_entropy_udf(output.QNAME, output.lld)
    )
    output = output.withColumn(
        "average_subdomain_length",
        get_avg_subdomain_length_udf(output.QNAME, output.lld),
    )
    output = output.withColumn(
        "min_subdomain_length", get_min_subdomain_length_udf(output.QNAME, output.lld)
    )
    output = output.withColumn(
        "max_subdomain_length", get_max_subdomain_length_udf(output.QNAME, output.lld)
    )
    output = output.withColumn(
        "label_count", get_label_count_udf(output.QNAME, output.lld)
    )
    output = output.withColumn(
        "subdomain_uppercase_char", get_uppercase_char_udf(output.QNAME, output.lld)
    )
    output = output.withColumn(
        "subdomain_numeric_char", get_numeric_char_udf(output.QNAME, output.lld)
    )
    #output = output.withColumn(
    #    "subdomain_english_word", get_english_word_udf(output.QNAME, output.lld)
    #)
    """
    Functions takes in only RDATA 
    """
    output = output.withColumn(
        "average_answer_length", get_avg_answer_length_udf(output.RDATA)
    )
    output = output.withColumn(
        "max_answer_length", get_max_answer_length_udf(output.RDATA)
    )
    output = output.withColumn(
        "average_answer_uppercase_char", get_avg_answer_uppercase_char_udf(output.RDATA)
    )
    output = output.withColumn(
        "average_answer_numeric_char", get_avg_answer_numeric_char_udf(output.RDATA)
    )
    output = output.withColumn(
        "average_answer_entropy", get_avg_answer_entropy_udf(output.RDATA)
    )
    return output


# # Functions # #
