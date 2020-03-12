# # Imports # #
# System Packages
import math
from collections import Counter

# Other Packages
#import tldextract

import pyspark
import nltk.corpus

from pyspark import SparkFiles
import nltk

#nltk.download("words")
# Own Functions

# # Imports # #

# # Functions # #


def get_subdomain_length(fqdn, tldn):
    # Return length of subdomain
    string = fqdn.replace(tldn + ".", "")
    return len(string)


def get_entropy(fqdn, tldn):
    # Return entropy of subdomain
    string = fqdn.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    p, lns = Counter(string), float(len(string))
    entropy = -sum(count / lns * math.log(count / lns, 2) for count in p.values())
    return entropy


def get_avg_subdomain_length(fqdn, tldn):
    # Return average length of subdomain
    string = fqdn.replace(tldn + ".", "")
    subdomain = string.split(".")
    total_subdomain_length = 0
    if len(subdomain) == 0:
        return 0.0
    else:
        number_of_subdomain = len(subdomain)
        for sd in subdomain:
            total_subdomain_length += len(sd)
        return total_subdomain_length / number_of_subdomain


def get_min_subdomain_length(fqdn, tldn):
    # Return minimum length of subdomain
    string = fqdn.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    subdomain = string.split(".")
    number_of_subdomain = len(subdomain)
    min_subdomain_length = len(subdomain[0])
    for sd in subdomain:
        if min_subdomain_length > len(sd):
            min_subdomain_length = len(sd)
    return min_subdomain_length


def get_max_subdomain_length(fqdn, tldn):
    # Return maximum length of subdomain
    string = fqdn.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    # string = str(string)
    # fqdn = tldextract.extract(string)
    # subdomain = fqdn[0]
    subdomain = string.split(".")
    number_of_subdomain = len(subdomain)
    max_subdomain_length = 0
    for sd in subdomain:
        if max_subdomain_length < len(sd):
            max_subdomain_length = len(sd)
    return max_subdomain_length


def get_label_count(fqdn, tldn):
    # Return number of labels
    string = fqdn.replace(tldn + ".", "")
    return string.count(".")


def get_uppercase_char(fqdn, tldn):
    # Return number of uppercase character over total
    string = fqdn
    string = string.replace(tldn + ".", "")
    if len(string) > 0:
        uppercase_char = sum(1 for c in str(string) if c.isupper())
        return uppercase_char / len(string)
    else:
        return 0.0


def get_numeric_char(fqdn, tldn):
    # Return number of numeric character over total
    string = fqdn
    string = string.replace(tldn + ".", "")
    if len(string) > 0:
        numeric_char = sum(1 for c in str(string) if c.isdigit())
        return numeric_char / len(string)
    else:
        return 0.0


def get_english_word(fqdn, tldn):
    #from pyspark import SparkFiles
    nltk_fp = SparkFiles.get("nltk_data.zip/nltk_data")
    #import nltk
    nltk.data.path.append(nltk_fp)
    # Return the number of english words over total
    string = fqdn
    string = string.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    if len(string) > 0:
        # nltk.download("words")
        english_vocab = set(w.lower() for w in nltk.corpus.words.words())
        all_words = {
            string[i : j + i]
            for j in range(2, len(string))
            for i in range(len(string) - j + 1)
        }
        word_list = list(english_vocab.intersection(all_words))
        if len(word_list) == 0:
            return 0.0
        else:
            longest_word = max(word_list, key=len)
            english_percentage = len(longest_word) / len(string)
            return english_percentage
    else:
        return 0.0


def get_avg_answer_length(array):
    # Return the average length of Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total_length = 0
    for item in array:
        total_length += len(item)
    return total_length / array_size


def get_avg_answer_entropy(array):
    # Return the average entropy of Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total_entropy = 0
    for item in array:
        p, lns = Counter(item), float(len(item))
        total_entropy += -sum(
            count / lns * math.log(count / lns, 2) for count in p.values()
        )
    return total_entropy / array_size


def get_max_answer_length(array):
    # Return the maximum Rdata length
    max_length = 0
    for item in array:
        if len(item) > max_length:
            max_length = len(item)
    return max_length


def get_avg_answer_uppercase_char(array):
    # Return the average number of uppercase character in Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total_uppercase = 0
    for item in array:
        total_uppercase += sum(1 for c in str(item) if c.isupper()) / len(item)
    return total_uppercase / array_size


def get_avg_answer_numeric_char(array):
    # Return the average number of numeric character in Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total_numeric = 0
    for item in array:
        total_numeric += sum(1 for c in str(item) if c.isdigit()) / len(item)
    return total_numeric / array_size


def excluded_rtype_rdata(string):
    excluded_list = [
        "RRSIG",
        "DNSKEY",
        "DS",
        "eNSEC",
        "NSEC3",
        "CDNSKEY",
        "CDS",
        "TSIG",
        "TKEY",
        "v=spf1",
        "docusign=",
        "verification=",
    ]
    if any(s for s in excluded_list if (s in string)):
        return True
    return False

def clean_str(string):
    if isinstance(string,str):
    	return string.replace(",","")
    

# # Functions # #
