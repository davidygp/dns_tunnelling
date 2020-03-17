# # Imports # #
# System Packages
import math
from collections import Counter

# Other Packages

import pyspark
import nltk.corpus
import numpy as np
from pyspark import SparkFiles

# Own Functions

# # Imports # #

# # Functions # #


def get_entropy(string: str) -> float:
    # Return entropy of string
    p, lns = Counter(string), float(len(string))
    entropy = -sum(count / lns * math.log(count / lns, 2) for count in p.values())
    return entropy


def get_subdomain_length(fqdn: str, tldn: str) -> int:
    # Return length of subdomain
    string = fqdn.replace(tldn + ".", "")
    return len(string)


def get_subdomain_entropy(fqdn: str, tldn: str) -> float:
    # Return entropy of subdomain
    string = fqdn.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    return get_entropy(string)


def get_aggregation_subdomain_length(fqdn: str, tldn: str, option: str) -> float:
    # Return minimum length of subdomain
    string = fqdn.replace(tldn + ".", "")
    if len(string) == 0:
        return 0.0
    subdomain = string.split(".")
    subdomain_length = [len(i) for i in subdomain]
    if option == "min":
        return np.float64(np.min(subdomain_length)).item()
    elif option == "max":
        return np.float64(np.max(subdomain_length)).item()
    elif option == "average":
        return np.float64(np.mean(subdomain_length)).item()
    else:
        print("Invalid option.")
        return 0.0


def get_label_count(fqdn: str, tldn: str) -> int:
    # Return number of labels
    string = fqdn.replace(tldn + ".", "")
    return string.count(".")


def get_char_percentage(fqdn: str, tldn: str, option: str) -> float:
    # Gets the percentage of uppercase or numeric characters
    fqdn = fqdn.replace(tldn + ".", "")
    if len(fqdn) > 0:
        if option == "uppercase":
            uppercase_char = sum(1 for c in str(fqdn) if c.isupper())
            return uppercase_char / len(fqdn)
        elif option == "numeric":
            numeric_char = sum(1 for c in str(fqdn) if c.isdigit())
            return numeric_char / len(fqdn)
        else:
            print("Invalid option.")
            return 0.0
    else:
        return 0.0


def get_avg_answer_entropy(array: list) -> float:
    # Return the average entropy of Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total_entropy = [get_entropy(item) for item in array]
    return np.float64(np.sum(total_entropy)).item() / array_size


def get_aggregation_answer_length(array: list, option: str) -> float:
    # Return the average or max length of Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    answer_length = [len(i) for i in array]
    if option == "max":
        return np.float64(np.max(answer_length)).item()
    elif option == "average":
        return np.float64(np.mean(answer_length)).item()
    else:
        print("Invalid option.")
        return 0.0


def get_answer_percentage(array: list, option: str) -> float:
    # Return the average number of uppercase character in Rdata
    array_size = len(array)
    if array_size == 0:
        return 0.0
    total = 0
    if option == "uppercase":
        percentage_array = [
            (sum(1 for c in str(item) if c.isupper()) / len(item)) for item in array
        ]
        return np.float64(np.mean(percentage_array)).item()
    elif option == "numeric":
        percentage_array = [
            (sum(1 for c in str(item) if c.isdigit()) / len(item)) for item in array
        ]
        return np.float64(np.mean(percentage_array)).item()
    else:
        print("Invalid option.")
        return 0.0


def clean_str(string: str) -> str:
    if isinstance(string, str):
        return string.replace(",", "")


# # Functions # #