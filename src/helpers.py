# Databricks notebook source
# MAGIC %md
# MAGIC ## Add Helper Functions

# COMMAND ----------

from typing import List
import tiktoken

def install_apt_get_packages(package_list: List[str]):
    """Install packages to the worker nodes with given package_list.

    Parameters:
        package_list (str): A space-separated list of apt-get packages.
    """
    import subprocess

    num_workers = max(
        1, int(spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers"))
    )
    print(f"number of works: {num_workers}")

    packages_str = " ".join(package_list)
    command = f"sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/* && sudo apt-get clean && sudo apt-get update && sudo apt-get install {packages_str} -y"
    subprocess.check_output(command, shell=True)

    def run_command(iterator):
        for x in iterator:
            yield subprocess.check_output(command, shell=True)

    data = spark.sparkContext.parallelize(range(num_workers), num_workers)
    # Use mapPartitions to run command in each partition (worker)
    output = data.mapPartitions(run_command)
    try:
        output.collect()
        print(f"{package_list} libraries installed")
    except Exception as e:
        print(f"Couldn't install {package_list} on all nodes: {e}")
        raise e

# COMMAND ----------

# optional helper functions, they are not used in the notebooks
def truncate_text_by_tokens(text, max_tokens=5000, encoding_name="cl100k_base"):
    """truncate text by the token count"""
    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(text, disallowed_special=())
    truncated_tokens = tokens[:max_tokens]
    truncated_text = encoding.decode(truncated_tokens)
    return truncated_text

def chunk_text_by_size(text, chunk_size=8000, model="cl100k_base", overlap=100, prompt_token_size=100):
    """chunk the text by the token count"""
    encoding = tiktoken.get_encoding(model)
    tokens = encoding.encode(text)
    
    # Text is already within the acceptable range
    if chunk_size > len(tokens) + prompt_token_size:
        return text  

    chunks = []
    start = 0
    
    while start < len(tokens) - prompt_token_size:
        end = start + chunk_size

        if start > 0:
            start = start - overlap
        
        chunk_tokens = tokens[start:end]
        chunk_text = encoding.decode(chunk_tokens)
        chunks.append(chunk_text)
        start = end
    
    return chunks

def token_count(txt, encoding_name="cl100k_base"):
    """Perform bype-token counts for the given text"""
    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(txt, disallowed_special=())
    return len(tokens)