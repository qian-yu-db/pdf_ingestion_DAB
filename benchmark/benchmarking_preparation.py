# Databricks notebook source
# MAGIC %pip install databricks-sdk -U -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text(name="catalog", label="catalog", defaultValue="qyu_test")
dbutils.widgets.text(name="schema", label="schema", defaultValue="genai_pdf_workflow")
dbutils.widgets.text(name="volume", label="catalog", defaultValue="pdf_batch")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Create a few different input file batches
# MAGIC
# MAGIC * 100 - 500

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import re

w = WorkspaceClient()
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

files = w.dbfs.list(volume_path)
files = list(files)

# COMMAND ----------

import random
from databricks.sdk.service.catalog import VolumeType

pdf_files = [f for f in files if f.path.endswith('.pdf')]

for size in [100, 200, 300, 400]:
    random_selection = random.sample(pdf_files, size)
    new_volume_name = f"pdf_batch_{size}"
    # List all volumes and check if the new volume name exists
    volumes_list = w.volumes.list(catalog_name=catalog, schema_name=schema)
    volume_names = [volume.name for volume in volumes_list]
    if new_volume_name not in volume_names:
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=new_volume_name,
            volume_type=VolumeType.MANAGED
        )
    new_volume_path = f"/Volumes/{catalog}/{schema}/{new_volume_name}"
    for file in random_selection:
        file_name = file.path.split("/")[-1]
        dbutils.fs.cp(file.path, f"{new_volume_path}/{file_name}")