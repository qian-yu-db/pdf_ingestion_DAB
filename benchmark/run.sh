#!/bin/bash

# Change to the parent directory of the script
cd "$(dirname "$0")/.."

# Your commands to run in the top level folder
echo "Current directory: $(pwd)"
ls -l

node_type_id=("m5d.2xlarge")
num_workers=(20 30 40)
pdf_batches=(100 300 500)

for num_workers in "${num_workers[@]}"; do
  for node_type_id in "${node_type_id[@]}"; do
    echo "Running benchmark for node type: ${node_type_id} and number of workers: ${num_workers}"
    databricks bundle validate -p e2_demo_fieldeng --var="node_type_id=${node_type_id},num_workers=${num_workers},catalog=qyu_test,schema=genai_pdf_workflow"
    databricks bundle deploy -p e2_demo_fieldeng --var="node_type_id=${node_type_id},num_workers=${num_workers},catalog=qyu_test,schema=genai_pdf_workflow"
    for batch in "${pdf_batches[@]}"; do
        notebook_params="volume=pdf_batch_${batch},table_prefix=bm_${batch}_pdfs"
        echo "Running benchmark for batch size: ${batch} PDFs"
        databricks bundle run -t dev -p e2_demo_fieldeng pdf_ingestion_benchmark --notebook-params="$notebook_params"
    done
  done
done
