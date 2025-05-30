#!/bin/bash

# Display help message
show_help() {
  echo "Usage: $0 [OPTIONS] [profile] [project_name]"
  echo
  echo "Run benchmark tests"
  echo
  echo "Options:"
  echo "  -h, --help    Show this help message"
  echo
  echo "Arguments:"
  echo "  profile       Databricks profile (default: DEFAULT)"
  echo "  project_name  Name of the project (default: pdf_ingestion)"
  echo
  echo "Example:"
  echo "  $0 my_profile my_project"
  exit 0
}

# Check for help flags
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help
fi

# Default values
default_profile="DEFAULT"
default_project_name="pdf_ingestion"

# Set arguments with defaults if not provided
profile=${1:-$default_profile}
project_name=${2:-$default_project_name}
benchmark_job="${project_name}_benchmark"

# Print message if using defaults
if [ -z "$1" ]; then
  echo "No profile provided, using default: $default_profile"
fi
if [ -z "$2" ]; then
  echo "No project name provided, using default: $default_project_name"
fi

# This script is to run in the top level folder of a Databricks Asset Bundle
echo "Checking whether the current directory is the top level folder of a Databricks Asset Bundle"
echo "Current directory: $(pwd)"
ls -l

# Add your desired cluster type and worker count here for benchmarking
node_type_ids=("m5d.2xlarge")
num_workers_list=(40)

for num_workers in "${num_workers_list[@]}"; do
  for node_type_id in "${node_type_ids[@]}"; do
    echo "Running benchmark for node type: ${node_type_id} and number of workers: ${num_workers}"
    databricks bundle validate -p ${profile} --var="node_type_id=${node_type_id},num_workers=${num_workers}"
    databricks bundle deploy -p ${profile} --var="node_type_id=${node_type_id},num_workers=${num_workers}"
    databricks bundle run -t dev -p ${profile} ${benchmark_job}
  done
done