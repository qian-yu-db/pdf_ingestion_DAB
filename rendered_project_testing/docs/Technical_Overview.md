# PDF Ingestion Tool - Technical Overview

## Core Architecture

### 1. Structured Streaming Pipeline

#### Bronze Layer
```python
# Autoloader configuration for binary files
df_bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load(input_path)
)

# Write to Delta table
df_bronze.writeStream.trigger(availableNow=True)
    .option("checkpointLocation", bronze_checkpoint_path)
    .table(bronze_table_name)
```

- Uses Autoloader to ingest binary files
- Stores raw content in Delta table
- Maintains file metadata (path, size, etc.)

#### Silver Layer
```python
# Read from bronze table
df_silver = spark.readStream.table(bronze_table_name)

# Process documents
df_silver.writeStream.trigger(availableNow=True)
    .option("checkpointLocation", silver_checkpoint_path)
    .foreachBatch(foreach_batch_function_silver)
    .start()
```

- Reads from bronze Delta table
- Processes documents using configured parser inside forEachBatch
- Handles both small and large files

## Processing Details

### 1. Small File Processing
```python
def process_small_files(df_small):
    """Process files under 2GB using parallel threads"""
    return (
        df_small.withColumn("text", process_document_bytes("content", "path"))
        .drop("content")
        .write.mode("append")
        .saveAsTable(silver_target_table)
    )
```

- Uses Pandas UDF for parallel processing
- Thread pool size based on CPU cores
- Configurable worker count: `min(8, os.cpu_count() * 2)`

### 2. Processing Modes: Salted vs Unsalted

#### Unsalted Processing (Default)
```python
# Process each file individually
df_small.withColumn("text", process_document_bytes("content", "path"))
    .drop("content")
    .write.mode("append")
    .saveAsTable(silver_target_table)
```

- Each file processed independently
- Simpler implementation
- Good for smaller batches
- Less memory overhead

#### Salted Processing
```python
# Group files into batches using random salt
df_small.withColumn("batch_rank", (F.floor(F.rand() * 40) + 1).cast("int"))
    .groupby("batch_rank")
    .agg(F.collect_list("content").alias("content"))
    .withColumn("text", F.explode(process_document_bytes_as_array_type("content", "path")))
    .drop("content")
    .drop("batch_rank")
    .write.mode("append")
    .saveAsTable(silver_target_table)
```

- Groups files into random batches (40 groups by default)
- Uses batch processing UDF
- Better for large numbers of small files
- More efficient resource utilization
- Reduces overhead of individual UDF calls

### 3. Large File Processing
```python
def process_large_file(file_path: str):
    """Process files over 2GB using Databricks Workflows"""
    job_id = get_job_id_by_name(LARGE_FILE_PROCESSING_WORKFLOW_NAME)
    run_response = workspace_utils.get_client().jobs.run_now(
        job_id=job_id,
        notebook_params={
            "file_path": file_path,
            "silver_target_table": silver_target_table,
            "parsed_img_dir": PARSED_IMG_DIR
        }
    )
    return run_response.run_id
```

- Triggers separate Databricks Workflow job
- Uses SDK to submit job asynchronously
- Processes file in dedicated cluster
- Tracks job status and results

## Key Components

### 1. Parser Interface
```python
class BaseParser(ABC):
    @abstractmethod
    def parse_document(self, content: bytes, file_type: FileType) -> str:
        """Parse single document and return extracted text"""
        pass
    
    @abstractmethod
    def parse_document_batch(self, contents: List[bytes], 
                           file_types: List[FileType]) -> List[str]:
        """Parse multiple documents in batch"""
        pass
```

### 2. Unstructured Parser Implementation
```python
class UnstructuredParser(BaseParser):
    def parse_document(self, content: bytes, file_type: FileType) -> str:
        """Process document using unstructured library"""
        elements = self._partition_document(content, file_type)
        return self._process_elements(elements)
```

## Extensible Design

### 1. Parser Factory Pattern
```python
class ParserFactory:
    @staticmethod
    def get_parser(parser_type: str, **kwargs) -> BaseParser:
        """Factory method to create parser instances"""
        if parser_type == "unstructured":
            return UnstructuredParser(**kwargs)
        # Future: Add Databricks AI Parser
        # if parser_type == "databricks_ai":
        #     return DatabricksAIParser(**kwargs)
```

### 2. File Type Support
```python
class FileType(Enum):
    """Supported document formats"""
    PDF = "pdf"
    DOCX = "docx"
    PPTX = "pptx"
```

## Error Handling

### 1. File Type Validation
```python
def get_file_type(file_path: str) -> FileType:
    """Validate and return file type"""
    ext = os.path.splitext(file_path)[1].lower().lstrip('.')
    try:
        return FileType(ext)
    except ValueError:
        supported_formats = ", ".join(f".{ft.value}" for ft in FileType)
        raise ValueError(f"Unsupported file extension: .{ext}")
```

### 2. Processing Errors
```python
@retry_on_failure(max_retries=5, delay=2)
def perform_partition(raw_doc_contents_bytes, file_path):
    """Process document with retry logic"""
    try:
        file_type = get_file_type(file_path)
        return parser.parse_document(raw_doc_contents_bytes, file_type)
    except ValueError as e:
        print(f"Unsupported file type: {e}")
        return f"ERROR: {str(e)}"
    except Exception as e:
        print(f"Processing error: {e}")
        return f"ERROR: {str(e)}"
```

- Retries failed operations (max 5 attempts)
- Handles unsupported file types
- Captures processing errors
- Returns error messages instead of failing

## Configuration Options

### Unstructured Parser Configuration
```python
parser_config = {
    # Enable table structure detection
    "infer_table_structure": True,
    
    # Language support
    "languages": ["eng"],
    
    # Processing strategy
    "strategy": "hi_res",
    
    # Element types to extract
    "extract_image_block_types": ["Table", "Image"],
    
    # Output directory for extracted images
    "extract_image_block_output_dir": PARSED_IMG_DIR,
    
    # Table processing options
    "table_processing": {
        "convert_to_markdown": True,
        "preserve_formatting": True
    }
}
```

### Processing Configuration
```python
processing_config = {
    # Large file threshold (2GB)
    "large_file_threshold": 2000 * 1024 * 1024,
    
    # Thread pool configuration
    "worker_cpu_scale_factor": 2,
    "max_workers": 8,
    
    # Retry configuration
    "max_retries": 5,
    "retry_delay": 2
}
```

## Future Extensibility

### 1. New Parser Integration
- Implement new parser by extending `BaseParser`
- Register with `ParserFactory`
- Configure parser-specific settings

### 2. Additional Document Types
- Add new type to `FileType` enum
- Implement type-specific processing
- Update validation logic

## Memory Management

### 1. Batch Processing with Memory Control
```python
import gc
import psutil
import pandas as pd
from typing import List, Tuple

def process_batch_with_memory_control(
    contents: List[bytes],
    file_paths: List[str],
    memory_threshold: float = 0.8  # 80% memory usage threshold
) -> List[str]:
    """Process a batch of documents with memory management"""
    
    def get_memory_usage() -> float:
        """Get current memory usage as percentage"""
        return psutil.Process().memory_percent()
    
    def clear_memory():
        """Clear memory and run garbage collection"""
        gc.collect()
        if hasattr(pd, 'clear_memory'):
            pd.clear_memory()
    
    results = []
    current_batch = []
    current_paths = []
    
    for content, path in zip(contents, file_paths):
        current_batch.append(content)
        current_paths.append(path)
        
        # Check memory usage before processing batch
        if get_memory_usage() > memory_threshold:
            # Process current batch
            batch_results = process_document_bytes(
                pd.Series(current_batch),
                pd.Series(current_paths)
            )
            results.extend(batch_results)
            
            # Clear memory
            clear_memory()
            
            # Reset batch
            current_batch = []
            current_paths = []
    
    # Process remaining documents
    if current_batch:
        batch_results = process_document_bytes(
            pd.Series(current_batch),
            pd.Series(current_paths)
        )
        results.extend(batch_results)
        clear_memory()
    
    return results

def foreach_batch_function_silver(batch_df, batch_id):
    """Enhanced batch processing with memory management"""
    # Split into small and large files
    df_small = batch_df.filter(F.col("length") <= LARGE_FILE_THRESHOLD)
    df_large = batch_df.filter(F.col("length") > LARGE_FILE_THRESHOLD)
    
    # Process small files with memory control
    if not df_small.isEmpty():
        contents = df_small.select("content").collect()
        paths = df_small.select("path").collect()
        
        results = process_batch_with_memory_control(
            [row.content for row in contents],
            [row.path for row in paths]
        )
        
        # Create result DataFrame
        result_df = spark.createDataFrame(
            [(path, text) for path, text in zip(paths, results)],
            ["path", "text"]
        )
        
        # Write results
        result_df.write.mode("append").saveAsTable(
            job_config.parsed_files_table_name
        )
    
    # Process large files (unchanged)
    process_large_files(df_large)
```

Key Features:
- Monitors memory usage during processing
- Processes documents in smaller batches when memory usage is high
- Clears memory between batches using garbage collection
- Handles both small and large files appropriately
- Maintains data consistency with proper error handling

Memory Management Strategies:
1. **Batch Size Control**: Dynamically adjusts batch size based on memory usage
2. **Garbage Collection**: Explicitly calls GC between batches
3. **Pandas Memory Clearing**: Uses pandas memory management when available
4. **Memory Threshold**: Configurable threshold for batch processing
5. **Error Recovery**: Maintains state between batches for reliability 