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
- Processes documents using configured parser
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
- Processes files directly in memory
- Thread pool size based on CPU cores
- Configurable worker count: `min(8, os.cpu_count() * 2)`

### 2. Large File Processing
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

## Best Practices

1. **File Preparation**
   - Use modern formats
   - Validate file integrity
   - Consider size thresholds

2. **Processing**
   - Monitor memory usage
   - Track processing errors
   - Configure appropriate workers

3. **Extensibility**
   - Follow interface contracts
   - Implement proper error handling
   - Maintain backward compatibility 