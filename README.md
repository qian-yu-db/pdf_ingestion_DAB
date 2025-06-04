# pdf_ingestion

## Architecture

![Architecture](imgs/unstructured_data_ingestion_arch.png)

This project enable databricks users to ingest large number of unstructured files (e.g. PDF, Docx, PPTx, etc) from a Databricks Unity Catalog volume into a Databricks Delta table. The project is designed to be 
high-performant, scalable and fault-tolerant.

## Spark Optimizations

* multi-threaded OCR based pdf parsing
* Automatic skewed tasks management
   * Users can set file size and page count threshold to bucket large data to seperate async jobs
   * Optional salted options to alleviate skewness in micro-batches
* Fault tolerance

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```
3. Setup you bundle

    ```
    databricks bundle init pdf_ingestion_template
    ```

* Follow the prompts to define your project name, set cloud environment, and set file size/page count threshold
* The databricks asset bundle job configurations will be auto created
* Review the main asset configuration `databricks.yml` for further customization for your workspace
  * Update `Variables` section with your workspace environment details
* Refer to [Databricks Asset Bundle Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)

4. To validate a development copy of this project:

   validate default setting for dev workflow (Note that "dev" is the default target, so the `--target` parameter is optional here.)
    ```
    $ databricks validate deploy --target dev
    ```

   validate with custom variables
   ```
   databricks bundle validate --target dev --var="catalog=test,schema=unstructured_ingestion,volume=pdfs,num_workers=5,table_prefix=test_job,max_concurrent_async_runs=2
   ```


5. To deploy a development copy of this project:

   Deploy the dev workflow to a databricks workspace
    ```
    $ databricks bundle deploy --target dev
    ```

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] pdf_ingestion_job` to your workspace.
    You can find that job by opening your workpace and clicking on *Workflows*.

   deploy with custom variables
   ```
   databricks bundle deploy --target dev --var="catalog=test,schema=unstructured_ingestion,volume=pdfs,num_workers=5,table_prefix=test_job,max_concurrent_async_runs=2
   ```

6. Similarly, to deploy a production copy, type:

   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day (defined in resources/pdf_ingestion_job.yml). The schedule is paused when deploying in development mode (see https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).


7. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

8. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

9. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.


## Roadmap

* Suppot databricks AI parse API
* Memory management
* Map-reduce by page splits option for large files


## Reference:

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)
- [Databricks Asset Bundle Tutorial](https://docs.databricks.com/aws/en/dev-tools/bundles/tutorials)