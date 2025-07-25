# PAQS Query 0 Docker Package for BigQuery

## Configuration

### Full BigQuery Configuration Example

Here's a complete example of a BigQuery configuration file (`dbconfig`):

```json
{
    "src_name": "src_bigquery",
    "src_args": {
        "project": "som-rit-phi-starr-prod",
        "dataset": "pcornet_final_2024_12_09"
    },
    "src_site": {
        "SiteName": "STAN_C2",
        "codeset_dataset": "Alvaro_pcornet",
        "codeset_project": "som-rit-phi-starr-dev",
        "path_to_service_token": "",
        "cdm_case": "lower",
        "can_index": false
    }
}
```

### Key Configuration Parameters

- **src_name**: Specifies "src_bigquery" to use the BigQuery connection
- **src_args**:
  - **project**: The main GCP project containing your PCORnet data
  - **dataset**: The BigQuery dataset containing your PCORnet tables
- **src_site**:
  - **SiteName**: Your site identifier (used in reporting)
  - **codeset_dataset**: The dataset where temporary codeset tables will be created
  - **codeset_project**: The GCP project where codeset tables should be created (can be different from main data project)
  - **path_to_service_token**: Path to service account key file (empty string uses ADC authentication)
  - **cdm_case**: Case format of your table/column names (typically "lower" or "snake_case")
  - **can_index**: Whether to create indexes (set to `false` for BigQuery as it doesn't support traditional indexes)

### Understanding the `codeset_project` Parameter

The `codeset_project` parameter was added to support scenarios where:

1. **Cross-project Operations**: Data may be stored in one project, but temporary tables need to be created in a different project where the user has write permissions
2. **Permission Segregation**: Production data might be in a read-only project, while analysis and temporary tables need a separate project with write access
3. **Resource Management**: Organizations may want to separate analytical workloads from production data storage

When `codeset_project` is specified, the system will:
- Create all codeset tables in the specified project rather than the main data project
- Use fully qualified table names (`project.dataset.table`) in all SQL queries
- Format SQL queries properly for BigQuery's syntax requirements

This parameter is crucial for environments where users have different permission levels across projects, allowing queries to read from a production project but write temporary tables to a development project.

## BigQuery Authentication Options

This PAQS Docker package supports two methods of authentication to Google BigQuery:

### Option 1: Service Account Key File Authentication (Original Method)

This method uses a Service Account Key file (JSON) to authenticate with Google BigQuery.

1. **Configure `dbconfig` file:**
   - Include the `path_to_service_token` parameter specifying the path to the service account key file **inside the container**:
   
   ```json
   {
     "src_name": "src_bigquery",
     "src_site": {
       "SiteName": "Your Site Name",
       "path_to_service_token": "/root/bq_token/application_default_credentials.json",
       "codeset_dataset": "your_dataset_name",
       "cdm_case": "snake_case",
       "can_index": false
     },
     "src_args": {}
   }
   ```

2. **Docker run command:**
   - Mount your service account key file to the container path specified in the `dbconfig`:
   
   ```bash
   docker run -it --rm \
     -v "/path/to/your/service-account-key.json:/root/bq_token/application_default_credentials.json:ro" \
     -v "/path/to/your/dbconfig:/root/dbconfig:ro" \
     -v "/path/on/host/for/results:/app/query/results" \
     paqs_query_0_bigquery:latest
   ```

### Option 2: Application Default Credentials (ADC) Authentication (New Method)

This method uses Google Cloud Application Default Credentials, which is often preferred when running in Google Cloud environments or when service account key files are not available.

1. **Configure `dbconfig` file:**
   - Either **omit** the `path_to_service_token` parameter entirely:
   
   ```json
   {
     "src_name": "src_bigquery",
     "src_site": {
       "SiteName": "Your Site Name",
       "codeset_dataset": "your_dataset_name",
       "cdm_case": "snake_case",
       "can_index": false
     },
     "src_args": {}
   }
   ```
   
   - Or set it to an empty string:
   
   ```json
   {
     "src_name": "src_bigquery",
     "src_site": {
       "SiteName": "Your Site Name",
       "path_to_service_token": "",
       "codeset_dataset": "your_dataset_name",
       "cdm_case": "snake_case",
       "can_index": false
     },
     "src_args": {}
   }
   ```

2. **Docker run command:**
   - Mount your local ADC credentials to the standard ADC location in the container:
   
   ```bash
   docker run -it --rm \
     -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
     -v "/path/to/your/dbconfig:/root/dbconfig:ro" \
     -v "/path/on/host/for/results:/app/query/results" \
     paqs_query_0_bigquery:latest
   ```

3. **Prerequisites for ADC:**
   - On your local machine, run `gcloud auth application-default login` to set up ADC
   - Ensure your ADC has the appropriate permissions for BigQuery access
   - If running in a GCE/GKE environment, the container can use the instance metadata service for ADC automatically

## Troubleshooting Authentication Issues

### Service Account Key Authentication Issues:
- Ensure the key file has the proper BigQuery permissions
- Verify the path in `dbconfig` matches the mounted path in the container
- Check that the key file is a valid JSON and has not been corrupted
- Confirm the key file has not been revoked or expired

### ADC Authentication Issues:
- Run `gcloud auth application-default login` on your local machine
- Ensure your ADC credentials have the necessary BigQuery permissions
- Verify you've mounted the gcloud directory correctly
- If using a GCP service like GCE/GKE, ensure the service account has BigQuery permissions

## Accessing Query Results

After executing a query, the results are stored in the `query/results` directory inside the container. To easily access and review these results, you can:

### Option 1: Mount the Results Directory When Running the Container

```bash
docker run -it --rm \
  -v "/path/to/your/dbconfig:/root/dbconfig:ro" \
  -v "/path/to/auth/credentials:/root/.config/gcloud:ro" \
  -v "/path/on/host/for/results:/app/query/results" \
  paqs_query_0_bigquery:latest
```

### Option 2: Copy Results from a Running Container

If your container is already running:

1. Find your container ID:
   ```bash
   docker ps
   ```

2. Copy the results directory to your host machine:
   ```bash
   docker cp container_id:/app/query/results /path/on/host/for/results
   ```

### Option 3: Execute Bash in the Container and Review Results

To interactively explore results inside the container:

```bash
docker exec -it container_id bash
cd /app/query/results
ls -la
```

### Key Output Files

The following files in the `query/results` directory provide important information:

- **paqs_query_0_report.html**: HTML report with query results and visualizations
- **paqs_query_0.log**: Detailed execution log (useful for troubleshooting)
- **test_results.csv**: Summary of test steps with pass/fail status
- **total_pat_counts.csv**: Patient counts generated by the query
- **signature.csv**: Query signature file

If you encounter errors during query execution, the log file (`paqs_query_0.log`) will contain detailed error messages and is essential for troubleshooting.

## BigQuery SQL Syntax Requirements

This package has been updated to properly handle BigQuery's SQL syntax requirements:

1. **Table References**: When specifying fully qualified table names in BigQuery, the code now uses the standard format:
   ```sql
   SELECT * FROM `project.dataset.table` AS alias
   ```
   
   Instead of the previously problematic format:
   ```sql
   SELECT * FROM (`project.dataset.table`) `alias`
   ```

2. **Codeset Loading**: The system automatically formats SQL correctly when loading codesets, avoiding syntax errors that previously occurred in steps like:
   - Loading codeset CSV files as temp tables
   - Joining codesets with CDM tables

If you encounter SQL syntax errors related to table references, ensure you're using the latest version of this package which includes the BigQuery syntax fixes.