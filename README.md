# bigdata-pipeline-project
# spring 26 amy stafford 

# Flight Data Lifecycle Pipeline (Module 6)

An automated data orchestration pipeline built with Apache Airflow and Google Cloud Platform to manage the end-to-end lifecycle of aviation datasets.

## 🏗️ Architecture (The 4-Stage Lifecycle)
1. **Ingestion**: Raw CSV acquisition into a VM-based ETL staging area.
2. **Validation**: Automated QA to identify "Dirty Data" (Nulls, dummy values) and prevent the "Hidden Data Factory."
3. **Storage/Transformation**: Cleaning of illegal values and migration to Google Cloud Storage (GCS) "Clean" buckets.
4. **Archival & Preservation**: Generation of JSON audit logs in GCS and automated code archival to GitHub.

## 🚀 Technologies Used
- **Orchestration**: Apache Airflow (Python)
- **Cloud Infrastructure**: GCP Compute Engine (VM)
- **Cloud Storage**: GCS (Multi-bucket strategy for raw, clean, and logs)
- **Version Control**: GitHub (Automated via BashOperator)

## 📊 Data Quality Rules
- Removal of dummy departure delays (`9999`).
- Validation of airport codes against a whitelist of major US hubs.
- Null-value filtering for critical carrier fields.
