## Step-by-Step Setup Guide

1. **GCP Account Setup:** Sign up for a GCP account if you don't have one already.

2. **Google SDK and Terraform Installation:** Install Google Cloud SDK and Terraform on your local machine following the official documentation.

3. **Service Account Creation:** Create a service account with the necessary roles for the project.

4. **Service Account JSON File Generation:** Generate a JSON key file for the service account and keep it secure.

5. **Update Terraform Configuration:** Update the `variables.tf` file in your Terraform project with the path to the service account JSON file.

6. **Run Terraform Plan and Apply for setting up GCS and BigQuery.** 

7. **Create Dataproc Cluster:** Use Terraform to create a standalone Dataproc cluster on GCP.

8. **Google Composer Setup:** Set up Google Composer 2 with Airflow 2.7.3 in your GCP project.

9. **Repository Files Deployment:** Copy the project repository files to a GCS bucket for job execution.

10. **Airflow Variable Configuration:** In the Airflow web server, navigate to the Admin tab and add the required variables for the DAG specified in `Airflow_Dags/chicago_project.py`. These variables may include GCP credentials, bucket paths, etc.

11. **DAG Deployment:** Copy the `chicago_project.py` DAG file to the Airflow DAGs GCS bucket. Ensure that the DAG is configured to run with a start date set to yesterday to trigger automatic execution.

With these steps completed, the Chicago Traffic Crash ETL project will be set up and ready to process and analyze traffic crash data on Google Cloud Platform.
