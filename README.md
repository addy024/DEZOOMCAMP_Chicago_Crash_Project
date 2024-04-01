# Dezoomcamp Project - Chicago Crash Data Report

![Project_Workflow](https://github.com/addy024/DEZOOMCAMP_Chicago_Crash_Project/blob/main/Workflow.png)

## Overview:

The Chicago Traffic Crash ETL (Extract, Transform, Load) project aims to process and analyze data related to traffic crashes in Chicago. The project extracts raw data from various sources, transforms it into a structured format, and loads it into a data warehouse for analysis. By leveraging Spark for data processing and analysis, the project provides valuable insights into traffic crash trends, contributing to informed decision-making and improved road safety measures.

## Data Sources:

Crash Data: Raw crash data containing information about crash records, including crash date, location, weather conditions, and crash severity.
Vehicle Data: Information about vehicles involved in crashes, such as vehicle type, condition, and damage.
People Data: Data about individuals involved in crashes, including demographic information, injuries, and actions taken.

## ETL Process:
Extraction: Raw data is extracted from various sources, including CSV files and Parquet files stored in Google Cloud Storage (GCS).
Transformation: The extracted data is transformed using Apache Spark. This involves cleaning, filtering, aggregating, and joining datasets to prepare them for analysis. For example, grouping crash data by hour to analyze crash trends over time, or identifying the most prevalent weather conditions for each type of crash.
Loading: The transformed data is loaded into a data warehouse or storage system, such as another Parquet file or a database, enabling easy access and analysis by stakeholders.

## Key Features:
Data Cleaning: Handling Schema Fix, removing duplicates, and ensuring data consistency.
Data Aggregation: Aggregating data to generate summary statistics, such as crash counts by hour or crash type.
Data Joining: Combining multiple datasets (e.g., crash data, vehicle data, people data) to enrich the analysis and gain comprehensive insights.
Data Visualization: Visualizing the analyzed data using Looker to create charts, graphs, and dashboards for easier interpretation.
Automated Pipeline: Implementing an automated ETL pipeline to periodically fetch, process, and load new data, ensuring the analysis is up-to-date using Airflow (Google Composer 2).

## Project Deliverables:
ETL Script: Python script for performing the ETL process using Apache Spark.
Data Lake: Storage Layer for various ETL process using Google Cloud Storage
Data Warehouse: Storage system(BigQuery) containing the processed and transformed data for analysis.
Data Analysis: Visualizations and insights derived from the analyzed data, highlighting trends, patterns, and actionable insights using Looker.

## Goals:
Identify patterns and trends in traffic crashes to inform road safety initiatives and policy decisions.
Provide stakeholders with actionable insights to mitigate risks and improve road safety measures.
Enable data-driven decision-making for city planners, transportation agencies, and law enforcement authorities.

## Conclusion:
The Chicago Traffic Crash ETL project plays a crucial role in analyzing and understanding traffic crash data to enhance road safety in the city. By leveraging ETL processes and Apache Spark for data processing and analysis, the project empowers stakeholders with valuable insights to create safer roads and reduce the frequency and severity of traffic crashes.
