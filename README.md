
# Apache Beam Pipeline for Cleaning Batch Data Using Cloud Dataflow and BigQuery
Apache Beam is an open-source, unified model for constructing both batch and streaming data processing pipelines. Beam supports multiple language-specific SDKs for writing pipelines against the Beam Model such as Java, Python, and Go and Runners for executing them on distributed processing backends, including Apache Flink, Apache Spark, Google Cloud Dataflow and Hazelcast Jet.

We are going to create a pipeline that cleans the data for making it analysis ready using apache beam (Python SDK), and run it using dataflow runner. Here we are going to use Craft Beers Dataset from Kaggle.

## The architecture uses:

Google Cloud Storage- to store CSV source files
Google Cloud Datastore- to store CSV file structure and field type
Google Cloud Dataflow to read files from Google Cloud Storage, Transform data base o-n the structure of the file and import the data into Google BigQuery
Google BigQuery- to store data in a Data Lake.

## Prerequisites

-Up and running GCP project with enabled billing account
-gcloud installed and initiated to your project
-Google Cloud Datastore enabled
-Google Cloud Dataflow API enabled
-Google Cloud Storage Bucket containing the file to import (CSV format) using the following naming convention: TABLENAME_*.csv
-Google Cloud Storage Bucket for tem and staging Google Dataflow files
-Google BigQuery dataset
-Python >= 2.7 and python-dev module
-gcc
-Google Cloud Application Default Credentials


## Basic flow of the pipeline 

1. Read the data from google cloud storage bucket (Batch). 
2. Apply some transformations such as splitting data by comma separator, dropping unwanted columns, convert data types, etc.
3. Write the data into data Sink (BigQuery) and analyze it.



## Data lake to data mart


For joining data from two different datasets in BigQuery, applying transformations to the joined dataset before uploading to BigQuery.

Joining two datasets from BigQuery is a common use case when a data lake has been implemented in BigQuery. Creating a data mart with denormalized datasets facilitates better performance when using visualization tools.

This pipeline contains 4 steps:

Read in the primary dataset from BigQuery.
Read in the reference data from BigQuery.
Custom Python code is used to join the two datasets. Alternatively, CoGroupByKey can be used to join the two datasets.
The joined dataset is written out to BigQuery.

## The joined dataset is written out to BigQuery


Finally the joined dataset is written out to BigQuery. This uses the same BigQueryIO API which is used in previous examples.