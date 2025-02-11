# Module 3 Homework

## Preparation

I have used the suggested python script, modifying it slightly, because I authenticated with gcloud, 
but also did not particularly like that the files were still on my local disk (which is still crying for space),
so I added a piece of code that removes the files after verifying its upload.

I have created two tables as instructed:
```
-- Create external table from GCS path

CREATE OR REPLACE EXTERNAL TABLE `<project_id>.homework_3.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_hw3_1313/yellow_tripdata_2024-*.parquet']
);

-- Create non-partitioned table from external table

CREATE OR REPLACE TABLE `<project_id>.homework_3.yellow_tripdata_non_partitioned` AS
SELECT * FROM `<project_id>.homework_3.external_yellow_tripdata`;
```

## Question 1:
> What is count of records for the 2024 Yellow Taxi Data?

SQL Script:
```
SELECT COUNT(1) FROM `homework_3.yellow_tripdata_non_partitioned`;
```

- 20,332,093

## Question 2:
> Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

SQL Script:
```
SELECT COUNT(DISTINCT PULocationID) FROM `<project_id>.homework_3.external_yellow_tripdata`;

SELECT COUNT(DISTINCT PULocationID) FROM `<project_id>.homework_3.yellow_tripdata_non_partitioned`;
```

- 0 MB for the External Table and 155.12 MB for the Materialized Table


## Question 3:
> Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```
SELECT PULocationID FROM `<project_id>.homework_3.yellow_tripdata_non_partitioned`;

SELECT PULocationID, DOLocationID FROM `<project_id>.homework_3.yellow_tripdata_non_partitioned`;
```

- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


## Question 4:
> How many records have a fare_amount of 0?

```
SELECT COUNT(1) FROM `<project_id>.homework_3.yellow_tripdata_non_partitioned` WHERE fare_amount=0;
```
- 8,333

## Question 5:
> What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- Partition by tpep_dropoff_datetime and Cluster on VendorID

SQL Script to create the table:
```
-- Create partitioned and clustered table
CREATE OR REPLACE TABLE `<project_id>.homework_3.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `<project_id>.homework_3.external_yellow_tripdata`;
```


## Question 6:
> Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>
> Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>
>Choose the answer which most closely matches.</br> 

```
-- Non partitioned table: 310,24 MB

SELECT DISTINCT VendorID 
FROM `homework_3.yellow_tripdata_non_partitioned` 
WHERE tpep_pickup_datetime >="2024-03-01" AND tpep_pickup_datetime < "2024-03-16";

-- Partitioned table 26,85 MB

SELECT DISTINCT VendorID 
FROM `homework_3.yellow_tripdata_partitioned_clustered` 
WHERE tpep_pickup_datetime >="2024-03-01" AND tpep_pickup_datetime < "2024-03-16";
```

- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table



## Question 7: 
> Where is the data stored in the External Table you created?

- GCP Bucket


## Question 8:
> It is best practice in Big Query to always cluster your data:

- False

With small tables (< 1 GB)  don't show significant improvement with partitioning and clustering,
but can add overhead costs in managing the table. Additionally, data with high cardinality can result in decreased efficiency, due to limitations in the number of clusters per partition.

## (Bonus: Not worth points) Question 9:
> No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

BigQuery estimates 0 B, because it retrieved the row count from the table's metadata. Adding a condition will cause BigQuery to estimate processed data.

## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3