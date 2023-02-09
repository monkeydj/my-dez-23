-- manual creation
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-23-375203.dez_asia.fhv_rides_external_2` OPTIONS (
        format = 'CSV',
        uris = ['gs://dez-prefect-test/data/fhv/fhv_tripdata_2019-*.csv.gz']
    );
-- 
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_external`;
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_external_2`;
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_native`;
-- 
-- 
SELECT COUNT(1)
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_native`
WHERE DOlocationID IS NULL
    AND PUlocationID IS NULL;
-- 
-- 
CREATE OR REPLACE TABLE `de-zoomcamp-23-375203.dez_asia.fhv_rides_native_partitioned` PARTITION BY DATE(pickup_datetime) AS
SELECT *
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_native`;
-- 
-- 
CREATE OR REPLACE TABLE `de-zoomcamp-23-375203.dez_asia.fhv_rides_native_partitioned_clustered` PARTITION BY DATE(pickup_datetime) CLUSTER BY affiliated_base_number AS
SELECT *
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_native`;
-- 
-- 
SELECT DISTINCT affiliated_base_number
FROM `de-zoomcamp-23-375203.dez_asia.fhv_rides_native_partitioned` -- `de-zoomcamp-23-375203.dez_asia.fhv_rides_native`
    -- `de-zoomcamp-23-375203.dez_asia.fhv_rides_native_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';