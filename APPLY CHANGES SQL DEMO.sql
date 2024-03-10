-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE target_SCD2_SQL;

APPLY CHANGES INTO
  live.target_SCD2_SQL
FROM
  stream(example.temp_bronze)
KEYS
  (deviceID)
APPLY AS DELETE WHEN
  operation_type = "delete"
SEQUENCE BY
  c_time
COLUMNS * EXCEPT
  (rpm)
STORED AS
  SCD TYPE 2;
