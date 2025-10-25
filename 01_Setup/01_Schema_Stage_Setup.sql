-- Set the context (Ensure you have the right role/warehouse selected)
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60;
USE WAREHOUSE COMPUTE_WH;

-- 1. Create Database
CREATE DATABASE IF NOT EXISTS CRICKET;

-- 2. Create Layered Schemas
CREATE SCHEMA IF NOT EXISTS CRICKET.LAND;
CREATE SCHEMA IF NOT EXISTS CRICKET.RAW;
CREATE SCHEMA IF NOT EXISTS CRICKET.CLEAN;
CREATE SCHEMA IF NOT EXISTS CRICKET.CONSUMPTION;

-- 3. Create JSON File Format in the Landing Schema
USE SCHEMA CRICKET.LAND;
CREATE FILE FORMAT IF NOT EXISTS MY_JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE -- Crucial: Used if the JSON data is an array of records (e.g., [{}, {}])
    COMMENT = 'File format for loading semi-structured cricket match JSON data.';

-- 4. Create Internal Named Stage
CREATE STAGE IF NOT EXISTS MY_STAGE
    FILE_FORMAT = MY_JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage to hold raw JSON files uploaded via PUT command or Snowsight.';
