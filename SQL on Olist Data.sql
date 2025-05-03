--Query for selecting from Silver layer
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://olistadlsstorage.dfs.core.windows.net/olist/silver/',
        FORMAT = 'PARQUET'
    ) AS RESULT;

-- Creating a schema and adding view to store the data

create schema gold;


-- Creating a view and storing the silver layer data to the view
CREATE VIEW gold.final
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistadlsstorage.dfs.core.windows.net/olist/silver/',
        FORMAT = 'PARQUET'
    ) AS RESULT1;


-- Selcting data from the view
select * from gold.final



-- creating a different view to store data having records with satus as delivered

CREATE VIEW gold.final_delivered
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistadlsstorage.dfs.core.windows.net/olist/silver/',
        FORMAT = 'PARQUET'
    ) AS RESULT12
    where order_status = 'delivered';



-- Selcting from the delivered view
select top 100 * from gold.final_delivered ;






-- Creating External table as select(CETAS)

-- To create external table we need an encryption and managed identity
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Abishek@01';
CREATE DATABASE SCOPED CREDENTIAL abiadmin WITH IDENTITY = 'Managed Identity';


SELECT * FROM sys.database_credentials;



-- saving the file to gold layer

--To save a file we need file format and path

--File format
CREATE EXTERNAL FILE FORMAT extfiledormat WITH(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

--Location
CREATE EXTERNAL DATA SOURCE goldlayer WITH(
    LOCATION = 'https://olistadlsstorage.dfs.core.windows.net/olist/gold/',
    CREDENTIAL = abiadmin
);

-- Saving file to external table using location and file format created above
CREATE EXTERNAL TABLE gold.final_table WITH (
        LOCATION = 'final_serving',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfiledormat
) AS
SELECT * FROM gold.final;
