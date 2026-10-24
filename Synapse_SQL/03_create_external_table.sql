-- 1. Master key (needed once, before the credential)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your-strong-password>';

-- 2. Credential using the workspace's managed identity
CREATE DATABASE SCOPED CREDENTIAL cred_adw
WITH IDENTITY = 'Managed Identity';

-- 3. External data sources (one per container)
--For Silver
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION   = 'https://adwstoragedatalake.dfs.core.windows.net/silver/',
    CREDENTIAL = cred_adw
);

--For Gold
CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION   = 'https://adwstoragedatalake.dfs.core.windows.net/gold/',
    CREDENTIAL = cred_adw
);


-- 4. External file format
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE      = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


--Create External Table EXTCALENDAR
CREATE EXTERNAL TABLE gold.extcalendar
WITH (
    LOCATION    = 'extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.calendar;


--Create External Table EXTCUSTOMERS
CREATE EXTERNAL TABLE gold.extcustomers
WITH (
    LOCATION    = 'extcustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customers;


--Create External Table EXT Product Categories
CREATE EXTERNAL TABLE gold.extprodcat
WITH (
    LOCATION    = 'extprodcat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.prodcat;


--Create External Table EXTPRODUCTS
CREATE EXTERNAL TABLE gold.extproducts
WITH (
    LOCATION    = 'extproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.products;


--Create External Table EXTRETURNS
CREATE EXTERNAL TABLE gold.extreturns
WITH (
    LOCATION    = 'extreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.returns;


--Create External Table EXTSALES
CREATE EXTERNAL TABLE gold.extsales
WITH (
    LOCATION    = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;


--Create External Table EXTterritories
CREATE EXTERNAL TABLE gold.extterritories
WITH (
    LOCATION    = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.territories;


--Create External Table EXT Product_Subcategories
CREATE EXTERNAL TABLE gold.extpSubCat
WITH (
    LOCATION    = 'extpSubCat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.psubcat;
