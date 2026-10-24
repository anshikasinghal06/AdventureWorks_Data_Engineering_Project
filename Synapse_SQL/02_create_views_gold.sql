-- Create View Calendar
CREATE VIEW gold.calendar
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) as QUERY_Cal


-- Create View Customers
CREATE VIEW gold.customers
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) as QUERY_Cust


-- Create View Product Categories
CREATE VIEW gold.prodcat
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
    FORMAT = 'PARQUET'
) as QUERY_ProdCat


-- Create View Products
CREATE VIEW gold.products
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) as QUERY_Prod


-- Create View Returns
CREATE VIEW gold.returns
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) as QUERY_Ret


-- Create View Sales
CREATE VIEW gold.sales
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) as QUERY_Sales


-- Create View Territories
CREATE VIEW gold.territories
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) as QUERY_Terr


-- Create View Product_Subcategories
CREATE VIEW gold.psubcat
AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adwstoragedatalake.dfs.core.windows.net/silver/Product_Subcategories/',
    FORMAT = 'PARQUET'
) as QUERY_PSubCat

