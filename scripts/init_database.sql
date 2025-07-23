/**************************************************************************************************
* Purpose:
* This SQL script is designed to initialize a clean database environment named 'newdata' 
* by deleting any existing version of the database and recreating it from scratch.
* It also establishes three schemas — bronze, silver, and gold — to support a layered 
* data architecture for ETL and analytics:
*   - bronze: Raw or minimally processed data
*   - silver: Cleaned and transformed data
*   - gold: Curated, business-ready data

* Warning:
* ⚠️ This script will permanently drop the existing 'newdata' database if it already exists.
* ⚠️ All existing data, users, and schema definitions in 'newdata' will be lost.
* ⚠️ Ensure that no critical data is stored in 'newdata' or that a full backup is taken before running this script.
* ⚠️ Intended for development, testing, or controlled environment use only.

* Author: Oluwafemi Popoola
* Date: 23rd July, 2025
**************************************************************************************************/


-- Switch to the 'master' database context
use master;
go

-- Check if the database 'newdata' already exists
if exists(select 1 from sys.databases where name = 'newdata')
begin
    -- If it exists, forcefully set it to SINGLE_USER mode to terminate other connections
    -- and roll back any active transactions immediately
    alter database newdata set single_user with rollback immediate;

    -- Drop the existing 'newdata' database
    drop database newdata;
end;
go

-- Create a new database named 'newdata'
create database newdata;
go

-- Switch to the newly created 'newdata' database
use newdata;
go

-- Create schema 'bronze' for raw or minimally processed data
create schema bronze;
go

-- Create schema 'silver' for cleaned and enriched data
create schema silver;
go

-- Create schema 'gold' for curated, business-ready data
create schema gold;
go
