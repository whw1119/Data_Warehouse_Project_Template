# Project: Data Warehouse

## Summary of the Project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 

The purpose of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Explanation of the Files
### create_tables.py
Connects to redshift and drops any existing tables if they exist, then create the staging, fact and dimension tables for the star schema on Redshift.

### etl.py
Loads data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift.

### sql_queries.py
Contains SQL queries used by `create_tables.py` and `etl.py`.

### dwh.cfg
Contains configuration data needed to connect to S3 and redshift database.

## How to Run
First run 'create_tables.py' an then 'run etl.py'.
