# Data-warehouse-nanodegree

This repo contains all files for the third project of "Data Engineering Nanodegree" course from Udacity. See the project description below.

# Summary

A music streaming startup called Sparkify wants to migrate its data to a cloud data warehouse.

This project aims to build an ETL capable of extracting data from S3, saving it in staging tables on Redshift and performing a transformation to provide an appropriate schema for the data analysis team.

# Database schema

##  Staging tables

* staging_events: app user activity events.
* staging_songs: songs and artists metadata.

## Star schema

### Fact table

* **songplays**: records in log data associated with song plays.

### Dimension tabels

* **users**: app users info.
* **songs**: songs info.
* **artists**: artists info.
* **time**: timestamps of records in songplays broken down into specific time units (hour, day, week, month, year, weekday).

# Files description

## create_tables.py

* Connect to the Redshift cluster
* Drop existing tables
* Create tables

## etl.py

* Connecto to the Reshift cluster
* Load data on staging tables
* Insert data on Starchema tables
