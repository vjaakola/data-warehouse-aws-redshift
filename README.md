### Data Warehouse project: ETL pipeline and AWS Redshift

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I'll demonstrate how to make a ETL pipeline and prepare JSON files to star schema datababase, that Sparkify's analytics can use for their analysis. 
Data warehouse is builded with AWS Redshift using Infrastructure-as-code (IaC).

Key components: ETL pipeline, AWS S3 and Redshift, Infrastructure-as-code (IaC)

This project is part of Udacity's Data Engineer Nanodegree program.
This is a simplified repository for only codes, the data is no available here.


## Database Schema

image ![Image of star schema](https:/....)

First we have to make staging tables for the copy of S3 bucket and then create and insert finale tables. I used to Zstandard (ZSTD) encoding because it provides a high compression ratio across diverse datasets. 
