# Sparkify Data Lake

## Overview

This project builds an ETL pipeline relying on Apache Spark for a data lake
hosted on Amazon S3 for Sparkify's analytics purposes.

The ETL loads song and log data in JSON format from S3; processes the data into
analytics tables in a star schema using Spark; and writes those tables into
partitioned parquet files on S3. A star schema has been used to allow the
Sparkify team to readily run queries to analyze user activity on their app.
Spark was used because it's powerful, can be run from a Python shell, and can
be deployed in the cloud, among other reasons.

## Structure

The project contains the following components:

* `etl.py` reads data from S3, processes it into analytics tables, and then writes them to S3
* `dl.cfg` [not pushed to GitHub repo] contains your AWS credentials 

## Analytics Schema

The analytics tables centers on the following fact table:

* `songplays` - which contains a log of user song plays

`songplays` has foreign keys to the following dimension tables:

* `users`
* `songs`
* `artists`
* `time`

## Instructions

You will need to create a configuration file `dl.cfg` with the following
structure:

```
[AWS]
AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
```

To execute the ETL pipeline from the command line, enter the following:

```
python etl.py
```
