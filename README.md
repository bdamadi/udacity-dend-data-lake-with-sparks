# Project: Data Lake

## Project Summary

This project is to build an ETL pipeline for analysis songs and users
activity on the specified music streaming app.

The given data set contains data of song data files and log files
stored in AWS S3. The ETL pipeline create a Spark session to process
songs data and log data files to create dimensional tables for using
by analytics team. The output tables are written as Parquet files
storing in S3 directores. Each table is stored as a seperated directory.

In the provided dataset, a song data file contains information of a 
song and its artist, and a log file records user activity on the app.
Each activity contains information that describes:

* The user who performs the action
* When the action is performed
* Which song (and its relevent data) the user was listenning.

For this analysic purpose, the fact table mainly records "song play"
action which contains songplay_id, start_time, user_id, level, song_id,
artist_id, session_id, location, and user_agent for a log record.

Dimension tables are for storing users in the app, songs and artists in
music database, and timestamp that broken down into detail resolution,
including hour, day, week, month, year, weekday.

## Database schema

### Fact table

`songplays` table stores records in log data associated with song plays.
Each record in a log file is populated as a recod in this table. There are
following design decisions I have made for this table:

* Primary key: using Spark's `monotonically_increasing_id` function
to generates monotonically increasing 64-bit integers as `songplays_id`.

* In addition to the requirement of `songplays` schema, I added two columns,
`month` and `year`, which are the month and year value extracted from
timestamp column. These two columns are used to partition songplays table
by year and month as required by the requirement.

### Dimension tables

`songs` table stores song records which are extracted from song data
file. `song_id` is used as primary key. To avoid duplication, Spark's
`distinct()` is used to extract distinct song rows from the input
song data.

`artists` table stores artist records which are extracted from song data
file. `artist_id` is used as primary key. To avoid duplication, Spark's
`distinct()` is used to extract distinct artist rows from the input
song data.

`users` table stores user records, extracted from each log record from
log files. `user_id` is used as primary key. To avoid duplication, Spark's
`distinct()` is used to extract distinct user rows from the input
log data.

`time` table stores broken-down timestamps from log records. The
timestamp value is used as primary key. The original timestamp column
of the input data set has microsecond-precision. This timestamp is
transformed into UNIX timestamp, e.i. second-precision. This reduces
number of data in time table because the analytics purpuse of this
pipeline don't requires time resolution more precise than seconds.

## Source code

### `dl.cfg`

This is the config file required to run the ETL pipeline. It defines
the AWS access key ID and secret so that Spark session can read the
input data set on S3 bucket as well as write output table to S3.

### `etl.py`

This file implements the ETL pipeline that processes all song data 
files and log files to populate data to the sparkify database.

## Running

The following steps are required to run this ETL pipeline:

1. Edit `dl.cfg` to set the AWS access key ID and secret. Make sure this
key has permission to write data to the output S3 directory.

2. Edit `etl.py` file to set the output S3 directory in variable 
`output_data` under `main` function.

3. Run the ETL pipeline by the following command, assume that the
data folder is placed in the same directory with `etl.py` program.

```
python etl.py
```

Or use the follwoing command to submit and run ETL as Spark jobs
on a AWS Spark EMR cluster:

```
spark-submit etl.py
```
