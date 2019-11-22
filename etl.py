import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


# Read in the config file which contains the AWS access key ID and secret
config = configparser.ConfigParser()
config.read('dl.cfg')

# Set the AWS access key to enviroment variables to allow Spark to access
# S3 buckets
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data from the given input data (input_data) to populate
    data for songs table and artists table. Output tables are written as 
    parquet files to the specified output S3 folder:

    * songs table: saved in 'songs' directory at the given output_data,
    partitioned by year and artist.

    * artists table: saved in 'artists' directory at the given output_data.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    # avoid duplicate in songs table
    songs_table = songs_table.distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') \
        .partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              col('artist_name').alias('name'), 
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude'))
    # avoid duplicate in artists table
    artists_table = artists_table.distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
        .parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    Process lof data from the given input data (input_data) to populate
    data for users table, time table and songplays table. Output tables 
    are written as parquet files to the specified output S3 folder:

    * users table: saved in 'users' directory at the given output_data.
    * time table: saved in 'time' directory at the given output_data.
    * songplays table: saved in 'songplays' directory at the given
    output_data, partitioned by year and month.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level')
    # avoid duplicate in users table
    users_table = users_table.distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite') \
        .parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts // 1000, 'long')
    df = df.withColumn('ts', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts), 'timestamp')
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select(col('ts').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime', 'E').alias('weekday'))
    # avoid duplicate in time table
    time_table = time_table.distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
        .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))

    # read in artist data to use for songplays table
    artist_df = spark.read.parquet(os.path.join(output_data, 'artists'))

    # joined with artist to populate artist name to song data
    song_df = song_df.join(artist_df.select(
            'artist_id', 
            col('name').alias('artist_name')), 
            'artist_id')

    # extract columns from joined song, artist and log datasets to create songplays table 
    conditions = [
        df.song == song_df.title,
        df.artist == song_df.artist_name,
        df.length == song_df.duration
    ]
    songplays_table = df \
        .join(song_df, conditions) \
        .select(col('ts').alias('start_time'),
                col('userId').alias('user_id'),
                'level',
                'song_id',
                'artist_id',
                col('sessionId').alias('session_id'),
                'location',
                col('userAgent').alias('user_agent'),
                # include extra fields, month and year, for partitioning
                # when writing as parquet files
                month('datetime').alias('month'),
                year('datetime').alias('year'),) \
        # set unique id for each songplays row using monotonically increasing id
        .withColumn('songplays_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'songplays'))


def main():
    """
    Main function that runs the ETL pipeline.

    It firstly processes song data from the S3 storage as a Spark dataframe
    to extract songs and artists into the corresponding tables. It then 
    processes log event data from the input S3 directory to populate user
    table, time table, and songs_play table.

    Each of the five tables are written to parquet files in a separate 
    directory on S3.
    """

    spark = create_spark_session()
    
    # Set the S3 directory which contains the input data set
    input_data = "s3a://udacity-dend/"
    # Set the S3 directory where output tables are saved
    output_data = "s3a://babak-udacity-project4/"

    # Run the ETL pipeline
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
