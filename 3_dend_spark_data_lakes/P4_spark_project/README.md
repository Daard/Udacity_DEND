Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


Schema for Song Play Analysis

Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

ETL

Load AWS credentials (dl.cfg)
Read Sparkify data from S3

song_data: s3://udacity-dend/song_data
log_data: s3://udacity-dend/log_data

Process the data using Apache Spark.
Transform the data and create five tables 
Load data back into S3


How to run 

There are two options:

1. Local mode

    unzip log-data.zip and song-data.zip. 
    
    unzip files must be wrapped in two directories: song_data and output_data.
    
    Run the etl.py script.
    
2. Create Amazon S3 bucket and fulfill dl.cfg file.
  
   Comment these line in etl.py:
   
    # for local run
    sd_input = config['LOCAL']['SONG_DATA']
    ld_input = config['LOCAL']['LOG_DATA']
    output_data = config['LOCAL']['OUTPUT_DATA']
    
    and uncomment these ones:
    
    sd_input = config['AWS']['SONG_DATA']
    ld_input = config['AWS']['LOG_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
    
    Run the etl.py script.
    
