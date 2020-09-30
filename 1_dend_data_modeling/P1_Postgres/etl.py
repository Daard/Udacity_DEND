import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


def process_song_file(cur, filepath):
    """
        - reads songs from filepath
        - preprocesses json fields
        - inserts data into songs and artists tables.
        
    Arguments:
        cur -- cursor of the sparkifydb database
        filepath -- filepath of the log file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.fillna(0)
    
    for i in range(len(df)):
        
        # insert song record
        song_data = song_data = list(df.loc[i, ['song_id', 'title', 'artist_id', 'year', 'duration']].values)
        song_data[-2] = int(song_data[-2])
        cur.execute(song_table_insert, song_data)
        
        # insert artist record
        artist_data = list(df.loc[i, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values)
        artist_data[-1] = int(artist_data[-1])
        artist_data[-2] = int(artist_data[-2])
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        - reads user logs from filepath
        - preprocesses json fields
        - inserts data into users and songplays tables.
        
    Arguments:
        cur -- cursor of the sparkifydb database
        filepath -- filepath of the log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    
    # insert time data records
    attrs = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data = t.map(lambda x : [getattr(x, attr)() if callable(getattr(x, attr)) else getattr(x, attr) for attr in attrs])
    time_data = np.stack(time_data)
    column_labels = attrs
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = (index, pd.to_datetime(row.ts, unit='ms').timestamp(), 
                     int(row.userId), row.level, songid, artistid,
                     row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Walks through all files nested under filepath, and processes all logs found.
    
    Arguments:
        cur -- cursor of the sparkifydb database
        conn -- connection to the sparkifycdb database
        filepath -- filepath parent of the logs to be analyzed
        func -- function to be used to process each log
        
    Returns:
        name of files processed
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    ETL function used for data from song and user activity logs
       
    Usage:
        python etl.py
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()