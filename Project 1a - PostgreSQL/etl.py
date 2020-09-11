import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ 
    This function reads the song files and stores the artist and song information in the database.

     1 - We read the json file located in the filepath passed as a parameter using the read_json function.
     2 - we create a list with the information of the song, using the pandas function 'values'  to capture the value at the indicated index.
     3 - Using the cursor passed as a parameter we proceed to pass the query song_table_insert which we export from the sql_queries file and the list that contains the information of the song to insert it in the songs table.
     4 - we create a list with the information of the artist, using the pandas function 'values'  to capture the value at the indicated index.
     5 - Using the cursor passed as a parameter we proceed to pass the query artist_table_insert which we export from the sql_queries file and the list that contains the information of the artist to insert it in the artists table.

     Parameters:
     cur (cursor): This allows us to execute PostgreSQL command in the database session.
     filepath (string): location of the file we will read

    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = [df.values[6], df.values[7], df.values[1], df.values[9], df.values[8]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[1], df.values[5], df.values[4], df.values[2], df.values[3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """     
    This function reads the log files, we create the time and user dimensions and the fact table songplay and insert the information in the database.
    
    1 - We read the json file located in the filepath passed as a parameter using the read_json function.
    2 - We filter the dataframe by the 'page' column, and we return only the cases where value is 'NextSong'.
    3 - We save the content of column 'ts' as datetime in variable t.
    4 - Using the variable t and the column ts, we create a dictionary containing the detail that will make up the time dimension.
    5 - We convert the dictionary into a dataframe and iterating through each row in time_df, we insert the data into the time table.
    6 - We proceed to create the user dataframe and iterating over each row of this dataframe we insert the user information in the users table.
    7 - Iterating over each file of the dataframe that contains the log file,
        1 - We make a select to the songs table, passing it as parameters song, artist and length, to return the song_id and artist_id.
        2 - if the select brought information about the song_id and artist_id we save them in the songid and artistid variables, otherwise we assign the value None.
        3 - We create the songplay_data tuple using the information from the dataframe file and the songid and artistid variables.
        4 - We insert the songplay data into the songplay table.
    
    Parameters:
    cur (cursor): This allows us to execute PostgreSQL command in the database session.
    filepath (string): location of the file we will read

    """
    
    # open log file
    df =  pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t =  df['ts'].astype('datetime64[ms]')
    
    # insert time data records
    time_data = {
        'start_time':df['ts']
        ,'hour':t.dt.hour
        ,'day':t.dt.day
        ,'week':t.dt.week
        ,'month':t.dt.month
        ,'year':t.dt.year
        ,'weekday':t.dt.weekday
    }
    column_labels = ('start_time', 'hour', 'day',  'week',  'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame({
        'user_id': df['userId']
        ,'first_name' : df['firstName']
        ,'last_name' : df['lastName']
        ,'gender' : df['gender']
        ,'level' : df['level']
    }) 


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
        songplay_data = (
            row.length
            ,row.userId
            ,row.level
            ,songid
            ,artistid
            ,row.sessionId
            ,row.location
            ,row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """ 

    This function reads the directory and using the function as a parameter, we process the data contained in it.

    1 - We create the empty all_files list and using the filepath parameter we read all the files contained in the directory with the extension .json
    and we add them to the all_files list.
    2 - We store the number of files contained in the directory in the variable num_files.
    3 - We print the number of files found and the directory path.
    4 - We iterate over the all_files list and pass it to the function pass as parameter 'func' the cursor and the json path.
    5 - We commit changes.
    6 - We print the number of processed files of the total.

    Parameters:
    cur (cursor): This allows us to execute PostgreSQL command in the database session.
    filepath (string): location of the file we will read
    conn (connection): Connection to the database
    func (function): This is the function with which we will process the file
    
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
    Function that will be executed when this file is called.

     1 - We create the connection to the database.
     2 - We save the cursor in the variable cur.
     3 - We execute the process_data function to process the song files, passing the path and the process_song_file function as a parameter.
     4 - We execute the process_data function to process the log files, passing the path and the process_log_file function as a parameter.
     5 - The connection is closed.

    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()