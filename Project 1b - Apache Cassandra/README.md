

# Data Modeling with Apache Cassandra

## **Overview**
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## **Requirements**
Create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Schema

**songs_play** 

```
sessionID, itemInSession, artist, song, length, PRIMARY KEY(sessionID, itemInSession)
```

**user_artists** 

```
userid, sessionID, itemInSession, artist, song, first_name, last_name, PRIMARY KEY(userid, sessionID)
```

**song_history** 

```
song, userid, artist, first_name, last_name, PRIMARY KEY(song, userid)
```


