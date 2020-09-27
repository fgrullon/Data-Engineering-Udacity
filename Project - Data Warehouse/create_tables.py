import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import re


def drop_tables(cur, conn):
    for key, query in drop_table_queries.items():
        print('Droping table: '+key)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for key, query in create_table_queries.items():
        print('Creating table: '+key)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER_DB'].values()))
    cur = conn.cursor()
    
    print('Drop Tables if they exists')
    drop_tables(cur, conn)
    print('Create Tables')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()