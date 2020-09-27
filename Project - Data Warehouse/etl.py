import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('Copy data to staging tables')
    for key, query in copy_table_queries.items():
        print('Copying to table: '+key)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    print('Inserting into Facts and Dimension tables')
    for key, query in insert_table_queries.items():
        print('Inserting to table: '+key)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER_DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()