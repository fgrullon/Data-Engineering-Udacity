import configparser
import psycopg2
from sql_queries import select_number_rows_queries


def get_results(cur, conn):
    """
        Count records for each table
        params:
        cur, conn
        return:
    """
    for query in select_number_rows_queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER_DB'].values()))
    cur = conn.cursor()
    
    get_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()