import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies data from S3 buckets into Staging tables, defined in copy_table_queries
    """
    print("Loading Staging Tables...")
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("Done")


def insert_tables(cur, conn):
    """
    Copies data from Staging tables into analytic tables
    """
    print("Loading data into tables...")
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("Done")

def main():
    """
    Creates database connection and handles loading of data into tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as e:
        print(e)
    
    conn.close()


if __name__ == "__main__":
    main()