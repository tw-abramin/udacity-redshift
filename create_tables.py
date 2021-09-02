import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables defined in drop_table_queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables defined in create_table_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Creates Database connection and handles safe creation of tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    try:
        print('Dropping any tables already created...')
        drop_tables(cur, conn)

        print('Creating tables...')
        create_tables(cur, conn)
    except Exception as e:
        print(e)

    print('Done')
    conn.close()


if __name__ == "__main__":
    main()