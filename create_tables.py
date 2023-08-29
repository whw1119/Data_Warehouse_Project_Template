import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop a table on Redshift

    Args:
        cur: cursor object
        conn: connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create a table on Redshift

    Args:
        cur: cursor object
        conn: connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    create the staging, fact and dimension tables for the star schema on Redshift, drop the tables if they exist already
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
