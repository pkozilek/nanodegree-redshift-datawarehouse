import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function reads data from files on S3 and insert on staging tables on Redshift.

    Parameters
    ----------
    cur : object
        Redshift cursor.
    conn : object
        Redshift connection.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function reads data from staging tables and insert data on a star schema for
    data analysis purposes.

    Parameters
    ----------
    cur : object
        Redshift cursor.
    conn : object
        Redshift connection.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This functin creates a connection to Redshift and build a database for data analysis
    based on S3 data files.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
