import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function connects to redshift and drop existing tables (clean the database).

    Parameters
    ----------
    cur : object
        Redshift cursor.
    conn : object
        Redshift connection.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function connects to redshift and create the necessary tables.

    Parameters
    ----------
    cur : object
        Redshift cursor.
    conn : object
        Redshift connection.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function creates a connection to Redshift, clean the database e create some tables.
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
