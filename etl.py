import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Extracting data from JSON files in s3://udacity-dend and executing INSERT queries to append data to staging tables
    , staging_events and staging_songs, in sparkifydb in AWS Redshift Cluster
    :param cur: The database cursor
    :param conn: The connection to Sparkify database in AWS Redshift Cluster
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Extracting data from JSON files in s3://udacity-dend and executing INSERT queries to append data to staging tables
    , users, songs, time, and artists, in sparkifydb in AWS Redshift Cluster
    :param cur: The database cursor
    :param conn: The connection to Sparkify database in AWS Redshift Cluster
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the sparkify database in
    AWS Redshift clustter and gets cursor to it.

    - Load JSON files to staging_events and staging_songs tables.

    - Insert data into fact and dim tables by get data from staging tables.

    - Finally, close the connection.
    :return: None
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
