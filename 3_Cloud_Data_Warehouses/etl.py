import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data from our S3 bucket into the staging tables. Data will 
    then be ready for redshift to insert into our main tables.
    
    Arguments:
        cur: the cursor object.
        conn: the postgres connection to our database.
    
    Returns:
        None
    """
    for query in copy_table_queries:
        print("Loading..",query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts data into our main tables from our staging tables'.
    
    Arguments:
        cur: the cursor object.
        conn: the postgres connection to our database.
    
    Returns:
        None
    """
    for query in insert_table_queries:
        print("Loading..",query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()