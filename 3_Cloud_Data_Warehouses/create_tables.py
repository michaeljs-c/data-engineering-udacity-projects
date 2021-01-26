import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function iterates through the DROP TABLE statements in sql_queries.py
    and executes them to reset our table schemas.
    
    Arguments:
        cur: the cursor object.
        conn: the postgres connection to our database.
    
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn): 
    """
    This function iterates through the CREATE statements in sql_queries.py
    and executes them'.
    
    Arguments:
        cur: the cursor object.
        conn: the postgres connection to our database.
    
    Returns:
        None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Dropping tables...")
    drop_tables(cur, conn)
    print("Creating tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()