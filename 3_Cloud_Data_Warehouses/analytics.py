from sql_queries import analytics_query
import configparser
import psycopg2

def run_analytics(cur, conn, table):
    cur.execute(analytics_query + " " + table)
    
    result = cur.fetchone()
    for row in result:
        print(table, "Row Count:", row)
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    for table in ["staging_events", "staging_songs", "songplays", "songs", "artists", "users", "time"]:
        run_analytics(cur, conn, table)
    conn.close()
    
if __name__ == "__main__":
    main()