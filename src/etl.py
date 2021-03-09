import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, truncate_table_queries
import sys


def truncate_tables(cur, conn):
    """Reads the truncate_table_queries list and executes each query.
       truncate tables: staging and dimensions tables only
       
       Input Arguments: cur - cursor, 
                        conn - database connection
    """
    for query in truncate_table_queries:
        print(f"### Executing query: {query}")
        cur.execute(query)
        conn.commit()
        
    print("### Tables truncated Correctly! ###")

def load_staging_tables(cur, conn):
    """Reads the copy_table_queries list and executes each query.
       loads the staging tables
       
       Input Arguments: cur - cursor, 
                        conn - database connection
    """
    for query in copy_table_queries:
        print(f"### Executing query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Reads the insert_table_queries list and executes each query.
       loads the fact and dimension tables
       
       Input Arguments: cur - cursor, 
                        conn - database connection
    """
    for query in insert_table_queries:
        print(f"### Executing query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('./aws/credentials.cfg')
    
    # connects to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))    
    cur = conn.cursor()
    
    try:
        truncate_tables(cur, conn)
    except psycopg2.Error as e:
        print("### Error in truncate_tables function ###")
        print(e)
        sys.exit() # ends etl process
    
    try:
        load_staging_tables(cur, conn)
    except psycopg2.Error as e:
        print("### Error in load_staging_tables function ###")
        print(e)
        sys.exit() # ends etl process
        
    try:
        insert_tables(cur, conn)
    except psycopg2.Error as e:
        print("### Error in insert_tables function ###")
        print(e)
        
    conn.close()
    
    print("### Loading Completed!! ###")

if __name__ == "__main__":
    main()