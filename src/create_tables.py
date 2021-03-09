import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Reads the drop_table_queries list and executes each query.
       drops all tables: staging, fact and dimensions tables
       
       Input Arguments: cur - cursor, 
                        conn - database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    print("### Tables Dropped Correctly! ###")

def create_tables(cur, conn):
    """Reads the create_table_queries list and executes each query.
       creates all tables: staging, fact and dimensions tables
       
       Input Arguments: cur - cursor, 
                        conn - database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
    print("### Tables Created Correctly! ###")

def main():
    config = configparser.ConfigParser()
    config.read('./aws/credentials.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    try:
        drop_tables(cur, conn)
    except psycopg2.Error as e:
        print(e)
        
    try:
        create_tables(cur, conn)
    except psycopg2.Error as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()