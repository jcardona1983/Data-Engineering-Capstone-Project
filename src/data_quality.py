import configparser
import psycopg2
import json
import pandas as pd


def count_records(cur, conn, df):
    """Counts # of records for each table and compares them to the ones 
       in the metadata file. also checks if a table doesn't exist.
       
       Input Arguments: cur - cursor, 
                        conn - database connection
                        df - pandas dataframe with metadata info
    """
    print("#### Data quality checking: counting records ####")

    for row in df.iterrows():
        table = row[1]['table']
        expected_rows = row[1]['expected_rows']
        
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            records = cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error: {e}") # Table doesn't exist
            conn.rollback()
            continue
            
        if records < 1:
            print(f"Error: Data quality check failed. {table} contained 0 rows")
        elif records == expected_rows:
            print(f"Passed: {table} loaded with {records} rows, expected rows:{expected_rows}")
        else:
            print(f"Error: Check failed.{table} loaded with {records} rows, expected rows:{expected_rows}") 


def check_null(cur, conn, df):
    """Checks # of records with NULL values or empty strings in the description column for dimension.
       also checks if a table doesn't exist.
       
       Input Arguments: cur - cursor, 
                        conn - database connection
                        df - pandas dataframe with metadata info
    """
    print("#### Data quality checking: identifying records with null or empty values in description column ####")
    
    for row in df.iterrows():
        table = row[1]['table']
        desc_col = row[1]['desc_column']
        
        if(desc_col == ""):
            print(f"No check needed for {table}")
            continue
        
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {desc_col} IS NULL OR ltrim(rtrim({desc_col})) = ''")
            records = cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error: {e}") # Table doesn't exist
            conn.rollback()
            continue

        if records >= 1:
            print(f"Error: Check failed. {table} contained {records} rows with NULL values in {desc_col}")
        else:
            print(f"Passed: Data quality on table {table} check passed with 0 NULL records")
        

def main():
    config = configparser.ConfigParser()
    config.read('./aws/credentials.cfg')
    
    # reads the metadata file
    with open('./metadata.json') as f:
        data = json.load(f)

    df_metadata = pd.DataFrame.from_dict(data)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # calling quality checks
    count_records(cur, conn, df_metadata)
    print("")
    check_null(cur, conn, df_metadata)
    
    conn.close()


if __name__ == "__main__":
    main()