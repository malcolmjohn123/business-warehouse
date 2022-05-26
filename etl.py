import psycopg2 as db
import db_credentials 
from sql import *

def execute_query(queries):
    with db.connect(**db_credentials.server_params) as conn:
        with conn.cursor() as curs:
            for query in queries:
                curs.execute(query)
                conn.commit()

def main():
    
    print("Dropping all the dimensional and fact tables")
    execute_query(drop_table_queries)

    print("Creating all the dimensional and fact tables")
    execute_query(create_table_queries)
    
    print("Loading Dimention tables")
    execute_query(insert_Dim_queries)

    print("Loading Fact tables")
    execute_query(insert_fact_queries)

if __name__ == "__main__":
    main()