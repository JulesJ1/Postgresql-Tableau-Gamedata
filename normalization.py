import psycopg
import os
from dotenv import load_dotenv


load_dotenv()
conf = {"host":os.getenv("SQLHOST"), 
        "dbname":"postgres", 
        "user":os.getenv("SQLUSER"),
        "password":os.getenv("SQLPASSWORD"),
        "port":os.getenv("SQLPORT")}


DATABASE_URL = f"{conf['user']}://postgres:{conf['password']}@{conf['host']}/gamedata"
TABLE_NAME = "games"


def create_table(table_name):
  with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
      cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} 
      """) 
      conn.commit()


def remove_column(column_name):
  with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
      cur.execute(f"""ALTER TABLE {TABLE_NAME} 
        DROP COLUMN {column_name}
      """) 
      conn.commit()

def add_column(column_name):
  with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
      cur.execute(f"""ALTER TABLE {TABLE_NAME}
        ADD COLUMN {column_name}
      """)
      conn.commit()

def insert_data(table):
    with psycopg.connect(DATABASE_URL) as conn:
      with conn.cursor() as cur:
        cur.execute(f"""
        
        WITH tags AS (SELECT string_to_array("Tags",',') AS names FROM games )
        INSERT INTO {table} (test_name) SELECT DISTINCT unnest("names") FROM tags LIMIT 10
        
        """)
        conn.commit()
        cur.close()
      conn.close()
    
def rename_column(column_name, new_name):
    with psycopg.connect(DATABASE_URL) as conn:
      with conn.cursor() as cur:
        cur.execute(f"""
        ALTER TABLE {TABLE_NAME}
        RENAME COLUMN {column_name} TO {new_name};
        
        """)
        conn.commit()
        cur.close()
      conn.close()
    
def delete_duplicates():
      with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
          cur.execute(f"""
                DELETE FROM {TABLE_NAME} g1
                WHERE EXISTS
                (
                  select null
                  FROM {TABLE_NAME} g2
                  WHERE g2.app_id = g1.app_id
                  AND g2.ctid > g1.ctid
                );
          
            """)
          conn.commit()
        cur.close()
      conn.close()



if __name__ == '__main__':

  query = """WITH tagslist AS (SELECT string_to_array("Tags",',') AS names FROM games )
            SELECT DISTINCT unnest("names") FROM tagslist
            LIMIT 10
            """
 
              
  rename_column('"App ID"',"app_id")