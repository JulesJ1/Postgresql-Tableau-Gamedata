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

conn = psycopg.connect(host=conf['host'], dbname=conf['dbname'], user=conf['user'],password=conf['password'],port=conf['port'])

cur = conn.cursor()

conn.set_session(autocommit=True)
cur.execute("CREATE TABLE IF NOT EXISTS gamedata")


conn.commit()

cur.close()

conn.close()