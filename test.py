import psycopg
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

conf = {"host":os.getenv("SQLHOST"), 
        "dbname":"postgres", 
        "user":os.getenv("SQLUSER"),
        "password":os.getenv("SQLPASSWORD"),
        "port":os.getenv("SQLPORT")}


conn = psycopg.connect(host=conf['host'], dbname=conf['dbname'], user=conf['user'],password=conf['password'],port=conf['port'])

cur = conn.cursor()

#conn.set_session(autocommit=True)
cur.execute("""CREATE TABLE IF NOT EXISTS test(
            test_id SERIAL PRIMARY KEY,
            test_name VARCHAR(255) NOT NULL
            )
    
    """)


conn.commit()

cur.execute("INSERT INTO test (test_name) VALUES ('value2')")
conn.commit()

cur.close()

conn.close()