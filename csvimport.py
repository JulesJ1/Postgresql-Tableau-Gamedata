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

conn.set_session(autocommit=True)
cur.execute("CREATE DATABASE gamedata")


conn.commit()

cur.close()

conn.close()


engine = create_engine(f"{conf['user']}://postgres:{conf['password']}@{conf['host']}/gamedata")


df = pd.read_csv("datasets/Steam-Trends-2023 - Games Data.csv")
df.to_sql("games",engine,if_exists="fail",index=False)