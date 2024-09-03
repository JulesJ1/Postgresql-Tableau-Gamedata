import psycopg
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

"""
Configure Database using the games file into games TABLE, tags TABLE and games_tags TABLE

"""


conf = {"host":os.getenv("SQLHOST"), 
        "dbname":"gamedata", 
        "user":os.getenv("SQLUSER"),
        "password":os.getenv("SQLPASSWORD"),
        "port":os.getenv("SQLPORT")}

DATABASE_URL = f"{conf['user']}://postgres:{conf['password']}@{conf['host']}/gamedata"
conn = psycopg.connect(host=conf['host'], dbname=conf['dbname'], user=conf['user'],password=conf['password'],port=conf['port'])

cur = conn.cursor()

#conn.set_session(autocommit=True)
cur.execute("""CREATE TABLE IF NOT EXISTS tags(
        tag_id serial PRIMARY KEY,
        tag_name text UNIQUE

)""")


conn.commit()

cur.execute("""CREATE TABLE IF NOT EXISTS games_tags(
        tag_id int REFERENCES tags(tag_id),
        game_id int REFERENCES games(app_id),
        CONSTRAINT tag_game PRIMARY KEY (tag_id,game_id)
     
)""")

#Delete Duplicates
cur.execute(f"""
DELETE FROM games g1
WHERE EXISTS
(
        select null
        FROM games g2
        WHERE g2.app_id = g1.app_id
        AND g2.ctid > g1.ctid
);

""")
conn.commit()

cur.execute("""ALTER TABLE games DROP COLUMN 'Reviews D7', DROP COLUMN 'Reviews D30', DROP COLUMN 'Reviews D90'""")

conn.commit()

cur.execute("""ALTER TABLE games ADD COLUMN est_owners; UPDATE games SET est_owners = reviews_total*32
""")
conn.commit()


cur.execute(f"""WITH tagslist AS (SELECT string_to_array("Tags",',') AS names FROM games )
        INSERT INTO tags (tag_name) SELECT DISTINCT unnest("names") FROM tagslist
""")

conn.commit()

cur.execute("""WITH gt as(
            SELECT DISTINCT g.app_id,g.tags, t.tag_name,t.tag_id 
            FROM games as g
            JOIN tags as t 
            ON g.tags LIKE '%' || t.tag_name || '%')
          INSERT INTO games_tags (game_id,tag_id) SELECT app_id,tag_id FROM gt
          """)
conn.commit()

cur.close()

conn.close()



#engine = create_engine(f"DATABASE_URL")
#df = pd.read_csv("datasets/Steam-Trends-2023 - Games Data.csv")
#df.to_sql("games",engine,if_exists="fail",index=False)