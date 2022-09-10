import os
import pandas as pd
import mysql.connector 
from dotenv import load_dotenv
from slugify import slugify

################### for verifying cron job####################
from datetime import datetime
now=datetime.now()
with open('/tmp/cron_log.txt',"a") as f:
    f.write("cornjob of db_update started at {} \n".format(now))
#############################################################

load_dotenv()


conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = os.getenv("db_password"),
            database = os.getenv("db_name")
        )

cur = conn.cursor()

###################creating reference tables###################
cur.execute("""create table IF NOT EXISTS main_cat(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            main_category_name_en varchar(255),
            main_category_name_ar text,
            main_category_identifier varchar(255),
            status int DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)

cur.execute("""create table IF NOT EXISTS sub_cat(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            sub_category_name_en varchar(255),
            sub_category_name_ar text,
            sub_category_identifier varchar(255),
            status int DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)

cur.execute("""create table IF NOT EXISTS category(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            category_name_en varchar(255),
            category_name_ar text,
            category_identifier varchar(255),
            status int DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)

cur.execute("""create table IF NOT EXISTS agency(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            agency_name_en varchar(255),
            agency_name_ar text,
            agency_identifier varchar(255),
            status int DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)

cur.execute("""create table IF NOT EXISTS source(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            platform_name_en varchar(255),
            platform_name_ar text,
            platform_identifier varchar(255),
            status int DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)


df =  pd.read_csv(os.getenv("news_sites_list"))

#main category
main_cat_en =  df["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
main_cat_ar =  df["Main Category AR"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()


#sub category
sub_cat_en = df["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
sub_cat_ar = df["Sub Category AR"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()

#category
cat_en = df["Category -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
cat_ar = df["Category - AR"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()

#agency
agency_en = df["News Agency in English"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
agency_ar = df["News Agency in arabic"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()

#platform
platform_en = df["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
platform_ar = df["Platform - AR"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()
   

################################ inserting in reference table correspondingly ###################################

for en,ar in zip(main_cat_en,main_cat_ar):
    select_stmt = "INSERT INTO main_cat (main_category_name_en,main_category_name_ar,main_category_identifier) SELECT %(category_name_en)s, %(category_name_ar)s,%(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM main_cat WHERE main_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(sub_cat_en,sub_cat_ar):
    select_stmt = "INSERT INTO sub_cat (sub_category_name_en,sub_category_name_ar,sub_category_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM sub_cat WHERE sub_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(cat_en,cat_ar):
    select_stmt = "INSERT INTO category (category_name_en,category_name_ar,category_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM category WHERE category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(agency_en,agency_ar):
    select_stmt = "INSERT INTO agency(agency_name_en,agency_name_ar,agency_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM agency WHERE agency_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(platform_en,platform_ar):
    select_stmt = "INSERT INTO source (platform_name_en,platform_name_ar,platform_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM source WHERE platform_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()










