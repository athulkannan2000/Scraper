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
            user = 'root',
            host=os.getenv("db_host"),
            password = os.getenv("db_password"),
            database = os.getenv("db_name")
        )

cur = conn.cursor()

###################creating reference tables###################
cur.execute("""create table IF NOT EXISTS main_categories(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            main_category_name_en varchar(255),
            main_category_name_ar text,
            main_category_identifier varchar(255),
            status int DEFAULT 0,
            created_at TIMESTAMP NULL DEFAULT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
            );
            """)

cur.execute("""create table IF NOT EXISTS sub_categories(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            category_id int NOT NULL,
            sub_category_name_en varchar(255),
            sub_category_name_ar text,
            sub_category_identifier varchar(255),
            status int DEFAULT 0,
            created_at TIMESTAMP NULL DEFAULT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
            );
            """)

cur.execute("""create table IF NOT EXISTS categories(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            category_name_en varchar(255),
            category_name_ar text,
            category_identifier varchar(255),
            status int DEFAULT 0,
            created_at TIMESTAMP NULL DEFAULT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
            );
            """)

cur.execute("""create table IF NOT EXISTS agencies(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            agency_name_en varchar(255),
            agency_name_ar text,
            agency_identifier varchar(255),
            status int DEFAULT 0,
            created_at TIMESTAMP NULL DEFAULT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
            );
            """)

cur.execute("""create table IF NOT EXISTS sources(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            platform_name_en varchar(255),
            platform_name_ar text,
            platform_identifier varchar(255),
            status int DEFAULT 0,
            created_at TIMESTAMP NULL DEFAULT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
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
    select_stmt = "INSERT INTO main_categories (main_category_name_en,main_category_name_ar,main_category_identifier) SELECT %(category_name_en)s, %(category_name_ar)s,%(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM main_categories WHERE main_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


main_cat_ref = cur.execute("SELECT id,main_category_name_en FROM main_categories")
main_cat_result = cur.fetchall()

for en,ar in zip(sub_cat_en,sub_cat_ar):

    map_main_to_sub_category = df.loc[df["Sub Category En"] == en]["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).unique().tolist()[0]
    
    main_cat_ = [tuple[0] for id, tuple in enumerate(main_cat_result) if tuple[1] == map_main_to_sub_category][0]

    select_stmt = "INSERT INTO sub_categories (category_id,sub_category_name_en,sub_category_name_ar,sub_category_identifier) SELECT  %(category_id)s, %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM sub_categories WHERE sub_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_id": main_cat_,
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(cat_en,cat_ar):
    select_stmt = "INSERT INTO categories (category_name_en,category_name_ar,category_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM categories WHERE category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(agency_en,agency_ar):
    select_stmt = "INSERT INTO agencies (agency_name_en,agency_name_ar,agency_identifier) SELECT  %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM agencies WHERE agency_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()


for en,ar in zip(platform_en,platform_ar):
    select_stmt = "INSERT INTO sources (platform_name_en,platform_name_ar,platform_identifier) SELECT %(category_name_en)s, %(category_name_ar)s, %(category_identifier)s FROM dual WHERE NOT EXISTS (SELECT * FROM sources WHERE platform_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar,
        "category_identifier": slugify(en)
    })
    conn.commit()










