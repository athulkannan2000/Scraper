import os
import re
import pandas as pd
import mysql.connector 
from dotenv import load_dotenv

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
            main_category_name_ar text
            );
            """)
cur.execute("""create table IF NOT EXISTS sub_cat(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            sub_category_name_en varchar(255),
            sub_category_name_ar text
            );
            """)
cur.execute("""create table IF NOT EXISTS category(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            category_name_en varchar(255),
            category_name_ar text
            );
            """)
cur.execute("""create table IF NOT EXISTS agency(
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            agency_category_name_en varchar(255),
            agency_category_name_ar text
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

def id_helper(cat,keyval,removed):
        fin="_".join([i[:keyval] for i in removed])
        exist= identifiers.values()
        if fin not in exist:
            return "_".join([i[:keyval] for i in removed])
        else:
            id_helper(cat,keyval+1,removed)  

def identifier_generator(col,keyval):
        exp="[A-Za-z0-9]+"
        # print("from function",col)
        for cat in col:
            removed = re.findall(exp,cat) #finding only words
            fin="_".join([i[:keyval] for i in removed])
            exist= identifiers.values()
            if fin not in exist:
                fin="_".join([i[:keyval] for i in removed])
                identifiers[cat]=fin
            else:
                identifiers[cat] = id_helper(cat,keyval+1,removed)
        return identifiers      



 ############### encoding the values and inserting in reference table correspondingly ###################################
identifiers={}
main_cat=identifier_generator(main_cat_en,3)
for en,ar,identifier in zip(main_cat_en,main_cat_ar,main_cat.values()):
    select_stmt = "INSERT INTO main_cat (main_category_name_en,main_category_name_ar) SELECT %(category_name_en)s, %(category_name_ar)s FROM dual WHERE NOT EXISTS (SELECT * FROM main_cat WHERE main_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar
    })
    conn.commit()


identifiers={} 
sub_cat=identifier_generator(sub_cat_en,3)
for en,ar,identifier in zip(sub_cat_en,sub_cat_ar,sub_cat.values()):
    select_stmt = "INSERT INTO sub_cat (sub_category_name_en,sub_category_name_ar) SELECT  %(category_name_en)s, %(category_name_ar)s FROM dual WHERE NOT EXISTS (SELECT * FROM sub_cat WHERE sub_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar
    })
    conn.commit()

identifiers={}
category=identifier_generator(cat_en,3)
for en,ar,identifier in zip(cat_en,cat_ar,category.values()):
    select_stmt = "INSERT INTO category (category_name_en,category_name_ar) SELECT  %(category_name_en)s, %(category_name_ar)s FROM dual WHERE NOT EXISTS (SELECT * FROM category WHERE category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar
    })
    conn.commit()

identifiers={}
agency=identifier_generator(agency_en,3)
for en,ar,identifier in zip(agency_en,agency_ar,agency.values()):
    select_stmt = "INSERT INTO agency (agency_category_name_en,agency_category_name_ar) SELECT  %(category_name_en)s, %(category_name_ar)s FROM dual WHERE NOT EXISTS (SELECT * FROM agency WHERE agency_category_name_en = %(category_name_en)s)"
    cur.execute(select_stmt,{
        "category_name_en": en,
        "category_name_ar": ar
    })
    conn.commit()










