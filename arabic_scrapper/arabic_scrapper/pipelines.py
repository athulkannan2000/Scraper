# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface

import os
from dotenv import load_dotenv
import mysql.connector
try:
    from arabic_scrapper.helper import news_list,agos_changer
except ImportError:
    from helper import news_list,agos_changer #useful in twitter update
import re
load_dotenv()

class ArabicScrapperPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = os.getenv("db_password"),
            database = os.getenv("db_name")
        )

        self.cur = self.conn.cursor()

            
        ################ creating main table ###################################
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS scraped_data(
                topic_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                news_agency_name int NOT NULL,
                page_url text,
                category int NOT NULL,
                title text,
                contents text,
                image_url text,
                published_date_and_time text ,
                author_name varchar(255),
                main_category int NOT NULL,
                sub_category int NOT NULL, 
                platform varchar(255), 
                media_type varchar(255), 
                urgency varchar(255), 
                created_at text, 
                updated_at text, 
                deleted_at text,
                tweet_created_at text DEFAULT Null,
                tweet_text text DEFAULT Null,
                tweet_id text DEFAULT Null,
                vdo_title text DEFAULT Null,
                vdo_description text DEFAULT Null,
                vdo_published_at text DEFAULT Null,
                vdo_thumbnail text DEFAULT Null,
                vdo_url text DEFAULT Null,
                FOREIGN KEY (main_category) REFERENCES main_cat(id),
                FOREIGN KEY (sub_category) REFERENCES sub_cat(id),
                FOREIGN KEY (category) REFERENCES category(id),
                FOREIGN KEY (news_agency_name) REFERENCES agency(id)
            )
        """)
        
        self.main_cat = self.cur.execute("SELECT id,main_category_name_en FROM main_cat")
        self.main_cat_result = self.cur.fetchall()

        self.sub_cat = self.cur.execute("SELECT id,sub_category_name_en FROM sub_cat")
        self.sub_cat_result = self.cur.fetchall()

        self.category = self.cur.execute("SELECT id,category_name_en FROM category")
        self.category_result = self.cur.fetchall()

        self.agency = self.cur.execute("SELECT id,agency_category_name_en FROM agency")
        self.agency_result = self.cur.fetchall()
        
    def process_item(self, item, spider):

        select_stmt = "INSERT INTO scraped_data (news_agency_name, page_url, category, title, contents, image_url, published_date_and_time, author_name, main_category, sub_category, platform, media_type, urgency, created_at, updated_at, deleted_at, tweet_created_at, tweet_text, tweet_id, vdo_title, vdo_description, vdo_published_at, vdo_thumbnail, vdo_url) SELECT %(news_agency_name)s, %(page_url)s, %(category)s, %(title)s, %(contents)s, %(image_url)s, %(published_date_and_time)s, %(author_name)s,%(main_category)s,%(sub_category)s,%(platform)s,%(media_type)s,%(urgency)s,%(created_at)s,%(updated_at)s,%(deleted_at)s,%(tweet_created_at)s,%(tweet_text)s,%(tweet_id)s,%(vdo_title)s,%(vdo_description)s,%(vdo_published_at)s,%(vdo_thumbnail)s,%(vdo_url)s  FROM dual WHERE NOT EXISTS (SELECT * FROM scraped_data WHERE title = %(title)s AND page_url = %(page_url)s)"
        try:
            agency   =  [tuple[0] for id, tuple in enumerate(self.agency_result) if tuple[1] == item["news_agency_name"]][0]
            main_cat_ = [tuple[0] for id, tuple in enumerate(self.main_cat_result) if tuple[1] == item["main_category"]][0]
            sub_cat_ =  [tuple[0] for id, tuple in enumerate(self.sub_cat_result) if tuple[1] == item["sub_category"]][0]  
            catagory =  [tuple[0] for id, tuple in enumerate(self.category_result) if tuple[1] == item["category"]][0]
            self.cur.execute(select_stmt, {
                "news_agency_name": agency,
                "page_url" : item['page_url'],
                "category" : catagory,
                "title" : item["title"],
                "contents": item["contents"],
                "image_url" : item["image_url"],
                "published_date_and_time" : item["date"],
                "author_name" : item["author_name"],
                "main_category" : main_cat_, 
                "sub_category": sub_cat_, 
                "platform":item["platform"],
                "media_type":item["media_type"], 
                "urgency":item["urgency"], 
                "created_at":item["created_at"], 
                "updated_at":item["updated_at"], 
                "deleted_at":item["deleted_at"],
                "tweet_created_at":item["tweet_created_at"],
                "tweet_text": item["tweet_text"],
                "tweet_id" :  item["tweet_id"],

                "vdo_title":item["vdo_title"],
                "vdo_description":item["vdo_description"],
                "vdo_published_at":item["vdo_published_at"],
                "vdo_thumbnail":item["vdo_thumbnail"],
                "vdo_url":item["vdo_url"],
                })
            self.conn.commit()
            return item

        except KeyError: # handling key error because spiders done have data for tweets
            print("$$$$$$$$$%%%%%%%%%%%%%%%insise%%%%%%%%%%%%%%%%%%%%%%$$$$$$$$$$$$$$")
            select_stmt = "INSERT INTO scraped_data (news_agency_name, page_url, category, title, contents, image_url, published_date_and_time, author_name, main_category, sub_category, platform, media_type, urgency, created_at, updated_at, deleted_at) SELECT %(news_agency_name)s, %(page_url)s, %(category)s, %(title)s, %(contents)s, %(image_url)s, %(published_date_and_time)s, %(author_name)s,%(main_category)s,%(sub_category)s,%(platform)s,%(media_type)s,%(urgency)s,%(created_at)s,%(updated_at)s,%(deleted_at)s  FROM dual WHERE NOT EXISTS (SELECT * FROM scraped_data WHERE title = %(title)s AND page_url = %(page_url)s)"
            agency   =  [tuple[0] for id, tuple in enumerate(self.agency_result) if tuple[1] == item["news_agency_name"]][0]
            main_cat_ = [tuple[0] for id, tuple in enumerate(self.main_cat_result) if tuple[1] == item["main_category"]][0]
            sub_cat_ =  [tuple[0] for id, tuple in enumerate(self.sub_cat_result) if tuple[1] == item["sub_category"]][0]  
            catagory =  [tuple[0] for id, tuple in enumerate(self.category_result) if tuple[1] == item["category"]][0]
            self.cur.execute(select_stmt, {
                "news_agency_name": agency,
                "page_url" : item['page_url'],
                "category" : catagory,
                "title" : item["title"],
                "contents": item["contents"],
                "image_url" : item["image_url"],
                "published_date_and_time" : item["date"],
                "author_name" : item["author_name"],
                "main_category" : main_cat_, 
                "sub_category": sub_cat_, 
                "platform":item["platform"],
                "media_type":item["media_type"], 
                "urgency":item["urgency"], 
                "created_at":item["created_at"], 
                "updated_at":item["updated_at"], 
                "deleted_at":item["deleted_at"],
                })
            self.conn.commit()
            return item
            

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()