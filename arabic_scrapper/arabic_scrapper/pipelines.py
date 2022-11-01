# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface
import os
from dotenv import load_dotenv
import mysql.connector
from arabic_scrapper.helper import parser_parse_isoformat
try:
    from arabic_scrapper.helper import news_list,agos_changer
except ImportError:
    from helper import news_list,agos_changer #useful in twitter update
import re
load_dotenv()

exp='[\u0627-\u064a0-9A-Za-z]+' #regular expression to take only arabic and english words for slugging ,using RE because sluggify library  doesn't work with arabic


class ArabicScrapperPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            user = 'root',
            password = os.getenv("db_password"),
            database = os.getenv("db_name")
        )

        self.cur = self.conn.cursor()

            
        ################ creating main table ###################################
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS posts(
                post_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                title text,
                agency_id int NOT NULL,
                post_url text,
                category_id int NOT NULL,
                image_url text,
                contents text,
                published_date_and_time text ,
                author_name varchar(255),
                main_category_id int NOT NULL,
                sub_category_id int NOT NULL, 
                media_type text, 
                slug text,
                urgency varchar(255), 
                source_id int, 
                status int DEFAULT 0,
                
                created_at TIMESTAMP NULL DEFAULT NULL, 
                updated_at TIMESTAMP NULL DEFAULT NULL, 
                deleted_at TIMESTAMP NULL DEFAULT NULL,
                tweet_created_at TIMESTAMP NULL DEFAULT NULL,
                tweet_text text DEFAULT NULL,
                tweet_id text DEFAULT NULL,
                vdo_title text DEFAULT NULL,
                vdo_description text DEFAULT NULL,
                vdo_published_at TIMESTAMP NULL DEFAULT NULL,
                vdo_thumbnail text DEFAULT NULL,
                vdo_url text DEFAULT NULL,

                active_id int DEFAULT NULL,
                trending_count varchar(255) DEFAULT NULL,
                
                FOREIGN KEY (main_category_id) REFERENCES main_categories(id),
                FOREIGN KEY (sub_category_id) REFERENCES sub_categories(id),
                FOREIGN KEY (category_id) REFERENCES categories(id),
                FOREIGN KEY (agency_id) REFERENCES agencies(id),
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        
        self.main_cat = self.cur.execute("SELECT id,main_category_name_en FROM main_categories")
        self.main_cat_result = self.cur.fetchall()

        self.sub_cat = self.cur.execute("SELECT id,sub_category_name_en FROM sub_categories")
        self.sub_cat_result = self.cur.fetchall()

        self.category = self.cur.execute("SELECT id,category_name_en FROM categories")
        self.category_result = self.cur.fetchall()

        self.agency = self.cur.execute("SELECT id,agency_name_en FROM agencies")
        self.agency_result = self.cur.fetchall()

        self.source = self.cur.execute("select id,platform_name_en from sources")
        self.source_result = self.cur.fetchall()
        
    def process_item(self, item, spider):
        
        select_stmt = "INSERT INTO posts (agency_id, post_url, category_id, title, contents, image_url, published_date_and_time, author_name, main_category_id, sub_category_id, source_id, media_type, urgency, created_at, updated_at, deleted_at, tweet_created_at, tweet_text, tweet_id, vdo_title, vdo_description, vdo_published_at, vdo_thumbnail, vdo_url, slug) SELECT %(agency_id)s, %(post_url)s, %(category_id)s, %(title)s, %(contents)s, %(image_url)s, %(published_date_and_time)s, %(author_name)s,%(main_category_id)s,%(sub_category_id)s,%(source_id)s,%(media_type)s,%(urgency)s,%(created_at)s,%(updated_at)s,%(deleted_at)s,%(tweet_created_at)s,%(tweet_text)s,%(tweet_id)s,%(vdo_title)s,%(vdo_description)s,%(vdo_published_at)s,%(vdo_thumbnail)s,%(vdo_url)s,%(slug)s  FROM dual WHERE NOT EXISTS (SELECT * FROM posts WHERE title = %(title)s AND post_url = %(post_url)s)"
        try:
            agency   =  [tuple[0] for id, tuple in enumerate(self.agency_result) if tuple[1] == item["news_agency_name"]][0]
            main_cat_ = [tuple[0] for id, tuple in enumerate(self.main_cat_result) if tuple[1] == item["main_category"]][0]
            sub_cat_ =  [tuple[0] for id, tuple in enumerate(self.sub_cat_result) if tuple[1] == item["sub_category"]][0]  
            catagory =  [tuple[0] for id, tuple in enumerate(self.category_result) if tuple[1] == item["category"]][0]
            source_ =  [tuple[0] for id, tuple in enumerate(self.source_result) if tuple[1] == item["platform"]][0]
            self.cur.execute(select_stmt, {
                "agency_id": agency,
                "post_url" : item['page_url'],
                "category_id" : catagory,
                "title" : item["title"],
                "contents": item["contents"],
                "image_url" : item["image_url"],
                "published_date_and_time" : parser_parse_isoformat(item["date"]),
                "author_name" : item["author_name"],
                "main_category_id" : main_cat_, 
                "sub_category_id": sub_cat_, 
                "source_id" : source_,
                "media_type" : item["media_type"], 
                "urgency" : item["urgency"], 
                "created_at" : item["created_at"], 
                "updated_at" : item["updated_at"], 
                "deleted_at" : item["deleted_at"],
                "tweet_created_at" :item["tweet_created_at"],
                "tweet_text" : item["tweet_text"],
                "tweet_id" :  item["tweet_id"],

                "vdo_title" : item["vdo_title"],
                "vdo_description" : item["vdo_description"],
                "vdo_published_at" : item["vdo_published_at"],
                "vdo_thumbnail" : item["vdo_thumbnail"],
                "vdo_url" : item["vdo_url"],

                "slug" : "-".join(re.findall(exp,item["title"]))
                })
            self.conn.commit()
            return item

        except KeyError: # handling key error because spiders done have data for tweets

            select_stmt = "INSERT INTO posts (agency_id, post_url, category_id, title, contents, image_url, published_date_and_time, author_name, main_category_id, sub_category_id, source_id, media_type, urgency, created_at, updated_at, deleted_at, slug) SELECT %(agency_id)s, %(post_url)s, %(category_id)s, %(title)s, %(contents)s, %(image_url)s, %(published_date_and_time)s, %(author_name)s,%(main_category_id)s,%(sub_category_id)s,%(source_id)s,%(media_type)s,%(urgency)s,%(created_at)s,%(updated_at)s,%(deleted_at)s,%(slug)s  FROM dual WHERE NOT EXISTS (SELECT * FROM posts WHERE title = %(title)s AND post_url = %(post_url)s)"

            agency   =  [tuple[0] for id, tuple in enumerate(self.agency_result) if tuple[1] == item["news_agency_name"]][0]
            main_cat_ = [tuple[0] for id, tuple in enumerate(self.main_cat_result) if tuple[1] == item["main_category"]][0]
            sub_cat_ =  [tuple[0] for id, tuple in enumerate(self.sub_cat_result) if tuple[1] == item["sub_category"]][0]  
            catagory =  [tuple[0] for id, tuple in enumerate(self.category_result) if tuple[1] == item["category"]][0]
            source_ =  [tuple[0] for id, tuple in enumerate(self.source_result) if tuple[1] == item["platform"]][0]
            self.cur.execute(select_stmt, {
                "agency_id": agency,
                "post_url" : item['page_url'],
                "category_id" : catagory,
                "title" : item["title"],
                "contents" : item["contents"],
                "image_url" : item["image_url"],
                "published_date_and_time" : parser_parse_isoformat(item["date"]),
                "author_name" : item["author_name"],
                "main_category_id" : main_cat_, 
                "sub_category_id" : sub_cat_, 
                "source_id" : source_,
                "media_type" : item["media_type"], 
                "urgency" : item["urgency"], 
                "created_at" : item["created_at"], 
                "updated_at" : item["updated_at"], 
                "deleted_at" : item["deleted_at"],
                "slug" : "-".join(re.findall(exp,item["title"]))
                })
            self.conn.commit()
            return item
            

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()