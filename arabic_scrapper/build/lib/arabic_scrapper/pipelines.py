# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface


from itemadapter import ItemAdapter
import mysql.connector
import re

class ArabicScrapperPipeline:
    def __init__(self):
        self.conn=mysql.connector.connect(host="localhost",user="root",passwd="root",database="scrapper_db")
        self.curr=self.conn.cursor()

        
    def process_item(self, item, spider):
        spider=re.findall(r"'(.*?)'", str(spider))[0]
        print("/////////////////////Spider//////////////",spider)

        if spider=="alanba_scrapper":
            self.curr.execute("""CREATE TABLE IF NOT EXISTS alanba_news(
            topic_id varchar(255),
            news_agency_name varchar(255),
            page_url text,
            category varchar(255),
            title text,
            contents text,
            image_url text,
            published_date_and_time text ,
            author_name varchar(255))""")
            query=f"insert into alanba_news values ('{item['topic_id']}','{item['news_agency_name']}','{item['page_url']}','{item['category']}','{item['title']}','{item['contents']}','{item['image_url']}','{item['date']}','Null');"

            self.curr.execute(query)
            self.conn.commit()

        elif spider=="alrai":
            self.curr.execute("""CREATE TABLE IF NOT EXISTS alrai_news(
            topic_id varchar(255),
            news_agency_name varchar(255),
            page_url text,
            category varchar(255),
            title text,
            contents text,
            image_url text,
            published_date_and_time text ,
            author_name varchar(255))""")
            query=f"insert into alrai_news values ('{item['topic_id']}','{item['news_agency_name']}','{item['page_url']}','{item['category']}','{item['title']}','{item['contents']}','{item['image_url']}','{item['date']}','Null');"

            self.curr.execute(query)
            self.conn.commit()
    
    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.curr.close()
        self.conn.close()

