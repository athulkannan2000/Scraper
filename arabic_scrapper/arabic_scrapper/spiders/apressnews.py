import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
import json 
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("apressnews",False)
now = datetime.now()

class ApressnewsSpider(scrapy.Spider):
    name = 'apressnews'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="post-title"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        apressnews_item=GeneralItem()
        date = response.xpath('//*[@class="date meta-item tie-icon"]/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        apressnews_item["news_agency_name"]="apressnews"
        apressnews_item["page_url"]=response.meta["page_link"]
        apressnews_item["category"]=response.meta["catagory"]
        apressnews_item["title"]=response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content entry clearfix"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        apressnews_item["contents"]=contents

        apressnews_item["image_url"]=response.xpath('//*[@class="single-featured-image"]/img/@src').extract_first()
        apressnews_item["date"]=date
        apressnews_item["author_name"]="apressnews"
        
        apressnews_item["main_category"]=response.meta["main_category"]
        apressnews_item["sub_category"]=response.meta["sub_category"]
        apressnews_item["platform"]=response.meta["platform"]
        apressnews_item["media_type"]=response.meta["media_type"]
        apressnews_item["urgency"]=response.meta["urgency"]
        apressnews_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        apressnews_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        apressnews_item["deleted_at"]=None

        yield apressnews_item
