import scrapy
import pandas as pd
import xmltodict
import requests
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from deep_translator import GoogleTranslator


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Alrai",False)
now = datetime.now()


class AlraiSpider(scrapy.Spider):
    name = 'alrai'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})
            
    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="container-details-text fontsize14height44"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        alrai_item=GeneralItem()
        date =str(response.xpath('//*[@class="article-date"]/text()').extract_first())
        time=str(response.xpath('//*[@class="article-time"]/text()').extract_first())
        date=date+" "+time
        date = GoogleTranslator(source='auto', target='en').translate(date)
        date = str(parser.parse(date))
        
        
      
        alrai_item["news_agency_name"]="alrai"
        alrai_item["page_url"]=response.meta["page_link"]
        alrai_item["category"]=response.meta["catagory"]
        alrai_item["title"]=response.xpath('//*[@class="article-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="article-desc"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        alrai_item["contents"]=contents
        alrai_item["image_url"]=response.xpath('//*[@class="layout-ratio"]/img/@src').extract_first()
        alrai_item["date"]=date
        alrai_item["author_name"]="alrai"
        
        alrai_item["main_category"]=response.meta["main_category"]
        alrai_item["sub_category"]=response.meta["sub_category"]
        alrai_item["platform"]=response.meta["platform"]
        alrai_item["media_type"]=response.meta["media_type"]
        alrai_item["urgency"]=response.meta["urgency"]
        alrai_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alrai_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alrai_item["deleted_at"]=None

        yield alrai_item