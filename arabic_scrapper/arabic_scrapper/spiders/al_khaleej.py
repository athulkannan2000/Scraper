import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("al-khaleej",False)
now = datetime.now()

class AlKhaleejSpider(scrapy.Spider):
    name = 'al-khaleej'

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
        alkhaleej_item=GeneralItem()
        date = response.xpath('//*[@class="post-meta-info"]/li[2]/text()').extract()
        date = date[1][21:-20]
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        alkhaleej_item["news_agency_name"]= "al-khaleej"
        alkhaleej_item["page_url"]=response.meta["page_link"]
        alkhaleej_item["category"]=response.meta["catagory"]
        alkhaleej_item["title"]=response.xpath('//*[@class="post-title lg"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content clearfix"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        alkhaleej_item["contents"]=contents
        alkhaleej_item["image_url"]=response.xpath('//*[@class="post-content-area"]/div/img/@src').extract_first()
        alkhaleej_item["date"]=date
        alkhaleej_item["author_name"]="alkhaleej"
        
        alkhaleej_item["main_category"]=response.meta["main_category"]
        alkhaleej_item["sub_category"]=response.meta["sub_category"]
        alkhaleej_item["platform"]=response.meta["platform"]
        alkhaleej_item["media_type"]=response.meta["media_type"]
        alkhaleej_item["urgency"]=response.meta["urgency"]
        alkhaleej_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alkhaleej_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alkhaleej_item["deleted_at"]=None

        yield alkhaleej_item