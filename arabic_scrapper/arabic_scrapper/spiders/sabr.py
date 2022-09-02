import scrapy
import pandas as pd
from arabic_scrapper.helper import load_dataset_lists
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("sabr",False)
now = datetime.now()

class SabrSpider(scrapy.Spider):
    name = 'sabr'
    
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="entry-title h5"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        sabr_item=GeneralItem()
        date=response.xpath('//*[@class="entry-meta entry-meta-single"]/div[2]/span/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
        print("//////////Date///////",date,type(date))
        sabr_item["news_agency_name"]="sabr"
        sabr_item["page_url"]=response.meta["page_link"]
        sabr_item["category"]=response.meta["catagory"]
        sabr_item["title"]=response.xpath('//*[@class="entry-title h1"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content herald-entry-content"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        sabr_item["contents"]=contents

        sabr_item["image_url"]=response.xpath('//*[@class="herald-post-thumbnail herald-post-thumbnail-single"]/span/img/@src').extract_first()
        sabr_item["date"]=date
        sabr_item["author_name"]="NULL"


        sabr_item["main_category"]=response.meta["main_category"]
        sabr_item["sub_category"]=response.meta["sub_category"]
        sabr_item["platform"]=response.meta["platform"]
        sabr_item["media_type"]=response.meta["media_type"]
        sabr_item["urgency"]=response.meta["urgency"]
        sabr_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        sabr_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        sabr_item["deleted_at"]=None

        yield sabr_item
