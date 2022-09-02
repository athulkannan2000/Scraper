import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists,agos_changer
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("hmak news",False)
now = datetime.now()

class HmakSpider(scrapy.Spider):
    name = 'hmak'
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
        hmak_item=GeneralItem()
        date = response.xpath('//*[@id="single-post-meta"]/span[2]/text()').extract_first()
        print("/////////////////////////",date)
        date=agos_changer(date) #used to change 5 mins/week/day/month/seconds/year ago to exact date and time
      
        hmak_item["news_agency_name"]="hmak news"
        hmak_item["page_url"]=response.meta["page_link"]
        hmak_item["category"]=response.meta["catagory"]
        hmak_item["title"]=response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content entry clearfix"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        hmak_item["contents"]=contents

        hmak_item["image_url"]=response.xpath('//*[@class="single-featured-image"]/img/@src').extract_first()
        hmak_item["date"]=date
        hmak_item["author_name"]=response.xpath('//*[@class="meta-author"]/a/font/font/text()').extract_first()

        hmak_item["main_category"]=response.meta["main_category"]
        hmak_item["sub_category"]=response.meta["sub_category"]
        hmak_item["platform"]=response.meta["platform"]
        hmak_item["media_type"]=response.meta["media_type"]
        hmak_item["urgency"]=response.meta["urgency"]
        hmak_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        hmak_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        hmak_item["deleted_at"]=None

        yield hmak_item
