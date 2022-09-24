import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alerada news")
now = datetime.now()


class AleradaSpider(scrapy.Spider):
    name = 'alerada'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            #print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links=response.xpath('//*[@class="post-title"]/a/@href').extract()
        #print("/////////////news links//////////",news_links,"//////////length of news links /////////////",len(news_links))
        for link in news_links:
            link="https://alerada.net"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        alerada_item=GeneralItem()
        date = response.xpath('//*[@class="date meta-item tie-icon"]/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        alerada_item["news_agency_name"]="alerada news"
        alerada_item["page_url"]=response.meta["page_link"]
        alerada_item["category"]=response.meta["catagory"]
        alerada_item["title"]=response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content entry clearfix"]//p/text()').extract()+response.xpath('//*[@class="entry-content entry clearfix"]//div/text()').extract()
        contents="".join(contents[0:len(contents)])
        alerada_item["contents"]=contents

        alerada_item["image_url"]="https://alerada.net"+response.xpath('//*[@class="single-featured-image"]//img/@src').extract_first()
        alerada_item["date"]=date
        alerada_item["author_name"]="alerada news"

        alerada_item["main_category"]=response.meta["main_category"]
        alerada_item["sub_category"]=response.meta["sub_category"]
        alerada_item["platform"]=response.meta["platform"]
        alerada_item["media_type"]=response.meta["media_type"]
        alerada_item["urgency"]=response.meta["urgency"]
        alerada_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alerada_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alerada_item["deleted_at"]=None

        yield alerada_item