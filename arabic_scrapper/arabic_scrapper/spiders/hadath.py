
from hashlib import new
import scrapy
import pandas as pd
# from tutorial.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists

from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("hadath ",False)
now = datetime.now()

class HadathSpider(scrapy.Spider):
    name = 'hadath'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//div[@class="img-fluidStyle"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link = "https://hadathkw.net"+link[2:]
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        hadath=GeneralItem()
        date = response.xpath('//*[@class="marginTop20"]/small/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        hadath["news_agency_name"]="hadath "
        hadath["page_url"]=response.meta["page_link"]
        hadath["category"]=response.meta["catagory"]

        hadath["title"]=response.xpath('//*[@class="newsTitle marginTop20"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="newsText"]/div/text()').extract()
        contents="".join(contents[0:len(contents)])
       
        hadath["contents"]=contents

        hadath["image_url"]="https://hadathkw.net"+response.xpath('//div[@class="innerContent "]/img/@src').extract_first()
        hadath["date"]=date
        hadath["author_name"]="	جريدة حدث الالكترونية"

        hadath["main_category"]=response.meta["main_category"]
        hadath["sub_category"]=response.meta["sub_category"]
        hadath["platform"]=response.meta["platform"]
        hadath["media_type"]=response.meta["media_type"]
        hadath["urgency"]=response.meta["urgency"]
        hadath["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        hadath["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        hadath["deleted_at"]=None
        yield hadath