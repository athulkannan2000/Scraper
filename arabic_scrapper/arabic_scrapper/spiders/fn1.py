import scrapy
import pandas as pd
from arabic_scrapper.helper import load_dataset_lists
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Fn1",False)
now = datetime.now()


class Fn1Spider(scrapy.Spider):
    name = 'fn1'
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
        f1_item=GeneralItem()
        date = response.xpath('//*[@class="post-meta"]/span[3]/text()').extract_first()+" "+response.xpath('//*[@class="post-meta"]/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        f1_item["news_agency_name"]="Fn1"
        f1_item["page_url"]=response.meta["page_link"]
        f1_item["category"]=response.meta["catagory"]

        f1_item["title"]=response.xpath('//*[@class="name post-title entry-title"]/span/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry"]//p/strong/text()').extract()
        contents="".join(contents[0:len(contents)])
        f1_item["contents"]=contents

        f1_item["image_url"]=response.xpath('//*[@class="entry"]/p/a/img/@src').extract_first()
        f1_item["date"]=date
        f1_item["author_name"]=response.xpath('//*[@class="post-meta-author"]/a/text()').extract_first()

        f1_item["main_category"]=response.meta["main_category"]
        f1_item["sub_category"]=response.meta["sub_category"]
        f1_item["platform"]=response.meta["platform"]
        f1_item["media_type"]=response.meta["media_type"]
        f1_item["urgency"]=response.meta["urgency"]
        f1_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        f1_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        f1_item["deleted_at"]=None

        
        yield f1_item
