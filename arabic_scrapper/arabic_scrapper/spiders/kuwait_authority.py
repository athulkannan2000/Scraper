import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Kuwait Authority for Partnership Projects",False)
now = datetime.now()

class KuwaitAuthoritySpider(scrapy.Spider):
    name = 'kuwait_authority'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="news-content"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        kuwait_authority=GeneralItem()
        date = response.xpath('//*[@class="date"]/text()').extract()[1]
        print("/////Date/////",date)
        date = str(parser.parse(date)).replace("-","/")

        kuwait_authority["news_agency_name"]="Kuwait Authority for Partnership Projects"
        kuwait_authority["page_url"]=response.meta["page_link"]
        kuwait_authority["category"]=response.meta["catagory"]
        kuwait_authority["title"]=response.xpath('//*[@class="info content-box"]/h3/text()').extract_first()
        
        contents=response.xpath('//*[@class="info content-box"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        kuwait_authority["contents"]=contents

        images=response.xpath('//*[@class="thumb"]/img/@src').extract()
        images=[*set(images)] #removes duplicates
        images=" ".join(images)

        kuwait_authority["image_url"]=images
        kuwait_authority["date"]=date
        kuwait_authority["author_name"]="Kuwait Authority for Partnership Projects"

        kuwait_authority["main_category"]=response.meta["main_category"]
        kuwait_authority["sub_category"]=response.meta["sub_category"]
        kuwait_authority["platform"]=response.meta["platform"]
        kuwait_authority["media_type"]=response.meta["media_type"]
        kuwait_authority["urgency"]=response.meta["urgency"]
        kuwait_authority["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kuwait_authority["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kuwait_authority["deleted_at"]=None
        yield kuwait_authority
