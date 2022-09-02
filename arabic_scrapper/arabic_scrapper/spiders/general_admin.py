import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("General Administration of Customs",False)
now = datetime.now()

class GeneralAdminSpider(scrapy.Spider):
    name = 'general_admin'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency):  
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="col-md-10 md-margin-bottom-40"]//p/b/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://www.customs.gov.kw/"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        general_admin_item=GeneralItem()
        date = response.xpath('//*[@class="row margin-bottom-40"]/div/p[2]/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
        general_admin_item["news_agency_name"]="General Administration of Customs"
        general_admin_item["page_url"]=response.meta["page_link"]
        general_admin_item["category"]=response.meta["catagory"]
        general_admin_item["title"]=response.xpath('//*[@class="row margin-bottom-40"]/div/p[1]/text()').extract_first()
        
        contents=response.xpath('//*[@class="row margin-bottom-40"]/div/p[3]/text()').extract()
        contents="".join(contents[0:len(contents)])
        general_admin_item["contents"]=contents

        general_admin_item["image_url"]="https://www.customs.gov.kw"+str(response.xpath('//*[@id="newsImage"]/@src').extract_first())
        general_admin_item["date"]=date
        general_admin_item["author_name"]=response.xpath('//*[@class="meta-author"]/a/font/font/text()').extract_first()

        general_admin_item["main_category"]=response.meta["main_category"]
        general_admin_item["sub_category"]=response.meta["sub_category"]
        general_admin_item["platform"]=response.meta["platform"]
        general_admin_item["media_type"]=response.meta["media_type"]
        general_admin_item["urgency"]=response.meta["urgency"]
        general_admin_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        general_admin_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        general_admin_item["deleted_at"]=None
        yield general_admin_item

