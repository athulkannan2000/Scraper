import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Directorate General of Civil Aviation",False)
now = datetime.now()

class DirectorateGeneralCivilAviationSpider(scrapy.Spider):
    name = 'Directorate_General_Civil_Aviation'

    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="ms-3 mb-4"]/a/@href').extract()
        date=response.xpath('//*[@class="fs-5 fw-400 text-black"]/text()').extract() #date is present in the outside page
        date=[str(parser.parse(i)).replace("-","/") for i in date]
        print("/////////////news links//////////",news_links,date)
        for link,date in zip(news_links,date):
            link="https://www.dgca.gov.kw"+link
            print("////////link//////////////",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'date':date,'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
                
    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        Directorate_General_item=GeneralItem()

        Directorate_General_item["news_agency_name"]="Directorate General of Civil Aviation"
        Directorate_General_item["page_url"]=response.meta["page_link"]
        Directorate_General_item["category"]=response.meta["catagory"]
        Directorate_General_item["title"]=response.xpath('//*[@class="mb-4 text-center fs-5 fw-500"]/strong/text()').extract_first()
        
        contents=response.xpath('//*[@class="col-lg-12 mt-4 mb-5"]//p/span/text()').extract()+response.xpath('//*[@class="col-lg-12 mt-4 mb-5"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        Directorate_General_item["contents"]=contents

        images=response.xpath('//*[@class="col-lg-12 mt-4 mb-5"]/a/@href').extract_first()
        Directorate_General_item["image_url"]=images
        Directorate_General_item["date"]=response.meta["date"]
        Directorate_General_item["author_name"]="Directorate General of Civil Aviation"

        Directorate_General_item["main_category"]=response.meta["main_category"]
        Directorate_General_item["sub_category"]=response.meta["sub_category"]
        Directorate_General_item["platform"]=response.meta["platform"]
        Directorate_General_item["media_type"]=response.meta["media_type"]
        Directorate_General_item["urgency"]=response.meta["urgency"]
        Directorate_General_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Directorate_General_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Directorate_General_item["deleted_at"]=None

        yield Directorate_General_item




