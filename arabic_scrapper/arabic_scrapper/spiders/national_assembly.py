import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("National Assembly of Kuwait",False)
now = datetime.now()

class NationalAssemblySpider(scrapy.Spider):
    name = 'national_assembly'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//table[1]//td/font/a/@href').extract()
        date=response.xpath('//table[1]//div/font/text()').extract() #date is present in the outside page
        date=[str(parser.parse(i)).replace("-","/") for i in date]
        print("//////news_links/////////",news_links,date,len(news_links),len(date))
        for link,date in zip(news_links,date):
            link="http://www.kna.kw/clt-html5/"+link
            print("////////link//////////////",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  

                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'date':date,'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
                
    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        national_assembly=GeneralItem()

        national_assembly["news_agency_name"]="National Assembly of Kuwait"
        national_assembly["page_url"]=response.meta["page_link"]
        national_assembly["category"]=response.meta["catagory"]
        national_assembly["title"]=response.xpath('//*[@class="text-uppercase"][1]/text()').extract_first()
        
        contents=response.xpath('//div//b/font/text()').extract()
        contents="".join(contents[0:len(contents)])
        national_assembly["contents"]=contents

        images="http://www.kna.kw/"+response.xpath('//li/a/img/@src').extract_first()

        national_assembly["image_url"]=images
        national_assembly["date"]=response.meta["date"]
        national_assembly["author_name"]="National Assembly of Kuwait"

        national_assembly["main_category"]=response.meta["main_category"]
        national_assembly["sub_category"]=response.meta["sub_category"]
        national_assembly["platform"]=response.meta["platform"]
        national_assembly["media_type"]=response.meta["media_type"]
        national_assembly["urgency"]=response.meta["urgency"]
        national_assembly["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        national_assembly["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        national_assembly["deleted_at"]=None

        yield national_assembly





