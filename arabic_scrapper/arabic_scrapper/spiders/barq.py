import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("barq news",False)
now = datetime.now()


class BarqSpider(scrapy.Spider):
    name = 'barq'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):

        pinned_news=response.xpath('//li[@class="c1"]//a/@href').extract()
        listed_news = response.xpath('//li[@class="c2 four-posts "]/a/@href').extract()
        banner_news = response.xpath('//li[@class="c2 titled-posts col-sm-4  five-post "]/a/@href').extract()+response.xpath('//li[@class="c2 titled-posts col-sm-4 "]/a/@href').extract()
        news_links=pinned_news+listed_news+banner_news
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        barq_item=GeneralItem()
        date = response.xpath('//*[@class="ptopic-title"]/span/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        barq_item["news_agency_name"]= "barq news"
        barq_item["page_url"]=response.meta["page_link"]
        barq_item["category"]=response.meta["catagory"]
        barq_item["title"]=response.xpath('//*[@class="ptopic-title"]/h3/text()').extract_first()
        
        contents=response.xpath('//*[@class="ptopic-body"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        barq_item["contents"]=contents

        barq_item["image_url"]=response.xpath('//*[@class="ptopic-title"]/img/@src').extract_first()
        barq_item["date"]=date
        barq_item["author_name"]="barq news"

        barq_item["main_category"]=response.meta["main_category"]
        barq_item["sub_category"]=response.meta["sub_category"]
        barq_item["platform"]=response.meta["platform"]
        barq_item["media_type"]=response.meta["media_type"]
        barq_item["urgency"]=response.meta["urgency"]
        barq_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        barq_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        barq_item["deleted_at"]=None
        yield barq_item
