from hashlib import new
import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
import json 
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("nokhbah news",False)
now = datetime.now()

class Fn1Spider(scrapy.Spider):
    name = 'nokhbah'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//h2[@class="title"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        nokhbah_news=GeneralItem()
        date = response.xpath('//time[@class="post-published updated"]/b/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        nokhbah_news["news_agency_name"]="nokhbah news"
        nokhbah_news["page_url"]=response.meta["page_link"]
        nokhbah_news["category"]=response.meta["catagory"]

        nokhbah_news["title"]=response.xpath('//h1[@class="single-post-title"]/span/text()').extract_first()
        
        contents=response.xpath('//div[@class="entry-content clearfix single-post-content"]/p/strong/text()').extract()+response.xpath('//div[@class="entry-content clearfix single-post-content"]/p/text()').extract()+response.xpath('//div[@id="pastingspan1"]/text()').extract()+response.xpath('//section[@id="paragraphs"]/p/text()').extract()+response.xpath('//span[@id="divArtContent"]/text()').extract()+response.xpath('//span[@id="divArtContent"]/p/text()').extract()+response.xpath('//div[@id="maincontent"]/p/text()').extract()+response.xpath('//div[@class="entry-content clearfix single-post-content"]/p/strong/span/text()').extract()
        print("/////////////contents//////////////",contents,type(contents))
        contents="".join(contents[0:len(contents)])
        nokhbah_news["contents"]=contents

        nokhbah_news["image_url"]= response.xpath('//div[@class="single-featured"]/a/img/@data-src').extract_first()
        print("\n nokhbah_news['image_url']", nokhbah_news["image_url"])
        nokhbah_news["date"]=date
        nokhbah_news["author_name"]= "النخبة الإخبارية"

        nokhbah_news["main_category"]=response.meta["main_category"]
        nokhbah_news["sub_category"]=response.meta["sub_category"]
        nokhbah_news["platform"]=response.meta["platform"]
        nokhbah_news["media_type"]=response.meta["media_type"]
        nokhbah_news["urgency"]=response.meta["urgency"]
        nokhbah_news["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        nokhbah_news["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        nokhbah_news["deleted_at"]=None
        yield nokhbah_news
        yield nokhbah_news


