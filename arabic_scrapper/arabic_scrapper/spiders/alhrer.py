import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alhrernews",False)
now = datetime.now()

class AlhrerSpider(scrapy.Spider):
    name = 'alhrer'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.pagination_handler,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.pagination_handler,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})


    def pagination_handler(self,response):
        # print("//////////////self.urls///////////",self.urls)
        urls=[]
        max_page = response.xpath('//*[@class="pagination"]//a/@href').extract()
        max_page = max_page[len(max_page)-1]
        max_page = max_page.split("/")
        print("///////////////Max page link//////////////////",max_page)
        max_page = max_page[len(max_page)-1]
        print("///////////////Max number of pages//////////////////",max_page)
        for page_num in range(1,int(max_page)+1):
            page=response.meta["current_url"]+f"/page/{page_num}"
            urls.append(page)
        
        for url in urls:
            print(url)
            # yield scrapy.Request(url=url,callback=self.link_extractor,meta={"catagory":response.meta["catagory"]})
            yield scrapy.Request(url=url,callback=self.link_extractor,meta={"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
            


    
    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="bp-head"]/h2/a/@href').extract()
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
        alhrer_item=GeneralItem()
        date = response.xpath('//*[@class="mom-post-meta single-post-meta"]/span/time/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
        print("/////////////////////////",date)
      
        alhrer_item["news_agency_name"]="alhrernews"
        alhrer_item["page_url"]=response.meta["page_link"]
        alhrer_item["category"]=response.meta["catagory"]
        alhrer_item["title"]=response.xpath('//*[@class="post-tile entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        alhrer_item["contents"]=contents

        alhrer_item["image_url"]=response.xpath('//*[@class="feature-img"]/img/@src').extract_first()
        alhrer_item["date"]=date
        alhrer_item["author_name"]="alhrer news"

        alhrer_item["main_category"]=response.meta["main_category"]
        alhrer_item["sub_category"]=response.meta["sub_category"]
        alhrer_item["platform"]=response.meta["platform"]
        alhrer_item["media_type"]=response.meta["media_type"]
        alhrer_item["urgency"]=response.meta["urgency"]
        alhrer_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alhrer_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alhrer_item["deleted_at"]=None

        yield alhrer_item
