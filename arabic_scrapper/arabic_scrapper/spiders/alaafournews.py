import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("AlaafourNews_",False)
now = datetime.now()


class AlaafournewsSpider(scrapy.Spider):
    name = 'alaafournews'
    def __init__(self):
        self.urls=[]
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            self.urls.append(page)
            yield scrapy.Request(url=page,callback=self.pagination_handler,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def pagination_handler(self,response):
        next_page = response.xpath('//*[@class="pagination"]/li[13]/a/@href').extract_first()

        if next_page==None or next_page=="None": #all page links are extracted
            # print("///////////////////// Crawled all pages /////////////////////")
            for url in self.urls:
                yield scrapy.Request(url=url,callback=self.link_extractor,dont_filter=True,meta={"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
                
        elif next_page!="None" or next_page!="":
            next_page="https://mugtama.com/"+str(next_page)
            self.urls.append(next_page)
            yield scrapy.Request(url=next_page,callback=self.pagination_handler,meta={"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
        

    def link_extractor(self,response):
        news_links=response.xpath('//*[@class="catItemTitle"]/a/@href').extract()

        print("/////////////news links//////////",news_links,"/////////////",response.meta["catagory"])
        for link in news_links:
            print("link",link)
            link="https://mugtama.com"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        alaafournews=GeneralItem()
        date = response.xpath('//*[@class="itemDateCreated"]/text()').extract_first()
        print("/////////////date//////////",date,type(date))
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
        print("////////////date after/////////////",date)
      
        alaafournews["news_agency_name"]= "AlaafourNews_"
        alaafournews["page_url"]=response.meta["page_link"]
        alaafournews["category"]=response.meta["catagory"]

        title=response.xpath('//h2[@class="itemTitle"]/text()').extract()
        title="".join(title[0:len(title)]).replace("\t","").replace("\n","")
        alaafournews["title"]=title

        contents=response.xpath('//*[@class="itemFullText"]//p/strong/span/text()').extract()
        if contents==None:
            contents=response.xpath('//*[@class="itemFullText"]//p/span/strong/text()').extract()
        contents="".join(contents[0:len(contents)])
        alaafournews["contents"]=contents
        alaafournews["image_url"]="https://mugtama.com"+response.xpath('//*[@class="itemImage"]/a/img/@data-src').extract_first()
        alaafournews["date"]=date

        author_name=response.xpath('//*[@class="itemAuthor"]/a/text()').extract_first()
        if author_name==None:
            author_name=response.xpath('//*[@class="itemAuthor"]/text()').extract_first()
        alaafournews["author_name"]=author_name

        alaafournews["main_category"]=response.meta["main_category"]
        alaafournews["sub_category"]=response.meta["sub_category"]
        alaafournews["platform"]=response.meta["platform"]
        alaafournews["media_type"]=response.meta["media_type"]
        alaafournews["urgency"]=response.meta["urgency"]
        alaafournews["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alaafournews["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alaafournews["deleted_at"]=None

        yield alaafournews

    




    
