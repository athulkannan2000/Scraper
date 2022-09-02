
# from curses import meta
from urllib import response
import scrapy
import pandas as pd
from arabic_scrapper.helper import news_list

from arabic_scrapper.items import AlanbaItem

dataset=news_list()
site_list=dataset.loc[dataset["News Agency in English"]=="ALANBA"]["Hyper link"].to_list() #list of sites to scrap
catagory=dataset.loc[dataset["News Agency in English"]=="ALANBA"]["Platform -EN"].to_list()

class AlanbaScrapperSpider(scrapy.Spider):
    name = 'alanba_scrapper'
    id=0
    def start_requests(self):
        for page,catagori in zip(site_list,catagory):
            # print("///////////////",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})

    def link_extractor(self,response):
        # print("Current_url",response.meta['current_url'])
        # print("//////////////////",response.text,"\\\\\\\\\\\\\\\") #used to print html of the page
        news_links=response.xpath('//*[@class="field_group"]/h2/a/@href').extract()
        # print('news_links :',news_links)
        for link in news_links:
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else: 
                self.id+=1   
                page_link="https://www.alanba.com.kw/"+link
                yield scrapy.Request(url=page_link,callback=self.details_scrapper,meta={'page_link':page_link,"catagory":response.meta["catagory"],"id":self.id})

    def details_scrapper(self,response):
        ########## used for csv json generation ###########
        """
        title=response.xpath('//*[@id="dvOrgArticle"]/article/h1/text()').extract_first()
        date=response.xpath('//div[@class="post_date"]/text()').extract()
        date=date[1][1:-1]
        news=response.xpath('//*[@id="maincontent"]/p/text()').extract()
        # news=news[60]
        image_url=response.xpath('//*[@class="pic_multipic post_thumb"]/a/@href').extract_first()
        topic_id=response.meta["catagory"]+"_"+date+'_'+str(response.meta["id"])
        # print("///////////////////",topic_id,"\\\\\\\\\\\\\\\\\\\\\\\\")
        yield {'topic_id':topic_id,'news_agency_name':"alanba",'page_url':response.meta["page_link"],"catagory":response.meta["catagory"],'title':title,"contents":news,"image_url":image_url,'date':date,}
        """
        ###########################Used to store data in Mysql################################
        alanba_item=AlanbaItem()
        date=response.xpath('//div[@class="post_date"]/text()').extract()
        date=date[1][1:-1]
        # print("/////////////////",date)
        alanba_item["topic_id"]=response.meta["catagory"]+"_"+date+'_'+str(response.meta["id"])
        alanba_item["news_agency_name"]="alanba"
        alanba_item["page_url"]=response.meta["page_link"]
        alanba_item["category"]=response.meta["catagory"]
        alanba_item["title"]=response.xpath('//*[@id="dvOrgArticle"]/article/h1/text()').extract_first()
        
        contents=response.xpath('//*[@id="maincontent"]/p/text()').extract()
        contents="".join(contents[0:len(contents)])
        alanba_item["contents"]=contents

        alanba_item["image_url"]=response.xpath('//*[@class="pic_multipic post_thumb"]/a/@href').extract_first()
        alanba_item["date"]=date
        yield alanba_item
        
        
        
        
        
        
        
        
        
        
        

        

