import scrapy
import pandas as pd
from arabic_scrapper.helper import load_dataset_lists
from arabic_scrapper.items import GeneralItem
from datetime import datetime

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alanba ",False)
now = datetime.now()

class AlanbaScrapperSpider(scrapy.Spider):
    name = 'alanba'
    def start_requests(self):
        print("///////////////////////inside/////////////////////")
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("//////Page,catgory/////////",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links=response.xpath('//*[@class="field_group"]/h2/a/@href').extract()
        for link in news_links:
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:   
                page_link="https://www.alanba.com.kw/"+link
                yield scrapy.Request(url=page_link,callback=self.details_scrapper,meta={'page_link':page_link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

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
        alanba_item=GeneralItem()
        date=response.xpath('//div[@class="post_date"]/text()').extract()
        date=date[1][1:-1]
        # print("/////////////////",date)
        alanba_item["news_agency_name"]="alanba "
        alanba_item["page_url"]=response.meta["page_link"]
        alanba_item["category"]=response.meta["catagory"]
        alanba_item["title"]=response.xpath('//*[@id="dvOrgArticle"]/article/h1/text()').extract_first() 
        contents=response.xpath('//*[@id="maincontent"]/p/text()').extract()
        contents="".join(contents[0:len(contents)])
        alanba_item["contents"]=contents
        alanba_item["image_url"]=response.xpath('//*[@class="pic_multipic post_thumb"]/a/@href').extract_first()
        alanba_item["date"]=date
        alanba_item["author_name"]="alanba"

        alanba_item["main_category"]=response.meta["main_category"]
        alanba_item["sub_category"]=response.meta["sub_category"]
        alanba_item["platform"]=response.meta["platform"]
        alanba_item["media_type"]=response.meta["media_type"]
        alanba_item["urgency"]=response.meta["urgency"]
        alanba_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alanba_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alanba_item["deleted_at"]=None
        yield alanba_item
        
        
        
        
        
        
        
        
        
        
        

        

