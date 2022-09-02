import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Ministry of Justice",False)
now = datetime.now()

class MinistryJusticeSpider(scrapy.Spider):
    name = 'ministry_justice'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="site-news-items site-flex"]//a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://pp.moj.gov.kw"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        ministry_justice=GeneralItem()
        date = response.xpath('//*[@class="site-news-item-date"]/text()').extract_first()
        print("/////Date/////",date)
        date = str(parser.parse(date)).replace("-","/")

        ministry_justice["news_agency_name"]="Ministry of Justice"
        ministry_justice["page_url"]=response.meta["page_link"]
        ministry_justice["category"]=response.meta["catagory"]
        ministry_justice["title"]=response.xpath('//*[@class="customeTitle"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="site-content"]/p/text()').extract()
        contents="".join(contents[0:len(contents)])
        ministry_justice["contents"]=contents

        images="https://pp.moj.gov.kw/"+response.xpath('//*[@class="site-content"]/img/@src').extract_first()
        ministry_justice["image_url"]=images
        ministry_justice["date"]=date
        ministry_justice["author_name"]="Ministry of Justice"
        ministry_justice["main_category"]=response.meta["main_category"]
        ministry_justice["sub_category"]=response.meta["sub_category"]
        ministry_justice["platform"]=response.meta["platform"]
        ministry_justice["media_type"]=response.meta["media_type"]
        ministry_justice["urgency"]=response.meta["urgency"]
        ministry_justice["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_justice["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_justice["deleted_at"]=None
        yield ministry_justice

