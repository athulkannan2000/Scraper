import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("central agency for information technology",False)
now = datetime.now()

class CentralAgencySpider(scrapy.Spider):
    name = 'central_agency'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):

        news_links =response.xpath('//*[@class="NewsPTitle"]/a/@href').extract()
        print("///////////////////news links////////////////",news_links)
        for link in news_links:
            print("link",link)
            link="https://www.cait.gov.kw"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        central_agency_item=GeneralItem()

        date = response.xpath('//*[@class="NewsContent"]/div/text()').extract_first()
        date = date = str(parser.parse(date)).replace("-","/")
      
        central_agency_item["news_agency_name"]="central agency for information technology"
        central_agency_item["page_url"]=response.meta["page_link"]
        central_agency_item["category"]=response.meta["catagory"]
        central_agency_item["title"]=str(response.xpath('//*[@class="newsItemDetail"]/h4/text()').extract_first())
        
        contents=response.xpath('//*[@class="NewsBody"]//p/span/text()').extract()+response.xpath('//*[@class="NewsBody"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        central_agency_item["contents"]=contents

        central_agency_item["image_url"]="https://www.cait.gov.kw/"+str(response.xpath('//*[@class="NewsBody"]/img/@src').extract_first())
        central_agency_item["date"]=date
        central_agency_item["author_name"]="central agency for information technology"

        central_agency_item["main_category"]=response.meta["main_category"]
        central_agency_item["sub_category"]=response.meta["sub_category"]
        central_agency_item["platform"]=response.meta["platform"]
        central_agency_item["media_type"]=response.meta["media_type"]
        central_agency_item["urgency"]=response.meta["urgency"]
        central_agency_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        central_agency_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        central_agency_item["deleted_at"]=None
        yield central_agency_item
