import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Ministry Of Electricity and Water and Renewable Energy",False)
now = datetime.now()

class MinistryElecSpider(scrapy.Spider):
    name = 'Ministry_Elec'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="col-xs-12 col-sm-6 col-md-4 col-lg-3"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://www.mew.gov.kw/ar/media-center/news"+link
            print("/////////link////////",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        ministry_elec=GeneralItem()
        date = response.xpath('//*[@class="news_date"]/text()').extract_first()
        date = str(parser.parse(date)).replace("-","/")
        print("/////Date/////",date)

        ministry_elec["news_agency_name"]="Ministry Of Electricity and Water and Renewable Energy"
        ministry_elec["page_url"]=response.meta["page_link"]
        ministry_elec["category"]=response.meta["catagory"]
        ministry_elec["title"]=response.xpath('//*[@class="news_title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="default_page news_details_page"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        ministry_elec["contents"]=contents

        images="https://www.mew.gov.kw"+response.xpath('//*[@class="thumbnail img_round"]/img/@src').extract_first()
        ministry_elec["image_url"]=images
        ministry_elec["date"]=date
        ministry_elec["author_name"]="Ministry Of Electricity and Water and Renewable Energy"
        
        ministry_elec["main_category"]=response.meta["main_category"]
        ministry_elec["sub_category"]=response.meta["sub_category"]
        ministry_elec["platform"]=response.meta["platform"]
        ministry_elec["media_type"]=response.meta["media_type"]
        ministry_elec["urgency"]=response.meta["urgency"]
        ministry_elec["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_elec["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_elec["deleted_at"]=None

        yield ministry_elec

