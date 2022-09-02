import scrapy
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Ministry of Awqaf and Islamic Affairs",False)
now = datetime.now()

class MinistryAwqafSpider(scrapy.Spider):
    name = 'Ministry_Awqaf'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="col-md-10"]/h5/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://www.awqaf.gov.kw"+link
            print("////////link//////////////",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})
                # pass

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        ministry_awqaf=GeneralItem()

        date = response.xpath('//*[@id="MainContent_date"]/text()').extract_first()
        date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
        print("/////Date/////",date)

        ministry_awqaf["news_agency_name"]="Ministry of Awqaf and Islamic Affairs"
        ministry_awqaf["page_url"]=response.meta["page_link"]
        ministry_awqaf["category"]=response.meta["catagory"]
        ministry_awqaf["title"]=response.xpath('//*[@id="MainContent_title"]/text()').extract_first()
        
        contents=response.xpath('//p//span/text()').extract()+response.xpath('//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        ministry_awqaf["contents"]=contents

        images=response.xpath('//*[@id="MainContent_divImage"]/img/@src').extract_first()
        ministry_awqaf["image_url"]=images
        ministry_awqaf["date"]=date
        ministry_awqaf["author_name"]="Ministry of Awqaf and Islamic Affairs"

        ministry_awqaf["main_category"]=response.meta["main_category"]
        ministry_awqaf["sub_category"]=response.meta["sub_category"]
        ministry_awqaf["platform"]=response.meta["platform"]
        ministry_awqaf["media_type"]=response.meta["media_type"]
        ministry_awqaf["urgency"]=response.meta["urgency"]
        ministry_awqaf["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_awqaf["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        ministry_awqaf["deleted_at"]=None

        yield ministry_awqaf


