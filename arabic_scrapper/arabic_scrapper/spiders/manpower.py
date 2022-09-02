import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Public authority of manpower",False)
now = datetime.now()

class Manpower(scrapy.Spider):
    name = 'manpower'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//div[@class="innermain"]/h4/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link = "https://www.manpower.gov.kw/"+link
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        capt_gov=GeneralItem()
        date = response.xpath('//div[@class="tp-row"]/span/text()').extract_first()
        #date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")

      
        capt_gov["news_agency_name"]="Public authority of manpower"
        capt_gov["page_url"]=response.meta["page_link"]
        capt_gov["category"]=response.meta["catagory"]

        capt_gov["title"]=response.xpath('//div[@class="inner"]/h3/text()').extract_first()
        
        contents=response.xpath('//p[@style="user-select: auto;"]/text()').extract()
        contents="".join(contents[0:len(contents)])
        capt_gov["contents"]=contents

        image_url = response.xpath('//div[@class="frame"]/img/@src').extract_first()
        # print(image_url)
        capt_gov["image_url"]= "manpower.gov.kw/"+image_url[2:]
        capt_gov["date"]=date
        capt_gov["author_name"]="الهيئة العامة للقوى العاملة"

        capt_gov["main_category"]=response.meta["main_category"]
        capt_gov["sub_category"]=response.meta["sub_category"]
        capt_gov["platform"]=response.meta["platform"]
        capt_gov["media_type"]=response.meta["media_type"]
        capt_gov["urgency"]=response.meta["urgency"]
        capt_gov["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        capt_gov["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        capt_gov["deleted_at"]=None
        yield capt_gov
        


