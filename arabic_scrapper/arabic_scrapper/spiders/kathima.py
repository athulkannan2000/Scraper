import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("kathima newspaper")
now = datetime.now()

class KathimaSpider(scrapy.Spider):
    name = 'kathima'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            # #print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="more-link button"]/@href').extract()
        # #print("/////////////news links//////////",news_links)
        for link in news_links:
            #print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        kathija_item=GeneralItem()
        date = response.xpath('//*[@id="single-post-meta"]/span[@class="date meta-item tie-icon"]/text()').extract_first()
        date = str(parser.parse(date)).replace("-","/")
        # #print("////////Date///////",date)
      
        kathija_item["news_agency_name"]="kathima newspaper"
        kathija_item["page_url"]=response.meta["page_link"]
        kathija_item["category"]=response.meta["catagory"]
        kathija_item["title"]=response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content entry clearfix"]//p/text()').extract()
        contents=" ".join(contents[0:len(contents)])
        kathija_item["contents"]=contents

        img_url=response.xpath('//*[@class="wp-block-image size-large"]/img/@src').extract_first()
        if img_url==None:
            img_url=response.xpath('//*[@class="wp-block-image size-full"]/img/@src').extract_first()

        kathija_item["image_url"]=img_url

        kathija_item["date"]=date
        kathija_item["author_name"]="kathima"

        kathija_item["main_category"]=response.meta["main_category"]
        kathija_item["sub_category"]=response.meta["sub_category"]
        kathija_item["platform"]=response.meta["platform"]
        kathija_item["media_type"]=response.meta["media_type"]
        kathija_item["urgency"]=response.meta["urgency"]
        kathija_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kathija_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kathija_item["deleted_at"]=None

        yield kathija_item

