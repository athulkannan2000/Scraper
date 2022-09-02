import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("ministry of commerce and industry",False)
now = datetime.now()

class MinistryCommerceSpider(scrapy.Spider):
    name = 'ministry_commerce'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency):  
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="news-box"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://www.moci.gov.kw"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        min_com_item=GeneralItem()
        date = response.xpath('//*[@class="page-head"]/h2/span/text()').extract_first()
        date = str(parser.parse(date)).replace("-","/")

        min_com_item["news_agency_name"]="ministry of commerce and industry"
        min_com_item["page_url"]=response.meta["page_link"]
        min_com_item["category"]=response.meta["catagory"]
        min_com_item["title"]=response.xpath('//*[@class="page-head"]/h2/text()').extract_first()
        
        contents=response.xpath('//*[@class="details"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        min_com_item["contents"]=contents

        images=response.xpath('//*[@class="image"]/img/@src').extract()
        images=[*set(images)] #removes duplicates
        images=" ".join(images)

        min_com_item["image_url"]=images
        min_com_item["date"]=date
        min_com_item["author_name"]="ministry of commerce and industry"

        min_com_item["main_category"]=response.meta["main_category"]
        min_com_item["sub_category"]=response.meta["sub_category"]
        min_com_item["platform"]=response.meta["platform"]
        min_com_item["media_type"]=response.meta["media_type"]
        min_com_item["urgency"]=response.meta["urgency"]
        min_com_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        min_com_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        min_com_item["deleted_at"]=None

        yield min_com_item

