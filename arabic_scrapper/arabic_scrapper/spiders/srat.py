import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists,agos_changer
from datetime import datetime

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("srat news",False)
now = datetime.now()

class SratSpider(scrapy.Spider):
    name = 'srat'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="post-details"]/h3/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        srat_item=GeneralItem()
        date = response.xpath('//*[@class="date meta-item"]/span[2]/text()').extract_first()
        print("/////////////////////////",date)
        date=agos_changer(date) #used to change 5 mins/week/day/month/seconds/year ago to exact date and time
      
        srat_item["news_agency_name"]="srat news"
        srat_item["page_url"]=response.meta["page_link"]
        srat_item["category"]=response.meta["catagory"]
        srat_item["title"]=response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content entry clearfix"]//p/text()').extract()+response.xpath('//*[@class="entry-content entry clearfix"]//div/text()').extract()
        contents="".join(contents[0:len(contents)])
        srat_item["contents"]=contents

        srat_item["image_url"]=response.xpath('//*[@class="single-featured-image"]/img/@src').extract_first()
        srat_item["date"]=date
        srat_item["author_name"]=response.xpath('//*[@class="meta-author meta-item"]/a/text()').extract_first()

        srat_item["main_category"]=response.meta["main_category"]
        srat_item["sub_category"]=response.meta["sub_category"]
        srat_item["platform"]=response.meta["platform"]
        srat_item["media_type"]=response.meta["media_type"]
        srat_item["urgency"]=response.meta["urgency"]
        srat_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        srat_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        srat_item["deleted_at"]=None

        yield srat_item
