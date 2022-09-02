import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists,agos_changer 
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("khalejianews",False)
now = datetime.now()

class KhalejianewsSpider(scrapy.Spider):
    name = 'khalejianews'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency):  
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="post-title"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        khalejinews_item=GeneralItem()
        date = response.xpath('//*[@class="title"]/p/text()').extract_first()
        # date ="".join(date[0:len(date)])
        print("//////////////////date////////////",date)
        date = agos_changer(date)
        print("///////////////////title////////////////",response.xpath('//*[@class="title"]/h2/text()').extract_first())

      
        khalejinews_item["news_agency_name"]="khalejianews"
        khalejinews_item["page_url"]=response.meta["page_link"]
        khalejinews_item["category"]=response.meta["catagory"]
        khalejinews_item["title"]=response.xpath('//*[@class="title"]/h2/text()').extract_first()
        
        contents=response.xpath('//*[@class="rtl"]/text()').extract()
        contents="".join(contents[0:len(contents)])
        khalejinews_item["contents"]=contents

        khalejinews_item["image_url"]=response.xpath('//*[@class="article-img"]/img/@src').extract_first()
        khalejinews_item["date"]=date
        khalejinews_item["author_name"]="khalejinews"

        khalejinews_item["main_category"]=response.meta["main_category"]
        khalejinews_item["sub_category"]=response.meta["sub_category"]
        khalejinews_item["platform"]=response.meta["platform"]
        khalejinews_item["media_type"]=response.meta["media_type"]
        khalejinews_item["urgency"]=response.meta["urgency"]
        khalejinews_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        khalejinews_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        khalejinews_item["deleted_at"]=None

        yield khalejinews_item

