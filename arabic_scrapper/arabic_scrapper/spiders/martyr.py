import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("martyr office",False)
now = datetime.now()

class MartyrSpider(scrapy.Spider):
    name = 'martyr'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="blog-post_wrapper"]/div[2]/h4/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        martyr_item=GeneralItem()
        date = response.xpath('//*[@class="blog-post_content"]/p[2]/strong[2]/text()').extract_first()
        if date==None:
            date=response.xpath('//*[@class="blog-post_content"]/p[2]/strong[1]/text()').extract_first()
            date=date.split(":")
            date = str(parser.parse(date[1])).replace("-","/")
        else:
            date = str(parser.parse(date)).replace("-","/")
      
        martyr_item["news_agency_name"]="martyr office"
        martyr_item["page_url"]=response.meta["page_link"]
        martyr_item["category"]=response.meta["catagory"]
        martyr_item["title"]=response.xpath('//*[@class="blog-post_title"]/text()').extract_first()
        
        contents=response.xpath('//*[@class="blog-post_content"]//p/span/text()').extract()+response.xpath('//*[@class="blog-post_content"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        martyr_item["contents"]=contents

        martyr_item["image_url"]=response.xpath('//*[@class="blog-post_media_part"]/img/@src').extract_first()
        martyr_item["date"]=date
        martyr_item["author_name"]="martyr news"

        martyr_item["main_category"]=response.meta["main_category"]
        martyr_item["sub_category"]=response.meta["sub_category"]
        martyr_item["platform"]=response.meta["platform"]
        martyr_item["media_type"]=response.meta["media_type"]
        martyr_item["urgency"]=response.meta["urgency"]
        martyr_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        martyr_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        martyr_item["deleted_at"]=None
        yield martyr_item


