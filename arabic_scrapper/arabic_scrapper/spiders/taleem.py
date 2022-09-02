import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("taleem news",False)
now = datetime.now()

class TaleemSpider(scrapy.Spider):
    name = 'taleem'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="item-inner"]//h2/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        taleem_item=GeneralItem()
        date = response.xpath('//*[@class="post-meta single-post-meta"]/span/time/b/text()').extract_first()
      
        taleem_item["news_agency_name"]="taleem news"
        taleem_item["page_url"]=response.meta["page_link"]
        taleem_item["category"]=response.meta["catagory"]
        taleem_item["title"]=response.xpath('//*[@class="single-post-title"]/span/text()').extract_first()
        
        contents=response.xpath('//*[@class="entry-content clearfix single-post-content"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        taleem_item["contents"]=contents

        taleem_item["image_url"]=response.xpath('//*[@class="post-thumbnail open-lightbox"]/img/@src').extract_first()
        taleem_item["date"]=date
        taleem_item["author_name"]="taleem"

        taleem_item["main_category"]=response.meta["main_category"]
        taleem_item["sub_category"]=response.meta["sub_category"]
        taleem_item["platform"]=response.meta["platform"]
        taleem_item["media_type"]=response.meta["media_type"]
        taleem_item["urgency"]=response.meta["urgency"]
        taleem_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        taleem_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        taleem_item["deleted_at"]=None

        yield taleem_item
