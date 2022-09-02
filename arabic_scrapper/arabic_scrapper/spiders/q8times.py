import scrapy
from arabic_scrapper.helper import load_dataset_lists
from arabic_scrapper.items import GeneralItem
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("q8times",False)
now = datetime.now()

class Q8timesSpider(scrapy.Spider):
    name = 'q8times'
    # allowed_domains = ['https://q8times.com/.*']
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("///////////////",page,catagori)
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="post-box-title"]/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            print("link",link)
            yield scrapy.Request(url=link, callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        q8_item=GeneralItem()
    
        q8_item["news_agency_name"]="q8times"
        q8_item["page_url"]=response.meta["page_link"]
        q8_item["category"]=response.meta["catagory"]
        q8_item["title"]=response.xpath('//*[@class="name post-title entry-title"]/span/text()').extract_first()
        contents=response.xpath('//*[@class="entry"]//p/text()').extract()
        contents="".join(contents[0:len(contents)])
        q8_item["contents"]=contents
        q8_item["image_url"]=response.xpath('//div[@class="entry"]/p/a/img/@src').extract_first()
        date =str(datetime.now())
        date =date.replace("-","/")
        date_and_time=date.split(".")[0]
        q8_item["date"]=date_and_time
        q8_item["author_name"]="q8times"

        q8_item["main_category"]=response.meta["main_category"]
        q8_item["sub_category"]=response.meta["sub_category"]
        q8_item["platform"]=response.meta["platform"]
        q8_item["media_type"]=response.meta["media_type"]
        q8_item["urgency"]=response.meta["urgency"]
        q8_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        q8_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        q8_item["deleted_at"]=None


        yield q8_item


