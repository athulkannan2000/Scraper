import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("sarmad",False)
now = datetime.now()

class SarmadSpider(scrapy.Spider):
    name = 'sarmad'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})
    def link_extractor(self,response):
        news_links=response.xpath('//*[@class="col-md-12"]/div/div/div/div/h2/a/@href').extract()
        print("/////////////news links//////////",news_links)
        for link in news_links:
            link="https://sarmad.com/"+link
            print("link",link)
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        sarmad_item=GeneralItem()
        date = response.xpath('//*[@class="post__title"]/text()').extract()
        date=date[2].replace("\t","").replace("\n","")
        print("//////////////////date//////////////////",date)
        date = str(parser.parse(date)).replace("-","/")
      
        sarmad_item["news_agency_name"]="sarmad"
        sarmad_item["page_url"]=response.meta["page_link"]
        sarmad_item["category"]=response.meta["catagory"]
        sarmad_item["title"]=response.xpath('//*[@class="post__title"]/h2/text()').extract_first()
        
        contents=response.xpath('//*[@class="post_description"]//p/span/text()').extract()+response.xpath('//*[@class="post_description"]//p/text()').extract()
        contents=" ".join(contents)
        sarmad_item["contents"]=contents

        sarmad_item["image_url"]="https://sarmad.com/"+response.xpath('//*[@class="post_thumb"]/img/@src').extract_first()

        sarmad_item["date"]=date
        sarmad_item["author_name"]="sarmad news"

        sarmad_item["main_category"]=response.meta["main_category"]
        sarmad_item["sub_category"]=response.meta["sub_category"]
        sarmad_item["platform"]=response.meta["platform"]
        sarmad_item["media_type"]=response.meta["media_type"]
        sarmad_item["urgency"]=response.meta["urgency"]
        sarmad_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        sarmad_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        sarmad_item["deleted_at"]=None
        yield sarmad_item
        

        
