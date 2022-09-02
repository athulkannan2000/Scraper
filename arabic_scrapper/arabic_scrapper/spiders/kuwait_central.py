import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists(" Kuwait Central Statistical Bureau",False)
now = datetime.now()

class KuwaitCentralSpider(scrapy.Spider):
    name = 'kuwait_central'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url=page,callback=self.link_extractor,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        news_links = response.xpath('//*[@class="col-md-10"]/article/a/@href').extract()
        print("///////////// news links //////////",news_links)
        for link in news_links:
            link="https://www.csb.gov.kw/Pages/"+link
            if link=="":
                continue #some pages may not have textual contents on that case it become empty
            else:  
                # yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"]})
                yield scrapy.Request(url=link,callback=self.details_scrapper,meta={'page_link':link,"catagory":response.meta["catagory"],"main_category":response.meta["main_category"],"sub_category":response.meta["sub_category"],"platform":response.meta["platform"],"media_type":response.meta["media_type"],"urgency":response.meta["urgency"]})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        kuwait_central=GeneralItem()
        now = datetime.now() # current date and time
        date = now.strftime("%Y:%m:%d %H:%M:%S")
        date = response.xpath('//*[@id="single-post-meta"]/span[2]/text()').extract_first()
        print("/////////////////////////",date)
        
        kuwait_central["news_agency_name"]=" Kuwait Central Statistical Bureau"
        kuwait_central["page_url"]=response.meta["page_link"]
        kuwait_central["category"]=response.meta["catagory"]
        kuwait_central["title"]=response.xpath('//h3/span/text()').extract_first()
        
        contents=response.xpath('//article/span/text()').extract()
        contents="".join(contents[0:len(contents)])
        kuwait_central["contents"]=contents

        kuwait_central["image_url"]="https://www.csb.gov.kw/"+str(response.xpath('//div[@class="row margin-bottom-0 "]/div/table/tbody/tr/td/img/@src').extract_first())
        kuwait_central["date"]=date
        kuwait_central["author_name"]="Kuwait Central Statistical Bureau" 

        kuwait_central["main_category"]=response.meta["main_category"]
        kuwait_central["sub_category"]=response.meta["sub_category"]
        kuwait_central["platform"]=response.meta["platform"]
        kuwait_central["media_type"]=response.meta["media_type"]
        kuwait_central["urgency"]=response.meta["urgency"]
        kuwait_central["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kuwait_central["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        kuwait_central["deleted_at"]=None
        yield kuwait_central
