import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from lxml import etree


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists(" Youth Public Authority",False)
now = datetime.now()

class YouthPublicAuthoritySpider(scrapy.Spider):
    name = 'Youth_Public_Authority'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            req=requests.get(page,verify=False)
            soup=BeautifulSoup(req.text,"lxml")
            dom = etree.HTML(str(soup)) 
            links=dom.xpath('//h3/a/@href')
            dates=dom.xpath("//div[@class='activities-list']/ul/li/text()")
            print("///////////////links///////////////",links)
            for link,date in zip(links,dates):
                link="https://www.youth.gov.kw/"+link
                print(link)
                req=requests.get(link)
                soup=BeautifulSoup(req.text,"lxml")
                dom = etree.HTML(str(soup)) 
                title=dom.xpath('//*[@id="PageContent_title"]/text()')[0]
                contents=dom.xpath('//*[@id="PageContent_Span2"]/text()')+dom.xpath('//*[@id="PageContent_lbl"]/text()')
                images=dom.xpath('//*[@class="text-center"]/img/@src')[0]
                contents="".join(contents[0:len(contents)])
                yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"date":str(date),"title":str(title),"contents":contents,"images":str(images),"current_url":str(link),"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})


    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        print("////////%%%%%%%%%%%%%%%% i am here %%%%%%%%%%%%%%%%%%%///////////////")
        Youth_Public_item=GeneralItem()

        Youth_Public_item["news_agency_name"]=" Youth Public Authority"
        Youth_Public_item["page_url"]=response.meta["current_url"]
        Youth_Public_item["category"]=response.meta["catagory"]
        Youth_Public_item["title"]=response.meta["title"]
        Youth_Public_item["contents"]=response.meta["contents"]
        Youth_Public_item["image_url"]=response.meta["images"]
        Youth_Public_item["date"]=response.meta["date"]
        Youth_Public_item["author_name"]=" Youth Public Authority"
        Youth_Public_item["main_category"]=response.meta["main_category"]
        Youth_Public_item["sub_category"]=response.meta["sub_category"]
        Youth_Public_item["platform"]=response.meta["platform"]
        Youth_Public_item["media_type"]=response.meta["media_type"]
        Youth_Public_item["urgency"]=response.meta["urgency"]
        Youth_Public_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Youth_Public_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Youth_Public_item["deleted_at"]=None
        yield  Youth_Public_item





