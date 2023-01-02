import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from lxml import etree


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("central agency for public tenders")
now = datetime.now()

class CentralAgencyForpublicTendersSpider(scrapy.Spider):
    name = 'CentralAgencyForpublicTenders'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            #print("////page,catagori///",page,catagori)
            req=requests.get(page,verify=False)
            soup=BeautifulSoup(req.text,"lxml")
            dom = etree.HTML(str(soup)) 
            links=dom.xpath('//p[@class="detail"]/a/@href')
            if len(links) == 0:
                links=dom.xpath('//div[@class="table-cell text-center"]/a/@href')
            links = ['https://capt.gov.kw'+item for item in links]

            dates=dom.xpath('//p[@class="date"]/text()')
            if len(dates) == 0:
                dates = dom.xpath('//div[@class="table-cell text-center"]/text()')
                dates = dates[2:]
                ls=[]
                for i in dates:
                    if i != '\n':
                        ls.append(i)
                dates = ls
            #print("///////////////links///////////////",links)
            for link,date in zip(links,dates):
                if link.endswith('.pdf'):
                    print(link)
                    title = "تحميل"
                    content = "قم بتنزيل ملف pdf باستخدام الرابط"
                    images = None
                else:
                    req=requests.get(link)
                    soup=BeautifulSoup(req.text,"lxml")
                    dom = etree.HTML(str(soup)) 
                    title=dom.xpath('//div[@class="title-area text-center"]/h1/text()')
                    content = dom.xpath('//*[@style="text-align: center;"]/span/span/span/span/span/strong/span/span/text()')
                    if len(content)==0:
                        content = dom.xpath('//*[@style="text-align: center;"]/span/span/span/strong/span/span/text()')+dom.xpath('//div[@class="page-width"]/p/strong/text()')
                    images=None
                yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"date":str(date),"title":str(title),"contents":content,"images":str(images),"current_url":str(link),"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})


    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        #print("////////%%%%%%%%%%%%%%%% i am here %%%%%%%%%%%%%%%%%%%///////////////")
        CAPT_item=GeneralItem()

        CAPT_item["news_agency_name"]=" central agency for public tenders"
        CAPT_item["page_url"]=response.meta["current_url"]
        CAPT_item["category"]=response.meta["catagory"]
        CAPT_item["title"]=response.meta["title"]
        CAPT_item["contents"]=response.meta["contents"]
        CAPT_item["image_url"]=response.meta["images"]
        CAPT_item["date"]=response.meta["date"]
        CAPT_item["author_name"]=" central agency for public tenders"
        CAPT_item["main_category"]=response.meta["main_category"]
        CAPT_item["sub_category"]=response.meta["sub_category"]
        CAPT_item["platform"]=response.meta["platform"]
        CAPT_item["media_type"]=response.meta["media_type"]
        CAPT_item["urgency"]=response.meta["urgency"]
        CAPT_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        CAPT_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        CAPT_item["deleted_at"]=None
        yield  CAPT_item





