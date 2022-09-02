import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
from bs4 import BeautifulSoup
from lxml import etree
import requests


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Public Institution for Social Security in Kuwait",False)
now = datetime.now()

class PublicInstitutionSpider(scrapy.Spider):
    name = 'Public_Institution'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            req=requests.get(page,verify=False)
            soup=BeautifulSoup(req.text,"lxml")
            dom = etree.HTML(str(soup)) 
            links=dom.xpath('//*[@class="row gray-box"]/div[2]/a/@href')
            print("/////////////links////////////",links)
            for link in links:
                yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"page_link":link,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc},dont_filter=True)
                
    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        req=requests.get(response.meta["page_link"],verify=False)
        soup=BeautifulSoup(req.text,"lxml")
        dom = etree.HTML(str(soup)) 
        title   =str(dom.xpath('//*[@class="mb-1 mt-3"]/text()')[0])
        content =str("".join(dom.xpath('//*[@dir="rtl"]/text()')))
        date    =str(dom.xpath('//small[1]/text()')[0])
        try:
            image=str(dom.xpath('//*[@class="d-block w-100"]/@src')[0])
        except IndexError:
            image=None

        Public_Institution=GeneralItem()
        Public_Institution["news_agency_name"]="Public Institution for Social Security in Kuwait"
        Public_Institution["page_url"]=str(response.meta["page_link"])
        Public_Institution["category"]=response.meta["catagory"]
        Public_Institution["title"]=title
        Public_Institution["contents"]=content
        Public_Institution["image_url"]=image
        Public_Institution["date"]=date
        Public_Institution["author_name"]="Public Institution for Social Security in Kuwait"
        Public_Institution["main_category"]=response.meta["main_category"]
        Public_Institution["sub_category"]=response.meta["sub_category"]
        Public_Institution["platform"]=response.meta["platform"]
        Public_Institution["media_type"]=response.meta["media_type"]
        Public_Institution["urgency"]=response.meta["urgency"]
        Public_Institution["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Public_Institution["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Public_Institution["deleted_at"]=None
        yield Public_Institution

