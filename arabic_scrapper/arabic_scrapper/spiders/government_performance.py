############################Used "requests" library instead of selenium########################
import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from deep_translator import GoogleTranslator
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
import requests
from lxml import etree
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("government performance follow up",False)
now = datetime.now()

class GovernmentPerformanceSpider(scrapy.Spider):
    name = 'government_performance'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency):  
            req=requests.get(page)
            dom = etree.HTML(req.text)
            urls=dom.xpath('//div/a/@href')
            print("///////////urls/////////////",urls)
            for url in urls:
                # print("//////////url//////////",url)
                url="https://www.gpf.gov.kw/Ar/"+url
                # yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"current_url":url,"catagory":catagori},dont_filter=True  )
                yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,dont_filter=True,meta={"current_url":url,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        req=requests.get(response.meta["current_url"])
        dom = etree.HTML(req.text)
        try:
            title=str(dom.xpath('//div/h4/text()')[0])
        except IndexError:
            title=str("".join(dom.xpath('//div/h4/text()')))
        try:
            date=str(dom.xpath('//div/span/span/p/strong/text()')[0].split(":")[1])
            date = str(parser.parse(date)).replace("-","/")
        except IndexError:
            date=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        try:
            contents=str(" ".join(dom.xpath('//span//p/text()')))+" "+(" ".join(dom.xpath('//span//div/text()')))
        except IndexError:
            contents=str(" ".join(dom.xpath('//span//p/text()')))
        try:
            image_url=str(dom.xpath('//div/div/img/@src')[0])
        except IndexError:
            image_url=str(" ".join(dom.xpath('//div/div/img/@src')))
        # print(contents)
        print(title,contents,date,image_url)
        gov_perf_item=GeneralItem()
        gov_perf_item["news_agency_name"]="government performance follow up"
        gov_perf_item["page_url"]=response.meta["current_url"]
        gov_perf_item["category"]=response.meta["catagory"]
        gov_perf_item["title"]=title
        gov_perf_item["contents"]=contents
        gov_perf_item["image_url"]=image_url
        gov_perf_item["date"]=date
        gov_perf_item["author_name"]="government performance follow up" 
        gov_perf_item["main_category"]=response.meta["main_category"]
        gov_perf_item["sub_category"]=response.meta["sub_category"]
        gov_perf_item["platform"]=response.meta["platform"]
        gov_perf_item["media_type"]=response.meta["media_type"]
        gov_perf_item["urgency"]=response.meta["urgency"]
        gov_perf_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        gov_perf_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        gov_perf_item["deleted_at"]=None
        yield gov_perf_item

