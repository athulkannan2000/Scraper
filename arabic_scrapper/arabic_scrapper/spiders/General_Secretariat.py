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

site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("General Secretariat of Supreme Council for Planning and Development (GSSCPD)",False)
now = datetime.now()

class GeneralSecretariatSpider(scrapy.Spider):
    name = 'General_Secretariat'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("////page,catagori///",page,catagori)
            # yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"current_url":page,"catagory":catagori})
            yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

   
    def details_scrapper(self,response):
        ##########################Used to store data in Mysql################################
        req=requests.get(response.meta["current_url"],verify=False)
        dom = etree.HTML(req.text)
        titles=dom.xpath('//h4/text()')
        titles=[str(titles) for titles in titles]

        date=dom.xpath("//tr/td/span/text()")[0:len(titles)]
        date = [str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/") for date in date]

        contents_tag=dom.xpath("//*[@class='about-content']")
        contents=[]
        for i in contents_tag:
            temp=i.xpath("//p/text()")
            temp="".join(temp)
            contents.append(temp)

        image_url=dom.xpath("//*[@class='container1']/a/@href")
        image_url=["https://www.scpd.gov.kw/"+i for i in image_url]
        general_sec_item=GeneralItem()

        for title,content,image,date in zip(titles,contents,image_url,date):
            # content=content[0]
            general_sec_item["news_agency_name"]="General Secretariat of Supreme Council for Planning and Development (GSSCPD)"
            general_sec_item["page_url"]=response.meta["current_url"]
            general_sec_item["category"]=response.meta["catagory"]
            general_sec_item["title"]=title
            general_sec_item["contents"]=content
            general_sec_item["image_url"]=image
            general_sec_item["date"]=date
            general_sec_item["author_name"]="General Secretariat of Supreme Council for Planning and Development (GSSCPD)"
            general_sec_item["main_category"]=response.meta["main_category"]
            general_sec_item["sub_category"]=response.meta["sub_category"]
            general_sec_item["platform"]=response.meta["platform"]
            general_sec_item["media_type"]=response.meta["media_type"]
            general_sec_item["urgency"]=response.meta["urgency"]
            general_sec_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
            general_sec_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
            general_sec_item["deleted_at"]=None
            yield general_sec_item

