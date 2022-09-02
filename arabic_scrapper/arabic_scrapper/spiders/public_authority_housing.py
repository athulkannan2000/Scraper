import scrapy
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime
from bs4 import BeautifulSoup
from lxml import etree
import requests


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Public Authority for Housing Welfare",False)
now = datetime.now()


class PublicAuthorityHousingSpider(scrapy.Spider):
    name = 'public_authority_housing'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            req=requests.get(page,verify=False)
            soup=BeautifulSoup(req.text,"lxml")
            dom = etree.HTML(str(soup)) 
            titles=dom.xpath('//*[@class="txtMedium"]/text()')
            contents = dom.xpath('//*[@class="txtsmall addReadMore showlesscontent"]/text()')+dom.xpath('//*[@class="row"]/div/a/@href')[0:len(titles)]
            dates = dom.xpath('//*[@class="txtsmall_colorB"]/text()') #date is present in the outside page
            dates = [str(parser.parse(i)).replace("-","/") for i in dates]
            images=dom.xpath('//*[@class="imgframe"]/img/@src')

            # print(contents,len(contents),"\n",dates,len(dates),"\n",titles,len(titles),"\n")
            for title,content,date,image in zip(titles,contents,dates,images):
                content="https://www.pahw.gov.kw"+content
                title=str(title)
                content=str(content)
                date=str(date)
                image="https://www.pahw.gov.kw"+str(image)
                yield scrapy.Request(url="https://www.google.com/",callback=self.details_scrapper,meta={"page_link":page,"image":image,"title":title,"content":content,"date":date,"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc},dont_filter=True)
                
    def details_scrapper(self,response):
        ###########################Used to store data in Mysql################################
        Pub_Auth_Housing=GeneralItem()
        Pub_Auth_Housing["news_agency_name"]="Public Authority for Housing Welfare"
        Pub_Auth_Housing["page_url"]=response.meta["page_link"]
        Pub_Auth_Housing["category"]=response.meta["catagory"]
        Pub_Auth_Housing["title"]=response.meta["title"]
        Pub_Auth_Housing["contents"]=response.meta["content"]
        Pub_Auth_Housing["image_url"]=response.meta["image"]
        Pub_Auth_Housing["date"]=response.meta["date"]
        Pub_Auth_Housing["author_name"]="Public Authority for Housing Welfare"
        Pub_Auth_Housing["main_category"]=response.meta["main_category"]
        Pub_Auth_Housing["sub_category"]=response.meta["sub_category"]
        Pub_Auth_Housing["platform"]=response.meta["platform"]
        Pub_Auth_Housing["media_type"]=response.meta["media_type"]
        Pub_Auth_Housing["urgency"]=response.meta["urgency"]
        Pub_Auth_Housing["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Pub_Auth_Housing["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        Pub_Auth_Housing["deleted_at"]=None
        yield Pub_Auth_Housing

