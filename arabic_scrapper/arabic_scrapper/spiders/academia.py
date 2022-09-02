import scrapy
import pandas as pd
import xmltodict
import requests
from arabic_scrapper.items import GeneralItem
from dateutil import parser
from deep_translator import GoogleTranslator
from arabic_scrapper.helper import load_dataset_lists
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("academia",False)
now = datetime.now()

class AcademiaSpider(scrapy.Spider):
    name = 'academia'
    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency): 
            print("///////////////",page,catagori)
            response=requests.get(page)
            dict_data = xmltodict.parse(response.content)
            # print("////////dict data//////",dict_data)
            for news in  dict_data["rss"]["channel"]["item"]:
                title=news["title"]
                page_url=news["link"]
                published_date=news["pubDate"]
                author=news["dc:creator"]
                contents=news["content:encoded"]
                date=news["pubDate"]
                date = str(parser.parse(GoogleTranslator(source='auto', target='en').translate(date))).replace("-","/")
                # yield scrapy.Request(url=page_url,callback=self.data_extractor,meta={'title':title,"contents":contents, "news_agency_name":"alrai", "page_url":page_url, "catagory":catagori,"published_date_and_time":date,"author":author})
                yield scrapy.Request(url=page_url,callback=self.data_extractor,meta={'title':title,"contents":contents, "news_agency_name":"alrai", "page_url":page_url, "catagory":catagori,"published_date_and_time":date,"author":author,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})
 
 
    def data_extractor(self, response):
        # print('//////////////////////////////response',response)
        academia_item=GeneralItem() 
        academia_item["news_agency_name"]="academia"
        academia_item["page_url"]=response.meta["page_url"]
        academia_item["category"]=response.meta["catagory"]
        academia_item["title"]=response.meta["title"]
        academia_item["contents"]=response.meta["contents"]
        academia_item["image_url"]=response.xpath('//*[@class="single-featured-image"]/img/@src').extract_first()
        academia_item["date"]=response.meta["published_date_and_time"]
        academia_item["author_name"]=response.meta["author"]

        academia_item["main_category"]=response.meta["main_category"]
        academia_item["sub_category"]=response.meta["sub_category"]
        academia_item["platform"]=response.meta["platform"]
        academia_item["media_type"]=response.meta["media_type"]
        academia_item["urgency"]=response.meta["urgency"]
        academia_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        academia_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        academia_item["deleted_at"]=None
        yield academia_item      