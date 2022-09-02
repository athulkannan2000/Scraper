import scrapy
import pandas as pd
import xmltodict
import requests
from arabic_scrapper.helper import news_list
from arabic_scrapper.items import AlraiItem

dataset=news_list()
site_list=dataset.loc[dataset["News Agency in English"]=="Alrai"]["Hyper link"].to_list() #list of sites to scrap
catagory=dataset.loc[dataset["News Agency in English"]=="Alrai"]["Platform -EN"].to_list()


class AlraiSpider(scrapy.Spider):
    name = 'alrai'
    id=0
    def start_requests(self):
        for page,catagori in zip(site_list,catagory):
            print("///////////////",page,catagori)
            response=requests.get(page)
            dict_data = xmltodict.parse(response.content)
            for news in  dict_data["rss"]["channel"]["item"]:
                self.id+=1
                title=news["title"]
                page_url=news["link"]
                published_date=news["pubDate"]
                author=news["author"]

                date=published_date.split(" ")[1:4]
                time=published_date.split(" ")[4:]
                date[1]=self.date_converter(date[1])
                date_and_time="/".join(date)+" "+time[0]

                topic_id=catagori+"_"+date_and_time+"_"+str(self.id)
          
                yield scrapy.Request(url=page_url,callback=self.data_extractor,meta={'title':title, "news_agency_name":"alrai", "page_url":page_url, "catagory":catagori,"published_date_and_time":date_and_time,"author":author,"topic_id":topic_id})


    def data_extractor(self, response):
        # print('//////////////////////////////response',response)
        
        contents=response.xpath('//*[@class="article-desc selectionShareable"]//p/text()').extract()
        contents=" ".join(contents[0:len(contents)])
        # alanba_item["contents"]=contents
        img_url=response.xpath('//*[@class="section-news-carousel"]//div[@class="layout-ratio"]/img/@src').extract()
        img_url=" ".join(img_url[0:len(img_url)])

        alrai_item=AlraiItem()

        alrai_item["topic_id"]=response.meta["topic_id"]
        alrai_item["news_agency_name"]="alrai"
        alrai_item["page_url"]=response.meta["page_url"]
        alrai_item["category"]=response.meta["catagory"]
        alrai_item["title"]=response.meta["title"]
        alrai_item["contents"]=contents
        alrai_item["image_url"]=img_url
        alrai_item["date"]=response.meta["published_date_and_time"]
        alrai_item["author_name"]=response.meta["author"]
        yield alrai_item


        

    def date_converter(self,date):
        date=date.lower()
        months = {
            'jan': "01",
            'feb': "02",
            'mar': "03",
            'apr':"04",
             'may':"05",
             'jun':"06",
             'jul':"07",
             'aug':"08",
             'sep':"09",
             'oct':"10",
             'nov':"11",
             'dec':"12"
            }
        return months[date]