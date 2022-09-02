import scrapy
import pandas as pd
from arabic_scrapper.helper import load_dataset_lists,selenium_path
from arabic_scrapper.items import GeneralItem
import time
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml import etree
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Diwan crown prince state of kuwait",False)
now = datetime.now()


class DiwanSpider(scrapy.Spider):
    name = 'diwan'
    def __init__(self):
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chromedriver=selenium_path()


    def start_requests(self):
        for page,catagori,main_categor,sub_categor,platfor,media_typ,urgenc in zip(site_list,catagory,main_category,sub_category,platform,media_type,urgency):  
            url = page
            driver = webdriver.Chrome(self.chromedriver,options=self.chrome_options)
            driver.delete_all_cookies()
            driver.get(url)
            # time.sleep(5)
            html=driver.page_source
            driver.quit()
            soup=BeautifulSoup(html,"lxml") #donwloading the entiring page 
            print("////////////////soup////////////",soup)
            dom = etree.HTML(str(soup)) 
            # print("//////////////////dom/////////////////",dom)
            urls=dom.xpath('//*[@class="news-texts"]/a/@href')    
            print("///////////////////all urls/////////////////////////",urls)
            for url in urls:
                page_url="https://cpd.gov.kw/Ar/"+url
                print("/////////////////////pageurl//////////////////",page_url)
                # yield scrapy.Request(url=page_url,callback=self.link_extractor,meta={"current_url":page_url,"catagory":catagori})
                yield scrapy.Request(url=page_url,callback=self.link_extractor,meta={"current_url":page_url,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def link_extractor(self,response):
        print("/////////////////////////////News page//////////////////////",response.meta["current_url"])
        #download
        driver = webdriver.Chrome(self.chromedriver,options=self.chrome_options)
        driver.delete_all_cookies()
        driver.get(response.meta["current_url"])
        html=driver.page_source
        driver.quit()
        soup=BeautifulSoup(html,"lxml")
        dom = etree.HTML(str(soup))

        contents=dom.xpath('//*[@class="news-texts"]//p/text()')
        contents="".join(contents[0:len(contents)])


        diwan_item=GeneralItem()
        diwan_item["news_agency_name"]="Diwan crown prince state of kuwait"
        diwan_item["page_url"]=response.meta["current_url"]
        diwan_item["category"]=response.meta["catagory"]
        diwan_item["title"]=str("".join(dom.xpath('//*[@class="news-texts"]/a/h3/text()')))
        diwan_item["contents"]=contents
        diwan_item["image_url"]="https://cpd.gov.kw"+str("".join(dom.xpath('//*[@class="carousel-item active"]/img/@src')))
        diwan_item["date"]=str("".join(dom.xpath('//*[@class="news-texts"]/p[1]/text()')))
        diwan_item["author_name"]="Diwan crown prince state of kuwait"

        diwan_item["main_category"]=response.meta["main_category"]
        diwan_item["sub_category"]=response.meta["sub_category"]
        diwan_item["platform"]=response.meta["platform"]
        diwan_item["media_type"]=response.meta["media_type"]
        diwan_item["urgency"]=response.meta["urgency"]
        diwan_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        diwan_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        diwan_item["deleted_at"]=None
        yield diwan_item

        