import scrapy
import pandas as pd
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists,selenium_path
from datetime import datetime
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml import etree
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Legal Advice and Legislation ",False)
now = datetime.now()

class LegalAdviceSpider(scrapy.Spider):
    name = 'legal_advice'
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
            time.sleep(3)
            html=driver.page_source
            driver.quit()
            soup=BeautifulSoup(html,"lxml") 
         
            dom = etree.HTML(str(soup)) 
            # print("//////////dom/////////", etree.tostring(dom))
    
            titles=dom.xpath('//*[@class="title1"]/text()')
            # print("///////////title///////////",titles,len(titles))
            contents=dom.xpath('//div[@dir="rtl"]/text()')
            # print("////////////////contents///////////",contents,len(contents))
            images=dom.xpath('//img[@id="ctl00_ctl34_g_a18aeaf6_582e_4f53_a965_7edb5b850eb7_GridView1_ctl02__newsImage"]/@src')
            print("//////////////images////////////////",images,len(images))
        
            for title,content,image in zip(titles,contents,images):
                image="https://www.fatwa.gov.kw"+str(image)
                print("/////////////////////",image,"\n",content,"\n",title)
                print("/////////////////page/////////////////",page,len(page))
                # yield scrapy.Request(url=page,callback=self.data_saver,meta={'title':str(title),'content':str(content),'image':image},dont_filter=True)
                yield scrapy.Request(url=page,callback=self.data_saver,dont_filter=True,meta={'title':str(title),'content':str(content),'image':image,"current_url":page,"catagory":catagori,"main_category":main_categor,"sub_category":sub_categor,"platform":platfor,"media_type":media_typ,"urgency":urgenc})

    def data_saver(self,response):
        legal_advice_item=GeneralItem() 
        now = datetime.now() # current date and time
        date = now.strftime("%Y:%m:%d %H:%M:%S")
        legal_advice_item["news_agency_name"]="Legal Advice and Legislation "
        legal_advice_item["page_url"]=response.meta["current_url"]
        legal_advice_item["category"]=response.meta["catagory"]
        legal_advice_item["title"]=response.meta["title"]
        legal_advice_item["contents"]=response.meta["content"]
        legal_advice_item["image_url"]=response.meta["image"]
        legal_advice_item["date"]=date
        legal_advice_item["author_name"]="Legal Advice and Legislation"
        legal_advice_item["main_category"]=response.meta["main_category"]
        legal_advice_item["sub_category"]=response.meta["sub_category"]
        legal_advice_item["platform"]=response.meta["platform"]
        legal_advice_item["media_type"]=response.meta["media_type"]
        legal_advice_item["urgency"]=response.meta["urgency"]
        legal_advice_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        legal_advice_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        legal_advice_item["deleted_at"]=None
        yield legal_advice_item




