import scrapy
from arabic_scrapper.helper import load_dataset_lists,selenium_path
from arabic_scrapper.items import GeneralItem
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml import etree
from datetime import datetime


site_list,catagory,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alwatan",False)
now = datetime.now()

class AlwatanSpider(scrapy.Spider):
    name = 'alwatan'
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
            dom = etree.HTML(str(soup)) 
            urls=dom.xpath('//*[@rel="nofollow"]//@href')
            for url in urls:
                page_url="http://alwatan.kuwait.tt/"+url
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

        contents=dom.xpath('//*[@id="divArtContent"]/text()')
        contents="".join(contents[0:len(contents)])


        alwatan_item=GeneralItem()
        alwatan_item["news_agency_name"]="alwatan"
        alwatan_item["page_url"]=response.meta["current_url"]
        alwatan_item["category"]=response.meta["catagory"]
        alwatan_item["title"]=str(dom.xpath('//*[@id="divMainTitle"]/text()')[0])
        alwatan_item["contents"]=contents
        alwatan_item["image_url"]=str(dom.xpath('//*[@id="test"]/tbody/tr[2]/td[1]/table/tbody/tr/td/table/tbody/tr[7]/td/table/tbody/tr/td[1]/table/tbody/tr[1]/td/img/@src')[0])
        alwatan_item["date"]=str(dom.xpath('//*[@class="WriterLink"]/text()')[1])
        alwatan_item["author_name"]="alwatan"
        alwatan_item["main_category"]=response.meta["main_category"]
        alwatan_item["sub_category"]=response.meta["sub_category"]
        alwatan_item["platform"]=response.meta["platform"]
        alwatan_item["media_type"]=response.meta["media_type"]
        alwatan_item["urgency"]=response.meta["urgency"]
        alwatan_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alwatan_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        alwatan_item["deleted_at"]=None

        yield alwatan_item

        