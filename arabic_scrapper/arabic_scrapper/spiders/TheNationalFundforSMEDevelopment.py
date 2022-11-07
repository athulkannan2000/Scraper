import scrapy
from arabic_scrapper.helper import load_dataset_lists,  parser_parse_isoformat, translate_text, datetime_now_isoformat
from arabic_scrapper.helper import load_dataset_lists,selenium_path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml import etree
from datetime import datetime


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("The National Fund for SME Development")
now = datetime.now()

class TheNationalFundforSMEDevelopmentSpider(scrapy.Spider):
    name = 'The-National-Fund-for-SME-Development'
    start_urls = news_sites_list
    def __init__(self):
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chromedriver=selenium_path()


    def start_requests(self):
        for i in range(len(self.start_urls)):
            url = self.start_urls[i]
            driver = webdriver.Chrome(self.chromedriver,options=self.chrome_options)
            driver.delete_all_cookies()
            driver.get(url)
            # time.sleep(5)
            html=driver.page_source
            driver.quit()
            soup=BeautifulSoup(html,"lxml") #donwloading the entiring page 
            dom = etree.HTML(str(soup)) 
            urls=dom.xpath('//*[@class="event-title"]//a/@href')
            print("all urls",urls)
            for url in urls:
                page_url="https://www.nationalfund.gov.kw"+url
                yield scrapy.Request(url = page_url, callback = self.parse_page, meta = {'current_url':page_url,'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
                

    def parse_page(self,response):

        driver = webdriver.Chrome(self.chromedriver,options=self.chrome_options)
        driver.delete_all_cookies()
        driver.get(response.meta["current_url"])
        html=driver.page_source
        driver.quit()
        soup=BeautifulSoup(html,"lxml")
        dom = etree.HTML(str(soup))
        # print("########### went inside cards ############")

        image_url = "\n".join(dom.xpath("//div[@class='ps-current']//li//img/@src"))
        # print("########image url######",image_url,type(image_url))
        # if(image_url == None):
        #     image_url = dom.xpath("//div[@class='ps-current']/li[@class='elt_1']/img/@src")[0]

        contents = "\n\n".join(dom.xpath("//div[@class='program-row content-box']/div[@class='prog-detail']/p/text()")+
                                dom.xpath("//div[@class='program-row content-box']/h2/text()")+ 
                                dom.xpath("//div[@class='program-row content-box']/h3/text()")+ 
                                dom.xpath("//div[@class='program-row content-box']/div[@class='prog-detail']/p/strong/text()")+
                                dom.xpath("//h3/strong[@style='user-select: auto;']/text()"))
                            
        # print("#########contents#########",contents)
        
        # print("$$$$$$$$$$$$ Hey ############")
        date=parser_parse_isoformat(translate_text(dom.xpath("//div[@class='program-row content-box']/ul[@class='info']/li/text()")[0]))
        title=" ".join(dom.xpath("//div[@class='program-row content-box']/h3/text()"))
        # print("########### Title ############",title,type(title))


        yield ({ 
                "news_agency_name": "The National Fund for SME Development",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   title,
                "contents": contents, 
                "date" :   date,
                "author_name" : "The National Fund for SME Development",
                "image_url" :  image_url,

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": str(now.strftime("%Y:%m:%d %H:%M:%S")),
                "updated_at": now,
                "deleted_at": None
        })
