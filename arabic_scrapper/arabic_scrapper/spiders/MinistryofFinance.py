import scrapy
from arabic_scrapper.helper import load_dataset_lists,  parser_parse_isoformat, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Ministry of Finance",False)
now = datetime_now_isoformat()

class MinistryofFinanceSpider(scrapy.Spider):
    name = 'Ministry-of-Finance'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//a[contains(@id,'ContentPlaceHolder1_grdNews_hlNews_')]/@href"
        for url in response.xpath(card_selector).extract():

            url  = f'https://www.mof.gov.kw/TheMOFNews/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        title_and_date = response.xpath("//span[@id='ContentPlaceHolder1_lblk']/text()").extract_first()
        title_and_date = title_and_date.split(' :: ')
        date = title_and_date[0]
        title = title_and_date[1]

        image_url = response.xpath("//img[@id='ContentPlaceHolder1_imgNews']/@src").extract_first()
        if(image_url != None):
            image_url = image_url.replace("../","")
            image_url = "https://www.mof.gov.kw/" + image_url

        yield {
                "news_agency_name": "Ministry of Finance",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :  title,
                "contents": response.xpath("//span[@id='ContentPlaceHolder1_lblNewsDetails1']/text()").extract_first(),
                "date" :  parser_parse_isoformat(date),
                "author_name" : "Ministry of Finance",
                "image_url" : image_url,

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
