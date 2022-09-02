import scrapy
import pandas as pd
from arabic_scrapper.helper import parser_parse_isoformat, translate_text, load_dataset_lists, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("parliament aldustor agency",False)
now = datetime_now_isoformat()

class AlwasatSpider(scrapy.Spider):
    name = 'aldustor'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='relative']/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'http://aldustor.kna.kw{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)


    def dateformatter(self,value):
        if(value == None):
            return value
        else:
            return parser_parse_isoformat(translate_text(value[0]))

    def parse_page(self,response):

        data = response.xpath("//div[@class='article_content article_contents2']/div/text()").extract_first()
        if(data):
            data = data.split(" |")
            content = " ".join(data)
        else:
            data = None
            content = None

        yield  {
                "news_agency_name": "parliament aldustor agency",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : response.xpath("//h1[@class='entry_title']/span/text()").extract_first(),
                "contents":  content,
                "date" :  self.dateformatter(data),
                "author_name" : "parliament aldustor agency",
                "image_url" : "http://aldustor.kna.kw" + response.xpath("//div[@class='article_image']/img/@src").extract_first(),
         
                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 