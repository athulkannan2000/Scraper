import scrapy
import pandas as pd
from scrapy import Selector
from arabic_scrapper.helper import load_dataset_lists, date_today, datetime_now_isoformat

"""
This site has robots.txt setup to block crawlers. Currently ROBOTSTXT_OBEY = False in settings.py 
"""

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alsabah",False)
now = datetime_now_isoformat()
news_sites_list = []

site_categories = ["local","parliament","municipality","economy","sport","entertainment","articles"]

for category in site_categories:
    news_sites_list.append(f'http://www.alsabahpress.com/{category}/{date_today()}/')

class AlsabahSpider(scrapy.Spider):
    name = 'alsabah'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})

    def parse(self, response):

        card_selector = response.xpath("//div[@class='article']").extract()

        for text in card_selector:

            sel = Selector(text = text)
            yield {
                "news_agency_name": self.name,
                "page_url" : response.url + sel.xpath("//div[@class='article']/h1/a/@href").extract_first(),
                "category" : response.meta["category_english"],
                "title" : sel.xpath("//div[@class='article']/h1/a/text()").extract_first(),
                "contents": sel.xpath("//div[@class='article']/p/text()").extract_first(),
                "date" : datetime_now_isoformat(),
                "author_name" : None,
                "image_url" : "http://www.alsabahpress.com/" + sel.xpath("//div[@class='article']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
            }

   



