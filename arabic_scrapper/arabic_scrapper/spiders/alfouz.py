import scrapy
from arabic_scrapper.helper import load_dataset_lists, agos_changer, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alfouz",False)
now = datetime_now_isoformat()

class AlfouzSpider(scrapy.Spider):
    name = 'alfouz'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//h3[@class='post-title']/a/@href"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        yield {
                "news_agency_name": self.name,
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : response.xpath("//h1[@class='post-title entry-title']/text()").extract_first(),
                "contents":  response.xpath("//div[@class='entry-content entry clearfix']/p/text()").extract_first(),
                "date" : agos_changer(response.xpath("//span[@class='date meta-item']/span/text()").extract_first()),
                "author_name" : "alfouz",
                "image_url" : response.xpath("//figure[@class='single-featured-image']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
