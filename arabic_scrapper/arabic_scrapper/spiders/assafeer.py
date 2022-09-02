import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, translate_text, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("assafeer newspaper",False)
now = datetime_now_isoformat()

class AssafeerSpider(scrapy.Spider):
    name = 'assafeer-newspaper'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//a[@class='ap-link']/@href"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        yield {
                "news_agency_name": "assafeer newspaper",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : response.xpath("//h1[@class='top-title']/text()").extract_first(),
                "contents":  response.xpath("//div[@class='entry']/p/text()").extract_first(),
                "date" :  parser_parse_isoformat(translate_text(response.xpath("//span[@class='ptime']/text()").extract_first())),
                "author_name" : "assafeer newspaper",
                "image_url" : response.xpath("//div[@class='top-media thumbnail']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
