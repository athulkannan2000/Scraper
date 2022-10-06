from importlib.resources import contents
import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, translate_text, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("aljarida")
now = datetime_now_isoformat()

class AljaridaSpider(scrapy.Spider):
    name = 'aljarida'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})

    def parse(self, response):

        card_selector = "//div[@class='text']/a/@href"
        for url in response.xpath(card_selector).extract():
            
            if(url[0 == "/"]):
                url = f'https://www.aljarida.com{url}'
                yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
   

    def dateformatter(self,value):
        return parser_parse_isoformat(translate_text(value))

    def parse_page(self,response):

        contents = response.xpath("//section[@id='paragraphs']/p/text()").extract()
        contents = "\n\n".join(contents)

        yield {
                "news_agency_name": self.name,
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : response.xpath("//div[@class='title']/h1/text()").extract_first(),
                "contents": contents,
                "date" :  self.dateformatter(response.xpath("//li[@class='time']/span[@class='date']/text()").extract_first()),
                "author_name" : response.xpath("//li[@class='writers']/a[@class='tagname']/text()").extract_first(),
                "image_url" : response.xpath("//span[@class='img']/span[2]/@data-src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
        } 
