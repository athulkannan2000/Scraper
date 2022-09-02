import scrapy
from arabic_scrapper.helper import parser_parse_isoformat, load_dataset_lists, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("al wasat",False)
now = datetime_now_isoformat()

class AlwasatSpider(scrapy.Spider):
    name = 'alwasat'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//a[@class='art_title']/@href"
        for url in response.xpath(card_selector).extract():
            
            url = f'http://www.alwasat.com.kw/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        yield {
                "news_agency_name": "al wasat",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : response.xpath("//span[@class='artside_small']/text()").extract_first(),
                "contents": response.xpath("//div[@class='Articlebody']/div[@id='pastingspan1']/span/text()").extract_first(),
                "date" : parser_parse_isoformat(response.xpath("//div[@align='right']/span[2]/font/text()").extract_first()),
                "author_name" : response.xpath("//span[@class='ArticleWriter']/text()").extract_first(),
                "image_url" : "http://www.alwasat.com.kw/" + response.xpath("//img[@class='imgBorder']/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
        } 
