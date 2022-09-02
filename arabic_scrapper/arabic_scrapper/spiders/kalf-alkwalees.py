import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("kalf_alkwalees",False)
now = datetime_now_isoformat()

class KalfAlkwaleesSpider(scrapy.Spider):
    name = 'kalf_alkwalees'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//h2[@class='post-title']/a/@href"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):
    
        contents = response.xpath("//div[@class='entry']/section/p[@class='ar']/text()").extract_first()
        if(contents == None):
            contents = response.xpath("//div[@class='entry']/div[@class='desc']/div[@id='pastingspan1']/text()").extract_first()
            if(contents == None):
                contents = response.xpath("//div[@class='entry']/p/text()").extract_first()
        
        yield {
                "news_agency_name": self.name,
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :  response.xpath("//span[@itemprop='name']/text()").extract_first(),
                "contents": contents,
                "date" :  parser_parse_isoformat(response.xpath("//span[@class='tie-date']/text()").extract_first()),
                "author_name" :response.xpath("//span[@class='post-meta-author']/text()").extract_first(),
                "image_url" : response.xpath("//div[@class='single-post-thumb']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
