import scrapy
from arabic_scrapper.helper import parser_parse_isoformat, load_dataset_lists, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alwehda news")
now = datetime_now_isoformat()

class AlwehdaSpider(scrapy.Spider):
    name = 'alwehda'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//ul[@class='v_list']/li/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://alwehdanews.com/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):
 
        contents = response.xpath("//div[@class='story_content']/span/text()").extract_first()
        if(contents == None):
            contents = response.xpath("//div[@class='story_content']/div/text()").extract()[1]
            if(contents == None):
                contents = response.xpath("//div[@class='story_content']/div/span/text()").extract_first()
                # if(contents == None):
                #     contents = response.xpath("//div[@class='story_content']/div[2]/span/text()").extract_first() 

        yield {
                "news_agency_name": "alwehda news",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :  response.xpath("//h1[@class='tit_1']/text()").extract_first(),
                "contents": contents,
                "date" :  parser_parse_isoformat(response.xpath("//div[@class='m_i_1']/time/span/text()").extract_first()),
                "author_name" : "alwehda news",
                "image_url" : "https://alwehdanews.com/" + response.xpath("//div[@class='image_content']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
