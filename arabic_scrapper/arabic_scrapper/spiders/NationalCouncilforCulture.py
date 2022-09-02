import scrapy
from arabic_scrapper.helper import load_dataset_lists,  parser_parse_isoformat, translate_text, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("National Council for Culture",False)
now = datetime_now_isoformat()

class NationalCouncilforCultureSpider(scrapy.Spider):
    name = 'National-Council-for-Culture'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='col-lg-12 tag-box tag-box-v3 box-shadow shadow-effect-1 ']/p/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://www.nccal.gov.kw/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)


    def parse_page(self,response):
        
        image_url = response.xpath("//div[contains(@id,'newsItem_')]/div[@dir='rt1']/img/@src").extract_first()
        if(image_url == None):
            image_url = response.xpath("//div[contains(@id,'newsItem_')]/p/img/@src").extract_first()
            if(image_url == None):
                image_url = response.xpath("//div[contains(@id,'newsItem_')]/p/span/img/@src").extract_first()
                if(image_url == None):
                    image_url = response.xpath("//div[contains(@id,'newsItem_')]/div[@dir='ltr']/img/@src").extract_first()
                    if(image_url == None):
                        image_url = response.xpath("//div[contains(@id,'newsItem_')]/p[@style='text-align: center;']/strong/img/@src").extract_first()
                    
        if(image_url != None):
            image_url = "https://www.nccal.gov.kw/" + image_url
    
        contents = response.xpath("//div[contains(@id,'newsItem_')]/p/text()").extract()[1]
        if(contents == None):
            contents = response.xpath("//div[contains(@id,'newsItem_')]/p/br/text()").extract_first()
            if(contents == None):
                contents = response.xpath("//div[contains(@id,'newsItem_')]/p/strong/text()").extract_first()
        yield {
                "news_agency_name": "National Council for Culture",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :  response.xpath("//div[contains(@id,'newsItem_')]/h4/text()").extract_first(),
                "contents": contents,
                "date" :  parser_parse_isoformat(translate_text(response.xpath("//div[contains(@id,'newsItem_')]/h5/text()").extract_first())),
                "author_name" : None,
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
