import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, translate_text, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("The official account of Communication & IT Regulatory Authority",False)
now = datetime_now_isoformat()

class CommunicationAndITRegulatoryAuthoritySpider(scrapy.Spider):
    name = 'communication-and-IT-regulatory-authority'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='section group']/div[@class='col']/a/@href"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        contents = response.xpath("//div[contains(@class,'ExternalClass')]/div/text()").extract_first()
        if(contents == None):
            contents = response.xpath("//div[contains(@class,'ExternalClass')]/span/text()").extract_first()
            if(contents == None):
                contents = response.xpath("//div[contains(@class,'ExternalClass')]/p/text()").extract_first()
                if(contents == None):
                    contents = response.xpath("//div[contains(@class,'ExternalClass')]/p/span/text()").extract_first()
        
        date = translate_text(response.xpath("//div[@class='DateTime']/text()").extract_first())
        date = date.split(": ")

        yield ({
                "news_agency_name": "The official account of Communication & IT Regulatory Authority",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   response.xpath("//div[@class='col span_4_of_6']/h2/text()").extract_first(),
                "contents": contents,
                "date" :   parser_parse_isoformat(date[1]),
                "author_name" : None,
                "image_url" : response.xpath("//div[@class='rsContent']/a/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         })
