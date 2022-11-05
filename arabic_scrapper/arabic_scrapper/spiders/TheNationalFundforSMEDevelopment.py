import scrapy
from arabic_scrapper.helper import load_dataset_lists,  parser_parse_isoformat, translate_text, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("The National Fund for SME Development")
now = datetime_now_isoformat()

class TheNationalFundforSMEDevelopmentSpider(scrapy.Spider):
    name = 'The-National-Fund-for-SME-Development'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//p[@class='event-title']/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://www.nationalfund.gov.kw{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        image_url = response.xpath("//div[@class='program-row content-box']/div[@class='prog-detail']/p/span/text()").extract_first()
        if(image_url == None):
            image_url = response.xpath("//div[@class='ps-current']/li[@class='elt_1']/img/@src").extract_first()

        contents = "\n\n".join(response.xpath("//div[@class='program-row content-box']/div[@class='prog-detail']/p/text()").extract() +
                response.xpath("//div[@class='program-row content-box']/h2/text()").extract() + response.xpath("//div[@class='program-row content-box']/h3/text()").extract() + 
                 response.xpath("//div[@class='program-row content-box']/div[@class='prog-detail']/p/strong/text()").extract() +
                 response.xpath("//h3/strong[@style='user-select: auto;']/text()").extract()  
        )

        yield ({ 
                "news_agency_name": "The National Fund for SME Development",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   response.xpath("//div[@class='program-row content-box']/h3/text()").extract_first(),
                "contents": contents, 
                "date" :   parser_parse_isoformat(translate_text(response.xpath("//div[@class='program-row content-box']/ul[@class='info']/li/text()").extract_first())),
                "author_name" : "The National Fund for SME Development",
                "image_url" :  image_url,

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
        })
