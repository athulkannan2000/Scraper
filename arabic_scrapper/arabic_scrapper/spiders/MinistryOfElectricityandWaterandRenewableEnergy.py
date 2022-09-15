import scrapy
from arabic_scrapper.helper import load_dataset_lists, datetime_now_isoformat, parser_parse_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Ministry Of Electricity and Water and Renewable Energy")
now = datetime_now_isoformat()

class SoutalkhaleejSpider(scrapy.Spider):
    name = "ministry-of-electricity-and-Water-and-renewable-energy"
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='row news_row']/div/a/@href"  
        for url in response.xpath(card_selector).extract():

            url = f'https://www.mew.gov.kw{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def date_formatter(self,value):

        parsed_date = parser_parse_isoformat(value)
        if parsed_date:
             return parsed_date
        else: 
            return datetime_now_isoformat()
    

    def parse_page(self,response):

        image_url = response.xpath("//section[@class='default_page news_details_page']/a[@class='thumbnail img_round']/img/@src").extract_first()
        if(image_url != None):
            image_url = "https://www.mew.gov.kw/" + image_url

    
        yield ({
                "news_agency_name": "Ministry Of Electricity and Water and Renewable Energy",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :  response.xpath("//section[@class='default_page news_details_page']/a[@class='news_title']/text()").extract_first(),
                "contents": response.xpath("//section[@class='default_page news_details_page']/p/text()").extract_first(),
                "date" :  parser_parse_isoformat(response.xpath("//section[@class='default_page news_details_page']/a[@class='news_date']/text()").extract_first()),
                "author_name" : "Ministry Of Electricity and Water and Renewable Energy",
                "image_url" : image_url,

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         }) 
