import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, translate_text, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Kuwait Foundation for the Advancement of Sciences",False)
now = datetime_now_isoformat()

class KuwaitFoundationfortheAdvancementofSciencesSpider(scrapy.Spider):
    name = 'kuwait-foundation-for-the-advancement-of-sciences'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):

            yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@id='news-title']/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://www.kfas.org{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        contents = response.xpath("//div[@class='col-12 text-justify']/p[2]/text()").extract_first()
        if(contents == None):
            contents = response.xpath("//div[@class='col-12 text-justify']/ul/li/p/text()").extract_first()

        date = response.xpath("//div[@class='col-12 text-justify']/div[@id='date-posted']/p/text()").extract()[1]
        date = date.replace('\n','')
        date = date.replace('\r','')

        image_url = response.xpath("//div[@class='col-12 text-justify']/img[@class='w-100']/@src").extract_first()
        if(image_url == None):
            image_url = response.xpath("//div[@class='col-12 text-justify']/p[@class='text-center']/img/@src").extract_first()
        if(image_url != None):
            image_url = "https://www.kfas.org" + image_url
        
        yield ({ 
                "news_agency_name": "Kuwait Foundation for the Advancement of Sciences",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   response.xpath("//h2[@class='m-b-0']/text()").extract_first(),
                "contents": contents,
                "date" : parser_parse_isoformat(translate_text(date)),
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
         })
