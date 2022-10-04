from email.mime import image
from importlib.resources import contents
import scrapy
from arabic_scrapper.helper import load_dataset_lists, parser_parse_isoformat, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("KUNA")
now = datetime_now_isoformat()

class KunaSpider(scrapy.Spider):
    name = 'kuna'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='row']/div[2]/h4/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://www.kuna.net.kw/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        contents =  response.xpath("//div[@class='post-content']/p/text()").extract()
        if(not len(contents)):
            contents = response.xpath("//div[@class='post-content']/text()").extract()
            if(not len(contents)):
                contents = response.xpath("//div[@class='post-content']/p/strong/text()").extract()
                if(not len(contents)):
                    contents = response.xpath("//div[@class='post-content']/p[2]/text()").extract()

        contents = " ".join(contents)

        contents = contents.replace('\r','')
        contents = contents.replace('\n','')
        contents = contents.replace('\t','')

        if(contents == ''): contents = None

        image_url = response.xpath("//figure[@class='post-img']/img/@src").extract_first()
        if(image_url != None):
            image_url = image_url.replace('./','')
            image_url = "https://www.kuna.net.kw/" + image_url

        yield ({ 
                "news_agency_name": "KUNA",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   response.xpath("//h1[@class='post-title']/a/text()").extract_first(),
                "contents": contents,
                "date" :   parser_parse_isoformat(response.xpath("//div[@class='article-info-bar']/time/text()").extract_first()),
                "author_name" : "KUNA",
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
