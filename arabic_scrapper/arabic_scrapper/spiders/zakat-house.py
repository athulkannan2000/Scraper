import scrapy
from arabic_scrapper.helper import load_dataset_lists,  parser_parse_isoformat, datetime_now_isoformat


news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("zakat house",False)
now = datetime_now_isoformat()

class ZakatHouseSpider(scrapy.Spider):
    name = 'zakat-house'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//div[@class='new_contact_left_text']/h3/a/@href"
        for url in response.xpath(card_selector).extract():

            url  = f'https://www.zakathouse.org.kw/{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)

    def parse_page(self,response):

        contents = response.xpath("//div[@class='zdes']/p[@dir='RTL']/span/text()").extract_first()
        if(contents == None):
            contents = response.xpath("//div[@class='zdes']/p[@style='text-align: right;']/span/text()").extract_first()
            if(contents == None):
                contents = response.xpath("//div[@class='zdes']/p/span/text()").extract_first()
                if(contents == None):
                    contents = response.xpath("//div[@class='zdes']/p/text()").extract_first()

        yield ({ 
                "news_agency_name": "zakat house",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" :   response.xpath("//div[@class='ztitle']/text()").extract_first(),
                "contents": contents,
                "date" :   parser_parse_isoformat(response.xpath("//div[@id='ctl00_ctl00_ContentPlaceHolderhome_ContentPlaceHolder1_Panel2']/text()").extract_first()),
                "author_name" : "zakat house",
                "image_url" : "https://www.zakathouse.org.kw/" + response.xpath("//section[@class='banner_aream']/img/@src").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         })
