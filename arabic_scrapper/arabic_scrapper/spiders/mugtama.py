import scrapy
from arabic_scrapper.helper import parser_parse_isoformat, translate_text, load_dataset_lists, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("mugtama magazine",False)
now = datetime_now_isoformat()

class MugtamaSpider(scrapy.Spider):
    name = 'mugtama-magazine'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//h3[@class='catItemTitle']/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://mugtama.com{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        title = response.xpath("//h2[@class='itemTitle']/text()").extract_first()
        title = title.replace('\n','')
        title = title.replace('\t','')
        title = title.replace(' ','')

        author = response.xpath("//span[@class='itemAuthor']/text()").extract_first()
        author = author.replace('\n','')
        author = author.replace('\t','')
    
        yield {
                "news_agency_name": "mugtama magazine",
                "page_url" : response.url,
                "category" : response.meta["category_english"],
                "title" : title,
                "contents": response.xpath("//div[@class='itemFullText']/p[@style='text-align: justify;']/strong/span/text()").extract_first(),
                "date" :  parser_parse_isoformat(translate_text(response.xpath("//span[@class='itemDateCreated']/text()").extract_first())),
                "author_name" : author,
                "image_url" : "https://mugtama.com/" + response.xpath("//span[@class='itemImage']/a/@href").extract_first(),

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
         } 
