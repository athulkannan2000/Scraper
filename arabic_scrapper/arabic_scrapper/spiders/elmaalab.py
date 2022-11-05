import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists, date_today, parser_parse_isoformat, datetime_now_isoformat, agos_changer, translate_text,  selenium_path



news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("Elmaalab")
now = datetime_now_isoformat()

class ElmaalabSpider(scrapy.Spider):
    name = 'elmaalab'
    start_urls = news_sites_list
    
    def start_requests(self):
        print("thisss: ",self.start_urls)
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//h2[@class='post-title']/a/@href"
        for url in response.xpath(card_selector).extract():

            url = f'https://elmaalab.com{url}'
            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        contents = "\n\n".join( response.xpath("//div[@class='entry-content entry clearfix']/p/text()").extract() +
                                response.xpath("//div[@class='entry-content entry clearfix']/div/text()").extract()
                    )

        news_item = GeneralItem()
      
        news_item["news_agency_name"] = "Elmaalab"
        news_item["page_url"] = response.url
        news_item["category"] = response.meta["category_english"]
        news_item["title"] = response.xpath('//*[@class="post-title entry-title"]/text()').extract_first()
        news_item["contents"] = contents
        news_item["image_url"] = "https:" + str(response.xpath("//div[@class='featured-area-inner']/figure[@class='single-featured-image']/img/@src").extract_first())
        news_item["date"] = parser_parse_isoformat(translate_text(response.xpath("//div[@class='post-meta clearfix']/span[@class='date meta-item tie-icon']/text()").extract_first()))
        news_item["author_name"] = response.xpath('Elmaalab').extract_first()
        
        news_item["main_category"] = response.meta["main_category"]
        news_item["sub_category"] = response.meta["sub_category"]
        news_item["platform"] = response.meta["platform"]
        news_item["media_type"] = response.meta["media_type"]
        news_item["urgency"] = response.meta["urgency"]
        news_item["created_at"] = now
        news_item["updated_at"] = now
        news_item["deleted_at"] = None

        yield news_item
    