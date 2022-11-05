import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists, date_today, parser_parse_isoformat, datetime_now_isoformat, agos_changer, translate_text,  selenium_path



news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("bourashed")
now = datetime_now_isoformat()

class BourashedSpider(scrapy.Spider):
    name = 'bourashed'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})
    
    def parse(self, response):

        card_selector = "//h4[@class='entry-title title']/a/@href"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        
        contents = "\n\n".join(response.xpath("//div[@class='col-lg-12 col-md-12 col-xs-12 col-sm-12']/p/text()").extract())

        title = response.xpath("//h1[@class='title single']/a/text()").extract_first()
        title = title.strip()

        news_item = GeneralItem()
      
        news_item["news_agency_name"] = "bourashed"
        news_item["page_url"] = response.url
        news_item["category"] = response.meta["category_english"]
        news_item["title"] = title
        news_item["contents"] = contents
        news_item["image_url"] = response.xpath("//div[@class='col-md-9 col-lg-9']/img[@class='img-fluid wp-post-image']/@src").extract_first()
        news_item["date"] = parser_parse_isoformat(response.xpath("//div[@class='media-body']/span[@class='mg-blog-date']/text()").extract_first())
        news_item["author_name"] = response.xpath("//div[@class='media-body']/h4/a/text()").extract_first()
        
        news_item["main_category"] = response.meta["main_category"]
        news_item["sub_category"] = response.meta["sub_category"]
        news_item["platform"] = response.meta["platform"]
        news_item["media_type"] = response.meta["media_type"]
        news_item["urgency"] = response.meta["urgency"]
        news_item["created_at"] = now
        news_item["updated_at"] = now
        news_item["deleted_at"] = None

        yield news_item
    