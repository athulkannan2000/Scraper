import json
import scrapy
from arabic_scrapper.helper import load_dataset_lists, datetime_now_isoformat

news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("alqabas",False)
now = datetime_now_isoformat()

news_categories_dict = {
    22 : "local news",
    28 : "parliament",
    25 : "legal news",
    3 : "economic news",
    17 : "articles",
    64759 : "culture",
    7 : "sport news"
}

urls_required = []
for key,value in news_categories_dict.items():
    urls_required.append(f"https://api.alqabas.com/api/landing/category/elastic/{key}?limit=24&isPremium=false")

class AlqabasSpider(scrapy.Spider):
    name = 'alqabas'
    start_urls = urls_required

    def start_requests(self):

        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]})

    def parse(self, response):

        response_text = json.loads(response.text)

        articles = response_text["data"]["result"][3]["data"] 

        for article in articles:

            yield {
                "news_agency_name": self.name,
                "page_url" : f'https://www.alqabas.com/article/{article["id"]}',
                "category" : response.meta["category_english"],
                "title" : article["title"],
                "contents": article["description"],
                "date" :  article["createdDate"],
                "author_name" : 'alqabas',
                "image_url" : article["image"],

                "main_category": response.meta["main_category"],
                "sub_category": response.meta["sub_category"],
                "platform": response.meta["platform"],
                "media_type": response.meta["media_type"],
                "urgency": response.meta["urgency"],
                "created_at": now,
                "updated_at": now,
                "deleted_at": None
            }


