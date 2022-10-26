from slugify import slugify

def write_file(agency_name,generated_text):

    file_name = agency_name.replace(" ","_").lower()
    with open(f'./arabic_scrapper/{file_name}.py',"w") as f:   #change path to server config
        f.write(generated_text)

    f.close()

def template_text(agency_name,news_card_selector,title,contents,date,author,image_url):
    
    class_name = agency_name.capitalize()
    spider_name = slugify(agency_name)

    string_template = f'''import scrapy
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.helper import load_dataset_lists, date_today, parser_parse_isoformat, datetime_now_isoformat, agos_changer, translate_text,  selenium_path



news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency = load_dataset_lists("{agency_name}")
now = datetime_now_isoformat()

class {class_name}Spider(scrapy.Spider):
    name = '{spider_name}'
    start_urls = news_sites_list

    def start_requests(self):
        
        for i in range(len(self.start_urls)):
             yield scrapy.Request(url = self.start_urls[i], callback = self.parse, meta = {{'category_english': categories_english[i],"main_category": main_category[i],"sub_category": sub_category[i],"platform": platform[i],"media_type": media_type[i],"urgency": urgency[i]}})
    
    def parse(self, response):

        card_selector = "{news_card_selector}"
        for url in response.xpath(card_selector).extract():

            yield scrapy.Request(url = url, callback = self.parse_page, meta = response.meta)
    

    def parse_page(self,response):

        
        contents = " ".join({contents})

        news_item = GeneralItem()
      
        news_item["news_agency_name"] = "{agency_name}"
        news_item["page_url"] = response.url
        news_item["category"] = response.meta["catagory"]
        news_item["title"] = response.xpath("{title}").extract_first(),
        news_item["contents"] = contents,
        news_item["image_url"] = response.xpath("{image_url}").extract_first(),
        news_item["date"] = {date},
        news_item["author_name"] = response.xpath("{author}").extract_first(),
        
        news_item["main_category"] = response.meta["main_category"]
        news_item["sub_category"] = response.meta["sub_category"]
        news_item["platform"] = response.meta["platform"]
        news_item["media_type"] = response.meta["media_type"]
        news_item["urgency"] = response.meta["urgency"]
        news_item["created_at"] = str(now.strftime("%Y:%m:%d %H:%M:%S"))
        news_item["updated_at"] = str(now.strftime("%Y:%m:%d %H:%M:%S"))
        news_item["deleted_at"] = None

        yield news_item
    '''
        

    return string_template



if __name__ == "__main__":

    agency_name = input("Agency Name: ")
    news_card_selector = input("Individual News Article XPath: ")
    title = input("Title XPath: ")  
    content_count = int(input("Number of XPaths for extracting contents(enter each XPath in new line): "))
    contents_string = ""
    while(content_count != 0):
        contents = input("Contents XPath: ") 
        if(content_count > 1):
            contents_string = contents_string + f'response.xpath("{contents}").extract_first() + '
        else:
            contents_string = contents_string + f'response.xpath("{contents}").extract_first()'
        
        content_count = content_count - 1

    contents = contents_string

    date = input("Date XPath: ")
    arabic_date = input("Date contains Arabic?(Y/N): ")
    author = input("Author XPath: ")
    image_url = input("Image Url XPath: ")

   

    if(arabic_date.lower() == "y"):
        date  = f'parser_parse_isoformat(translate_text(response.xpath("{date}").extract_first()))'
    elif(arabic_date.lower() == "n"):
        date  = f'parser_parse_isoformat(response.xpath("{date}").extract_first())'
    


    generated_text = template_text(agency_name,news_card_selector,title,contents,date,author,image_url)

    write_file(agency_name,generated_text)











