# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from attr import field
import scrapy
from scrapy.item import Item,Field 


class AlanbaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    topic_id=Field()
    news_agency_name=Field()
    page_url=Field()
    category=Field()
    title=Field()
    contents=Field()
    image_url=Field()
    date=Field()

class AlraiItem(scrapy.Item):
    topic_id=Field()
    news_agency_name=Field()
    page_url=Field()
    category=Field()
    title=Field()
    contents=Field()
    image_url=Field()
    date=Field()
    author_name=Field()