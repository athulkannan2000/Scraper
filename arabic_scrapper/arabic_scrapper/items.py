# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from attr import field
import scrapy
from scrapy.item import Item,Field 


class GeneralItem(scrapy.Item):
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
    author_name=Field()
    main_category=Field()
    sub_category =Field()
    platform=Field()
    media_type =Field()
    urgency=Field()
    created_at=Field()
    updated_at =Field()
    deleted_at=Field()
    tweet_created_at=Field()
    tweet_text=Field()
    tweet_id=Field()
    vdo_title=Field()
    vdo_description=Field()
    vdo_published_at=Field()
    vdo_thumbnail=Field()
    vdo_url=Field()
    

