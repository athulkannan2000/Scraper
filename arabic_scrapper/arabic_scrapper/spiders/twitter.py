from ast import YieldFrom
import scrapy
import pandas as pd
import tweepy
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.pipelines import ArabicScrapperPipeline

dataset=pd.read_csv('arabic_scrapper/spiders/News Aggregator Websites & Categories list - EN-AR - version 1 (1).xlsx - GOV and Private.csv')
dataset=dataset.loc[dataset["Platform -EN"]=="Twitter"]

names=dataset["News Agency in English"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
site_list=dataset["Hyper link"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list() #list of sites to scrap
catagory=dataset["Category -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
main_category=dataset["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
sub_category=dataset["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
platform=dataset["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
media_type=dataset["Media or Text - EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
urgency= dataset["Urgency"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()

api_key = "S40EuX45JCpUjH53w9d4vNruz"
api_key_secret = "cQLbpVD6WagtDADsxKZPaiXA0hBs5A6LApvbcpwt7P00d0fzlk"
access_token = "1129957250414419968-cOpUfqteqHeCglwAzlItqHuo0ExnhY"
access_token_secret = "fZM3ebXBi2uSnP0vXKA67hsnmTwxpFPeeXU9xrqq0y6mR"

auth = tweepy.OAuthHandler(api_key,api_key_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)
try:
    api.verify_credentials()
    print('Successful Authentication')
except:
    print('###############Failed authentication################')
now = datetime.now()

class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    def start_requests(self):
        for name,page_url,cat,main_cat,sub_cat,plat,media_typ,urgenc in zip(names,site_list,catagory,main_category,sub_category,platform,media_type,urgency):
            print(page_url,type(page_url))
            username=page_url.split("/")[-1]
            try:
                user = api.get_user(screen_name=username) # Store user as a variable
            except: #some of the users doesn't exist so that may result in 404
                continue
            tweets = api.user_timeline(id=username, count=10)
            print("$$$$$$$$$$$$$$$$$$$$$$NO of tweets$$$$$$$$$$$$$$$$$$",len(tweets))
            for tweet in tweets:
                yield scrapy.Request(url="https://google.com",callback=self.details_saver,dont_filter=True,meta={"tweet":tweet,"name":name,"page_url":page_url,"cat":cat,"main_cat":main_cat,"sub_cat":sub_cat,"plat":plat,"media_typ":media_typ,"urgency":urgenc})
                
                
                
    def details_saver(self,response):
        # created=str(parser.parse(str(tweet.created_at)))
        created=str(parser.parse(str(response.meta["tweet"].created_at)))
        id="https://twitter.com/twitter/statuses/"+str(response.meta["tweet"].id)
        try:
            tw_text=response.meta["tweet"].text
        except:
            tw_text=response.meta["tweet"].full_tex
        # print("$$$$$$$$$$$$$$",type(id),type(tw_text),type(page_url),"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        db_item=GeneralItem()
        db_item["news_agency_name"]= response.meta["name"]
        db_item["page_url"]= response.meta["page_url"]
        db_item["category"]= response.meta["cat"]
        db_item["title"]=str(tw_text)     
        db_item["contents"]=None
        db_item["image_url"]=None
        db_item["date"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["author_name"]=None
        db_item["main_category"]=str(response.meta["main_cat"])
        db_item["sub_category"]=str(response.meta["sub_cat"])
        db_item["platform"]=str(response.meta["plat"])
        db_item["media_type"]=str(response.meta["media_typ"])
        db_item["urgency"]=str(response.meta["urgency"])
        db_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["deleted_at"]=None
        db_item["tweet_created_at"]=str(created) 
        db_item["tweet_text"]=str(tw_text)
        db_item["tweet_id"]=str(id)

        db_item["vdo_title"]=None
        db_item["vdo_description"]=None
        db_item["vdo_published_at"]=None
        db_item["vdo_thumbnail"]=None
        db_item["vdo_url"]=None
        print("$$$$$$$$$$$$$$$$$$$$$ OOOOOKKKKKK saved sucessfully $$$$$$$$$$$$$$$$$$$$$$$:\n",db_item)
        yield db_item
            # ea