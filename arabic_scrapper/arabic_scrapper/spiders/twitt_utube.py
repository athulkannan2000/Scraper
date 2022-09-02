from unicodedata import category
import tweepy
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime

from arabic_scrapper.items import GeneralItem

GeneralItem()

# dataset=news_list()
# dataset=dataset.loc[dataset["Platform -EN"]=="Twitter"]

# site_list=dataset["Hyper link"].to_list() #list of sites to scrap
# catagory=dataset["Category -EN"].to_list()
# main_category=dataset["Main Category EN"].to_list()
# sub_category=dataset["Sub Category En"].to_list()
# platform=dataset["Platform -EN"].to_list()
# media_type =dataset["Media or Text - EN"].to_list()
# urgency= dataset["Urgency"].to_list()

# print(len(site_list),len(category),len(main_category))
"""
load_dotenv()
api_key = os.getenv("you_api_key")
api_key_secret =os.getenv("you_api_key_secret")
access_token = os.getenv("you_access_token")
access_token_secret = os.getenv("you_access_token_secret")
auth = tweepy.OAuthHandler(api_key,api_key_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)
try:
    api.verify_credentials()
    print('Successful Authentication')
except:
    print('###############Failed authentication################')
user = api.get_user(screen_name=username) # Store user as a variable
# Get user Twitter statistics
# print(f"user.followers_count: ", user.followers_count)
# print(f"user.listed_count: ", user.listed_count)
# print(f"user.statuses_count: ", user.statuses_count)
tweets = api.user_timeline(id=username, count=10)
print("$$$$$$$$$$$$$$$$$$$$$$NO of tweets$$$$$$$$$$$$$$$$$$",len(tweets))
# created_at=[]
# tweet_id=[]
# tweet_text=[]
for tweet in tweets:
    created=str(parser.parse(str(tweet.created_at)))
    id="https://twitter.com/twitter/statuses/"+str(tweet.id)
    try:
        tw_text=tweet.text
    except:
        tw_text=tweet.full_text
    # created_at.append(created)
    # tweet_id.append(id)
    # tweet_text.append(tw_text)
    # return created_at,tweet_text,tweet_id
    print("$$$$$$$$$$$$$$",type(tweet_id),type(tweet_text),type(page_url),"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    db_item=GeneralItem()
    now = datetime.now()
    # print("/////////////////",date)
    db_item["news_agency_name"]="alanba "
    db_item["page_url"]=str(page_url)
    db_item["category"]=str(cat)
    db_item["title"]=None 
    db_item["contents"]=None
    db_item["image_url"]=None
    db_item["date"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
    db_item["author_name"]=None
    db_item["main_category"]=str(main_cat)
    db_item["sub_category"]=str(sub_cat)
    db_item["platform"]=str(plat)
    db_item["media_type"]=str(media_type)
    db_item["urgency"]=str(urgency)
    db_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
    db_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
    db_item["deleted_at"]=None
    db_item["tweet_created_at"]=str(created_at)
    db_item["tweet_text"]=str(tweet_text)
    db_item["tweet_id"]=str(tweet_id)
    print("$$$$$$$$$$$$$$$$$$$$$ OOOOOKKKKKK saved sucessfully $$$$$$$$$$$$$$$$$$$$$$$:\n",db_item)
    yield db_item
    # # return created_at,tweet_text,tweet_id

    """