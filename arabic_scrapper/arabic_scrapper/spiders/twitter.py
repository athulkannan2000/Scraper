import scrapy
import pandas as pd
import tweepy
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.pipelines import ArabicScrapperPipeline
import re

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
    #print('Successful Authentication')
except:
    print('###############Failed authentication################')
     
now = datetime.now()

# exp='[\u0627-\u064a0-9A-Za-z]+'

p = re.compile('[a-z]+')
at = re.compile('@[a-z]+')
#h = re.compile('#[a-zA-Z]+')
h = re.compile('#')
link_remover=r'http://\S+|https://\S+|www.\S+'
# hastag_remover=r'#[\u0627-\u064a0-9A-Za-z]+'
hash=r'#'

class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    def start_requests(self):
        pipeline=ArabicScrapperPipeline()
        for name,page_url,cat,main_cat,sub_cat,plat,media_typ,urgenc in zip(names,site_list,catagory,main_category,sub_category,platform,media_type,urgency):
            #print(page_url,type(page_url))
            username=page_url.split("/")[-1]
            print('####### UserName #######',username)
            try:
                # print("$$$$$$$$$$$$$$$ Username $$$$$$$$$$$$$$$$$$",username)
                user = api.get_user(screen_name=username) # Store user as a variable
                # print("########## page_url #############",page_url)
            except: #some of the users doesn't exist so that may result in 404
                continue
            # print("############### User Name :")
            # tweets = api.user_timeline(id=username, count=10)
            tweets = api.user_timeline(id=username, tweet_mode='extended',count=10) #### New
            # print("$$$$$$$$$$$$$$$$$$$$$$NO of tweets$$$$$$$$$$$$$$$$$$",len(tweets))
            for tweet in tweets:

                created=str(parser.parse(str(tweet.created_at)))
                id="https://twitter.com/twitter/statuses/"+str(tweet.id)

                # print("id:",id,"\nthis test: ",tweet.text)
                try:
                    tw_text=tweet.text
                    tw_text=re.sub(link_remover,"",tw_text)
                    # tw_text=" ".join(re.findall(exp,tw_text)) #finding only arabic
                except:
                    tw_text=tweet.full_text
                    tw_text=re.sub(link_remover,"",tw_text)
                    # tw_text=" ".join(re.findall(exp,tw_text))

                media_type_="text"
                # print("AFTER test: ",str(tw_text))

                try:
                    type = tweet.extended_entities["media"][0]["type"] #added try beacuse in some response extended entities is not there it is present only if the tweet contains image or video
                                     
                except:
                    type="text"
                    pass
                
                if type=="video":
                    media_type_="media"

                try:
                    video_url=tweet.extended_entities["media"][0]["video_info"]["variants"][0]["url"]
                except :
                    video_url=None

                try:
                    image_urls=[]
                    for i in tweet.extended_entities["media"]:
                        image_urls.append(i["media_url"])
                    image_urls=",".join(image_urls)
                except:
                    image_urls=None

                try:
                    if tweet.retweeted_status:
                        print("$$$$$$$$$$$$ its a retweet $$$$$$$$$$$$$$$")
                        continue
                    else:
                        pass
                except:
                    pass

                #################### Code to eliminate tweets which has more than 4 english words ####################
                st=str(tw_text)
                text=re.sub(link_remover,"",st)
                st=str(text)
                x = st.split()
                latest_text=[]
                en_w_count=0
                for i in x:
                    # print("#### words ######",i)
                    n = p.match(i)
                    if n:
                        en_w_count = en_w_count+1                    
                    elif at.match(i):
                        continue
                    elif h.match(i):
                        i=i[1:]
                    #m = at.match(i) 
                    #o = h.match(i)
                    latest_text.append(i)
                
                 
                print("# of Eng_words: ",en_w_count)

                text=re.sub(link_remover,"",tweet.full_text)
                #text=re.sub(p,"",text)
                text=re.sub(at,"",text)
                text=re.sub(h,"",text)
                # text=re.sub(hastag_remover,'',text) #removes entire hashtag
                text=re.sub(hash,'',text) #removes only hash not entire hastag
                # --------------------Update on 21 Nov------------------------

                text = ' '.join(latest_text)
                if en_w_count>4:
                  continue

                pipeline=ArabicScrapperPipeline()
                pipeline.process_item(item={
                    "news_agency_name":name,
                    "page_url": page_url,
                    "category": cat,
                    "title":str(tw_text),     
                    "contents":None,
                    "image_url":image_urls,
                    "date":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                    "author_name":None,
                    "main_category":main_cat,
                    "sub_category":sub_cat,
                    "platform":plat,    
                    "media_type":media_type_,
                    "urgency":urgenc,
                    "created_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                    "updated_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                    "deleted_at":None,
                    "tweet_created_at":str(created),
                    "tweet_text":text,
                    "tweet_id":str(id),
                    "vdo_title":None,
                    "vdo_description":None,
                    "vdo_published_at":None,
                    "vdo_thumbnail":None,
                    "vdo_url":video_url
                },spider="twitter")
                yield scrapy.Request(url="https://google.com",callback=self.dummy)
                # yield scrapy.Request(url="https://google.com",callback=self.details_saver,dont_filter=True,meta={"tweet":tweet,"name":name,"page_url":page_url,"cat":cat,"main_cat":main_cat,"sub_cat":sub_cat,"plat":plat,"media_typ":media_typ,"urgency":urgenc})
                
    def dummy(self,response):
        return None

    