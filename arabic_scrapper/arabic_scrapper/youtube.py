import pandas as pd
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime
from items import GeneralItem
from pipelines import ArabicScrapperPipeline
from googleapiclient.discovery import build
from items import GeneralItem
from pipelines import ArabicScrapperPipeline


dataset=pd.read_csv("spiders/News Aggregator Websites & Categories list - EN-AR - version 1 (1).xlsx - GOV and Private.csv")
dataset=dataset.loc[dataset["Platform -EN"]=="Youtube"]

names=dataset["News Agency in English"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
site_list=dataset["Hyper link"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list() #list of sites to scrap
catagory=dataset["Category -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
main_category=dataset["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
sub_category=dataset["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
platform=dataset["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
media_type=dataset["Media or Text - EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
urgency= dataset["Urgency"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()

api_key = "AIzaSyChf5BrFhvjm2lhht6jL7fkSoHsgwhRCmM"
api_service_name = "youtube"
api_version="v3"
youtube = build('youtube', 'v3', developerKey=api_key)

pipe=ArabicScrapperPipeline()
db_item=GeneralItem()

def scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,media_typ,urgenc):
    id_request = youtube.playlistItems().list(part = 'contentDetails',playlistId = playlist_id,maxResults = 2)
    try:
        id_response = id_request.execute()
    except :
        print(playlist_id,"###########playlist not existing#########")
        return None

    for res in range(len(id_response["items"])):
        video_id=id_response["items"][res]["contentDetails"]["videoId"]
        video_url="https://www.youtube.com/watch?v"+video_id
        print("\n video url",video_url)

        #getting video details
        vdo_info_request = youtube.videos().list(part='snippet, statistics',id = video_id)
        vdo_info_response = vdo_info_request.execute()
        # print(vdo_info_response["items"])
        title=vdo_info_response["items"][0]["snippet"]["title"]
        description=vdo_info_response["items"][0]["snippet"]["description"]
        published_at=vdo_info_response["items"][0]["snippet"]["publishedAt"]
        date=published_at.split("T")[0]
        time=published_at.split("T")[1]
        dt = str(parser.parse(date)).split(" ")[0]+" "+str(parser.parse(time)).split(" ")[1]
        image_url=vdo_info_response["items"][0]['snippet']['thumbnails']['default']['url'] 
        print("\ntitle :",type(title),"\n description :",type(description),"\npublished at :",type(published_at),"\n image_url :",type(image_url),"\n date and time",type(dt))
        now = datetime.now()
        db_item["news_agency_name"]=name
        db_item["page_url"]=site
        db_item["category"]=cat
        db_item["title"]=str(title)    
        db_item["contents"]=None
        db_item["image_url"]=None
        db_item["date"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["author_name"]=None
        db_item["main_category"]=str(main_cat)
        db_item["sub_category"]=str(sub_cat)
        db_item["platform"]=str(plat)
        db_item["media_type"]=str(media_typ)
        db_item["urgency"]=str(urgenc)
        db_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["deleted_at"]=None
        db_item["tweet_created_at"]=None
        db_item["tweet_text"]=None
        db_item["tweet_id"]=None

        db_item["vdo_title"]=title
        db_item["vdo_description"]=description
        db_item["vdo_published_at"]=dt
        db_item["vdo_thumbnail"]=image_url
        db_item["vdo_url"]=video_url
        print("$$$$$$$$$$$$$$$$$$$$$ OOOOOKKKKKK saved sucessfully $$$$$$$$$$$$$$$$$$$$$$$:\n",db_item)
        pipe.process_item(db_item,None)



for name,site,cat,main_cat,sub_cat,plat,med_type,urgency in zip(names,site_list,catagory,main_category,sub_category,platform,media_type,urgency):
    print(name,site,urgency)
    diff=site.split("/")[-1]
    if diff=="videos":
        channel_name=site.split("/")[-2]
        print("##############channel name###################",channel_name)
        request = youtube.channels().list(part = 'snippet, contentDetails,statistics',forUsername=channel_name)
        response= request.execute()
        try:
            playlist_id=response["items"][0]['contentDetails']['relatedPlaylists']['uploads']
        except KeyError:
            continue

        respon=scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)

        if response==None:
            continue
        
    else: # if the link cotains playlist id
        playlist_id=site.split("/")[3].split("=")[1]
        scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)
        

