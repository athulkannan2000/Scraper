# from email.mime import image
import scrapy
import pandas as pd
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime
from googleapiclient.discovery import build
from arabic_scrapper.items import GeneralItem
from arabic_scrapper.pipelines import ArabicScrapperPipeline

dataset=pd.read_csv("arabic_scrapper/spiders/News Aggregator Websites & Categories list - EN-AR - version 1 (1).xlsx - GOV and Private.csv")
dataset=dataset.loc[dataset["Platform -EN"]=="Youtube"]

names=dataset["News Agency in English"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
site_list=dataset["Hyper link"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list() #list of sites to scrap
catagory=dataset["Category -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
main_category=dataset["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
sub_category=dataset["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
platform=dataset["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
media_type=dataset["Media or Text - EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()
urgenci= dataset["Urgency"].replace(to_replace= ['\r','\n'], value= '', regex=True).to_list()

api_key = "AIzaSyChf5BrFhvjm2lhht6jL7fkSoHsgwhRCmM"
api_service_name = "youtube"
api_version="v3"
youtube = build('youtube', 'v3', developerKey=api_key)
now = datetime.now()
class YoutubeSpider(scrapy.Spider):
    pipeline=ArabicScrapperPipeline
    name = 'youtube'
    def start_requests(self):
        pipeline=ArabicScrapperPipeline()
        for name,site,cat,main_cat,sub_cat,plat,med_type,urgency in zip(names,site_list,catagory,main_category,sub_category,platform,media_type,urgenci):

            diff=site.split("/")[-1]
            print(diff,site)
            if diff=="videos":
                channel_identifier_type = site.split("/")[-3]
                if(channel_identifier_type == "channel"):
                    channel_name = site.split("/")[-2]
                    request = youtube.channels().list(part = 'snippet, contentDetails,statistics',id=channel_name, maxResults=20)
                    response = request.execute()
                else: 
                    channel_name = site.split("/")[-2]
                    request = youtube.channels().list(part = 'id, snippet, contentDetails,statistics',forUsername=channel_name, maxResults=20,)
                    response = request.execute() 
                try:
                    playlist_id=response["items"][0]['contentDetails']['relatedPlaylists']['uploads']
                except KeyError:
                    continue
                titles,descriptions,dts,image_urls,video_urls=self.scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)
                if titles==None:
                    continue
                else:
                    for title,description,dt,image_url,video_url in zip(titles,descriptions,dts,image_urls,video_urls):

                        pipeline=ArabicScrapperPipeline()
                        pipeline.process_item(item={
                            "news_agency_name":str(name),
                            "page_url":str(site),
                            "category":str(cat),
                            "title":str(title),
                            "contents":None,
                            "image_url":None,
                            "date":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "author_name":None,
                            "main_category":str(main_cat),
                            "sub_category":str(sub_cat),
                            "platform":str(plat),
                            "media_type":str(med_type),
                            "urgency":str(urgency),
                            "created_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "updated_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "deleted_at":None,
                            "tweet_created_at":None,
                            "tweet_text":None,
                            "tweet_id":None,
                            "vdo_title":str(title),
                            "vdo_description":str(description),
                            "vdo_published_at":str(dt),
                            "vdo_thumbnail":str(image_url),
                            "vdo_url":str(video_url)
                        },spider="youtube")
                        yield scrapy.Request(url="https://google.com",callback=self.dummy) #now also it calls google but it wont affect in the saving process of our data


                
            else: # if the link cotains playlist id                
                playlist_id=site.split("/")[3].split("=")[1]

                titles,descriptions,dts,image_urls,video_urls=self.scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)
                if titles==None:
                    continue
                else:
                    for title,description,dt,image_url,video_url in zip(titles,descriptions,dts,image_urls,video_urls):
                        pipeline.process_item(item={
                            "news_agency_name":str(name),
                            "page_url":str(site),
                            "category":str(cat),
                            "title":str(title),
                            "contents":None,
                            "image_url":None,
                            "date":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "author_name":None,
                            "main_category":str(main_cat),
                            "sub_category":str(sub_cat),
                            "platform":str(plat),
                            "media_type":str(med_type),
                            "urgency":str(urgency),
                            "created_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "updated_at":str(now.strftime("%Y:%m:%d %H:%M:%S")),
                            "deleted_at":None,
                            "tweet_created_at":None,
                            "tweet_text":None,
                            "tweet_id":None,
                            "vdo_title":str(title),
                            "vdo_description":str(description),
                            "vdo_published_at":str(dt),
                            "vdo_thumbnail":str(image_url),
                            "vdo_url":str(video_url)
                        },spider="youtube")
                        yield scrapy.Request(url="https://google.com",callback=self.dummy)
                            # yield scrapy.Request(url="https://google.com",callback=self.details_saver,dont_filter=True,meta={"name":name,"site":site,"cat":cat,"main_cat":main_cat,"sub_cat":sub_cat,"plat":plat,"med_type":med_type,"urgency":urgency,"title":title,"description":description,"dt":dt,"image_url":image_url,"video_url":video_url})
                    
    def dummy(self,reponse):
        return None


    def scrapper(self,playlist_id,name,site,cat,main_cat,sub_cat,plat,media_typ,urgenc):

        id_request = youtube.playlistItems().list(part = 'contentDetails',playlistId = playlist_id,maxResults = 10)
        try:
            id_response = id_request.execute()
        except :
            return None,None,None,None,None
        tit=[]
        desc=[]
        da_ti=[]
        img_url=[]
        vdo_url=[]
        for res in range(len(id_response["items"])):
            video_id=id_response["items"][res]["contentDetails"]["videoId"]
            video_url="https://www.youtube.com/embed/"+video_id

            vdo_info_request = youtube.videos().list(part='snippet, statistics',id = video_id)
            vdo_info_response = vdo_info_request.execute()
            try:
                title=vdo_info_response["items"][0]["snippet"]["title"]
                description=vdo_info_response["items"][0]["snippet"]["description"]
                published_at=vdo_info_response["items"][0]["snippet"]["publishedAt"]
            except IndexError:
                continue
            date=published_at.split("T")[0]
            time=published_at.split("T")[1]
            dt = str(parser.parse(date)).split(" ")[0]+" "+str(parser.parse(time)).split(" ")[1]

            image_url=vdo_info_response["items"][0]['snippet']['thumbnails']['default']['url'] 
            
            tit.append(title)
            desc.append(description)
            da_ti.append(dt)
            img_url.append(image_url)
            vdo_url.append(video_url)
        return tit,desc,da_ti,img_url,vdo_url