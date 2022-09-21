from email.mime import image
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
print("lkggggggggggggggggggg ",dataset.shape)
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
    name = 'youtube'
    def start_requests(self):
        for name,site,cat,main_cat,sub_cat,plat,med_type,urgency in zip(names,site_list,catagory,main_category,sub_category,platform,media_type,urgenci):
            #print(name,site,urgency)
            diff=site.split("/")[-1]
            if diff=="videos":
                channel_name=site.split("/")[-2]
                request = youtube.channels().list(part = 'snippet, contentDetails,statistics',forUsername=channel_name)
                response= request.execute() 
                try:
                    playlist_id=response["items"][0]['contentDetails']['relatedPlaylists']['uploads']
                except KeyError:
                    continue
                titles,descriptions,dts,image_urls,video_urls=self.scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)
                if titles==None:
                    #print("############returned title#################")
                    continue
                else:
                    for title,description,dt,image_url,video_url in zip(titles,descriptions,dts,image_urls,video_urls):
                        yield scrapy.Request(url="https://google.com",callback=self.details_saver,dont_filter=True,meta={"name":name,"site":site,"cat":cat,"main_cat":main_cat,"sub_cat":sub_cat,"plat":plat,"med_type":med_type,"urgency":urgency,"title":title,"description":description,"dt":dt,"image_url":image_url,"video_url":video_url})


                
            else: # if the link cotains playlist id
                playlist_id=site.split("/")[3].split("=")[1]
                titles,descriptions,dts,image_urls,video_urls=self.scrapper(playlist_id,name,site,cat,main_cat,sub_cat,plat,med_type,urgency)
                if titles==None:
                    #print("############returned title#################")
                    continue
                else:
                    for title,description,dt,image_url,video_url in zip(titles,descriptions,dts,image_urls,video_urls):
                        yield scrapy.Request(url="https://google.com",callback=self.details_saver,dont_filter=True,meta={"name":name,"site":site,"cat":cat,"main_cat":main_cat,"sub_cat":sub_cat,"plat":plat,"med_type":med_type,"urgency":urgency,"title":title,"description":description,"dt":dt,"image_url":image_url,"video_url":video_url})
                    

    def details_saver(self,response):
        db_item=GeneralItem()
        #print("$$$$$$$$$$$$$$$$$$$$$$$$$ Hey there $$$$$$$$$$$$$$$$$$$$$$$$$$$$$",str(response.meta["name"]),str(response.meta["site"]),str(response.meta["cat"]),str(response.meta["title"]),str(now.strftime("%Y:%m:%d %H:%M:%S")),str(response.meta["main_cat"]),str(response.meta["sub_cat"]),str(response.meta["plat"]),str(response.meta["med_type"]),str(response.meta["urgency"]),str(now.strftime("%Y:%m:%d %H:%M:%S")),str(now.strftime("%Y:%m:%d %H:%M:%S")),str(response.meta["title"]),str(response.meta["description"]),str(response.meta["dt"]),str(response.meta["image_url"]),str(response.meta["video_url"]))
        db_item["news_agency_name"]=str(response.meta["name"])
        db_item["page_url"]=str(response.meta["site"])
        db_item["category"]=str(response.meta["cat"])
        db_item["title"]=str(response.meta["title"])
        db_item["contents"]=None
        db_item["image_url"]=None
        db_item["date"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["author_name"]=None
        db_item["main_category"]=str(response.meta["main_cat"])
        db_item["sub_category"]=str(response.meta["sub_cat"])
        db_item["platform"]=str(response.meta["plat"])
        db_item["media_type"]=str(response.meta["med_type"])
        db_item["urgency"]=str(response.meta["urgency"])
        db_item["created_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["updated_at"]=str(now.strftime("%Y:%m:%d %H:%M:%S"))
        db_item["deleted_at"]=None
        db_item["tweet_created_at"]=None
        db_item["tweet_text"]=None
        db_item["tweet_id"]=None
        db_item["vdo_title"]=str(response.meta["title"])
        db_item["vdo_description"]=str(response.meta["description"])
        db_item["vdo_published_at"]=str(response.meta["dt"])
        db_item["vdo_thumbnail"]=str(response.meta["image_url"])
        db_item["vdo_url"]=str(response.meta["video_url"])
        #print("$$$$$$$$$$$$$$$$$$$$$ OOOOOKKKKKK saved sucessfully $$$$$$$$$$$$$$$$$$$$$$$:\n",db_item)
        yield db_item


    def scrapper(self,playlist_id,name,site,cat,main_cat,sub_cat,plat,media_typ,urgenc):

        id_request = youtube.playlistItems().list(part = 'contentDetails',playlistId = playlist_id,maxResults = 10)
        try:
            id_response = id_request.execute()
        except :
            #print(playlist_id,"###########playlist not existing#########")
            return None,None,None,None,None
        tit=[]
        desc=[]
        da_ti=[]
        img_url=[]
        vdo_url=[]
        for res in range(len(id_response["items"])):
            video_id=id_response["items"][res]["contentDetails"]["videoId"]
            video_url="https://www.youtube.com/embed/"+video_id
            #print("\n video url",video_url)

            #getting video details
            vdo_info_request = youtube.videos().list(part='snippet, statistics',id = video_id)
            vdo_info_response = vdo_info_request.execute()
            # #print(vdo_info_response["items"])
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

            #print("\ntitle :",type(title),"\n description :",type(description),"\npublished at :",type(published_at),"\n image_url :",type(image_url),"\n date and time",type(dt))
            
            tit.append(title)
            desc.append(description)
            da_ti.append(dt)
            img_url.append(image_url)
            vdo_url.append(video_url)
        return tit,desc,da_ti,img_url,vdo_url
            
            
            # pipe.process_item(db_item,None)