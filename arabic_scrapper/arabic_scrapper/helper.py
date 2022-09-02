import pandas as pd 
from datetime import date,datetime,timedelta
import dateutil.parser as parser
from deep_translator import GoogleTranslator
import os
from dotenv import load_dotenv
from word2number import w2n

load_dotenv()
try:
    news_agencies_list =  pd.read_csv(os.getenv("news_sites_list"))
except:
    news_agencies_list = pd.read_csv('spiders/News Aggregator Websites & Categories list - EN-AR - version 1 (1).xlsx - GOV and Private.csv')
def load_dataset_lists(news_agency_name,url_type):
    try:
        if(url_type == True):
            news_sites_list = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Hyper link"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            categories_english = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            main_category = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            sub_category = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            platform = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            media_type = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Media or Text - EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            urgency = news_agencies_list.loc[(news_agencies_list["News Agency in English"] == f'{news_agency_name}') & (news_agencies_list["Platform -EN"] == f'{url_type}')]["Urgency"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
        
        else:
            news_sites_list = news_agencies_list.loc[news_agencies_list["News Agency in English"] == f'{news_agency_name}']["Hyper link"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            categories_english = news_agencies_list.loc[news_agencies_list["News Agency in English"] == f'{news_agency_name}']["Category -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            main_category = news_agencies_list.loc[news_agencies_list["News Agency in English"] == f'{news_agency_name}']["Main Category EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            sub_category = news_agencies_list.loc[news_agencies_list["News Agency in English"]== f'{news_agency_name}']["Sub Category En"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            platform = news_agencies_list.loc[news_agencies_list["News Agency in English"]== f'{news_agency_name}']["Platform -EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            media_type = news_agencies_list.loc[news_agencies_list["News Agency in English"]== f'{news_agency_name}']["Media or Text - EN"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()
            urgency = news_agencies_list.loc[news_agencies_list["News Agency in English"]== f'{news_agency_name}']["Urgency"].replace(to_replace= ['\r','\n'], value= '', regex=True).tolist()


        return (news_sites_list,categories_english,main_category,sub_category,platform,media_type,urgency)
    except :
        return None

def news_list():
    try:
        return news_agencies_list
    except :
        return None
def date_today():
    try:
        return date.today()
    except :
        return None

def parser_parse_isoformat(value):
    try:
        return parser.parse(value).isoformat()
    except :
        return None

def datetime_now_isoformat():
    try:
        return datetime.now().isoformat()
    except :
        return None

def x_days_ago_date(value):
    try:
        today = date.today()
        past_date = today - timedelta(days=value)
        return str(past_date)
    except :
        return None

def x_hours_ago_date(value):
    try:
        today = date.today()
        past_date = today - timedelta(hours=value)
        return str(past_date)
    except :
        return None


def translate_text(value):
    try:
        return GoogleTranslator(source='auto', target='en').translate(value)
    except :
        return None


def agos_changer(value): # used to change "1 year/min/sec ago" to accurate date and time
    
    curr = datetime.now()
    date=value
    date = GoogleTranslator(source='auto', target='en').translate(date).split(" ")
    if date[0]=="a": # in some cases one is translated as a -->one year ago(in arabic)-->a year ago(english)
        date[0]="one"
    if date[1]=="seconds" or date[1]=="second":
        second=w2n.word_to_num(date[0])
        date = curr+timedelta(seconds=-second)
        return str(date)
    elif date[1]=="minutes"or date[1]=="minute":
        minutes=w2n.word_to_num(date[0])
        date = curr+timedelta(minutes=-minutes)
        return str(date)
    elif date[1]=="hours"or date[1]=="hour":
        hours=w2n.word_to_num(date[0])
        date = curr+timedelta(hours=-hours)
        return str(date)
    elif date[1]=="days"or date[1]=="day":
        days=w2n.word_to_num(date[0])
        date = curr+timedelta(days = -days)
        return str(date)   
    elif date[1]=="weeks"or  date[1]=="week":
        week=w2n.word_to_num(date[0])
        date = curr+timedelta(weeks=-week)
        return str(date)
    elif date[1]=="months"or date[1]=="month":
        month=w2n.word_to_num(date[0])
        week=4*month
        date = curr+timedelta(weeks=-week)
        return str(date)
    elif date[1]=="year"or date[1]=="years":
        year=w2n.word_to_num(date[0])
        week=52*year
        date = curr+timedelta(weeks=week)
        return str(date)


def word_to_num(value):
    try:
        num_dict = { 'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9 }
        return num_dict[value.lower()]
    except :
        return None
    
def selenium_path():
    return os.getenv("selenium_path")

        