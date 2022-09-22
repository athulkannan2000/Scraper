import datetime
st = str(datetime.datetime.now())
with open("/tmp/time_log.txt","a") as doc:
  doc.write(f"\n start time is {st}")
  
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy import spiderloader
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import os

print("#################inside#################")
os.chdir("/root/Scraper/arabic_scrapper")
#os.chdir("C:/Users/rbw19/OneDrive/Desktop/Scraper/arabic_scrapper")
print("######current directory is###########",os.getcwd())
configure_logging()
settings = get_project_settings() 
runner = CrawlerRunner(settings)  

spider_loader = spiderloader.SpiderLoader.from_settings(settings)
# spiders = spider_loader.list()
spiders=[ 'twitter', 'youtube']
print(spiders)
classes = [spider_loader.load(name) for name in spiders]

print("############classes##########",classes)


###################testing cron execution

from datetime import datetime
now=datetime.now()
with open('/tmp/cron_log.txt',"a") as f:
    f.write("cornjob of concurrent spiders started at {} \n".format(now))
###############################################
    
for spiderClass in classes:
    runner.crawl(spiderClass)
d = runner.join()
d.addBoth(lambda _: reactor.stop())
reactor.run() 


et = str(datetime.datetime.now())
with open("/tmp/time_log.txt","a") as doc:
  doc.write(f"\n end time is {et}")