from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy import spiderloader
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import os

print("#################inside#################")
os.chdir("/root/Scraper/arabic_scrapper")
print("######current directory is###########",os.getcwd())
configure_logging()
settings = get_project_settings() 
runner = CrawlerRunner(settings)  

spider_loader = spiderloader.SpiderLoader.from_settings(settings)
spiders = spider_loader.list()
classes = [spider_loader.load(name) for name in spiders]
print("############3classes##########",classes)

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

