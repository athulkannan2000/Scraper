from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy import spiderloader
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import os
from multiprocessing import Process

print("#################inside#################")
os.chdir("/root/Scraper/arabic_scrapper")
print("######current directory is###########",os.getcwd())
configure_logging()
settings = get_project_settings() 
runner = CrawlerRunner(settings)  

spider_loader = spiderloader.SpiderLoader.from_settings(settings)
spiders = spider_loader.list()
classes = [spider_loader.load(name) for name in spiders]

# print("############classes##########\n",len(classes))

def crawlerSetOne():
    classes_one = classes[:40]
    for spiderClass in classes_one:
        runner.crawl(spiderClass)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run() 

def crawlerSetTwo():
    classes_two = classes[40:]
    for spiderClass in classes_two:
        runner.crawl(spiderClass)
    e = runner.join()
    e.addBoth(lambda _: reactor.stop())
    reactor.run() 
    
<<<<<<< HEAD
=======

##################testing cron execution
>>>>>>> e7607109fcb672de1657f10ce7b54e805fb55e18

##################testing cron execution
from datetime import datetime
now=datetime.now()
with open('/tmp/cron_log.txt',"a") as f:
    f.write("cornjob of concurrent spiders started at {} \n".format(now))
###############################################
    

if __name__ == "__main__":

    processes = [
                    Process(target=crawlerSetOne),
                    Process(target=crawlerSetTwo)
                ]


    for process in processes:
        process.start()

    for process in processes:
        process.join()