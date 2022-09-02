import schedule
import time
import os


print('Scheduler initialised')
# schedule.every(3).minutes.do(lambda: os.system('python db_update.py'))
schedule.every(1).minutes.do(lambda: os.system('python concurrent_spiders_execute.py'))


while True:
    schedule.run_pending()
    time.sleep(1)
