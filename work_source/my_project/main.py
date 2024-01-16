import schedule
from zvonok_calls_import import etl_zvonok_info
from obladi_api import run_update_obladi
import logging


logging.basicConfig(level=logging.INFO, filename="fc_log.log", filemode="a",
                    format="%(asctime)s: [%(levelname)s] - [%(module)s] %(message)s")
logging.info('Main script started...')

schedule.every().day.at("00:10").do(etl_zvonok_info)
schedule.every(1).hour.at(":30").do(run_update_obladi)

while True:
    schedule.run_pending()
