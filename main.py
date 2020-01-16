#!venv/bin/python3
import logging
from logging.handlers import RotatingFileHandler

import sync_mongo
from sync_mongo import SyncMongo
__level = logging.DEBUG

logger = logging.getLogger(__name__)

uri = "repl1/localhost:27017,localhost:27018,localhost:27019"

def setup_logging():
    FORMAT='%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=FORMAT, level=__level)

    logger = logging.getLogger()
    handler = RotatingFileHandler('main.log', maxBytes=2*1024*1024, backupCount=4)
    handler.setLevel(__level)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

setup_logging()

logger.info("Starting")
SyncMongo(uri, "superheroesdb", "superheroes", "superclone" ).sync()
logger.info("completed")

