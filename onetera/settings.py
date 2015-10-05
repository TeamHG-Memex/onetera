# -*- coding: utf-8 -*-

# Scrapy settings for topic project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
from scrapy.settings.default_settings import SPIDER_MIDDLEWARES, DOWNLOADER_MIDDLEWARES
import logging
from os import environ

BOT_NAME = 'onetera'

SPIDER_MODULES = ['onetera.spiders']
NEWSPIDER_MODULE = 'onetera.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'topic (+http://www.yourdomain.com)'

SPIDER_MIDDLEWARES.update({
    'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 1000,
    'scrapy.spidermiddleware.depth.DepthMiddleware': None,
    'scrapy.spidermiddleware.offsite.OffsiteMiddleware': None,
    'scrapy.spidermiddleware.referer.RefererMiddleware': None,
    'scrapy.spidermiddleware.urllength.UrlLengthMiddleware': None
})

DOWNLOADER_MIDDLEWARES.update({
    'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 1000,
})

SCHEDULER = 'onetera.scheduler.OneteraScheduler'
SPIDER_MIDDLEWARES.update({
    'frontera.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
})

#SEEDS_SOURCE = 'seeds.txt'

HTTPCACHE_ENABLED = False
REDIRECT_ENABLED = True
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 120
RETRY_ENABLED = False
DOWNLOAD_MAXSIZE = 10*1024*1024

# auto throttling
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_DEBUG = False
AUTOTHROTTLE_MAX_DELAY = 3.0
AUTOTHROTTLE_START_DELAY = 0.25
RANDOMIZE_DOWNLOAD_DELAY = False

# concurrency
CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 10
DOWNLOAD_DELAY = 0.0

# logging
LOGSTATS_INTERVAL = 10
LOG_LEVEL = 'INFO'
# EXTENSIONS = {
#    'scrapy_jsonrpc.webservice.WebService': 500
#    }
# JSONRPC_ENABLED = True
# JSONRPC_LOGFILE = 'jsonrpc.log'

#
#
# Frontera settings
#
#
MAX_REQUESTS = 0
MAX_NEXT_REQUESTS = 256
OVERUSED_SLOT_FACTOR = 5.0
DELAY_ON_EMPTY = 5.0

# Url storage
#BACKEND = 'onetera.backends.MemoryScoreBackend'

BACKEND = 'onetera.backends.RDBMSScoreBackend'
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///url_storage.sqlite'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_DROP_ALL_TABLES = True
SQLALCHEMYBACKEND_CLEAR_CONTENT = True
SQLALCHEMYBACKEND_MODELS = {
    'Page': 'onetera.backends.ScoredPage',
}

# Logging
LOGGING_ENABLED = True
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = True
LOGGING_BACKEND_ENABLED = True
LOGGING_DEBUGGING_ENABLED = False

LOGGING_BACKEND_LOGLEVEL = logging.WARNING
LOGGING_MANAGER_LOGLEVEL = logging.INFO


KAFKA_LOCATION = environ["KAFKA_LOCATION"] if "KAFKA_LOCATION" in environ else "%s:%s" % \
    (environ["KAFKACONTAINER_PORT_9092_TCP_ADDR"], environ["KAFKACONTAINER_PORT_9092_TCP_PORT"])

ONETERA_GROUP = "onetera"
ONETERA_RESULTS_TOPIC = "broadcrawler-output"
ONETERA_INCOMING_TOPIC = "broadcrawler-frontera-input"

