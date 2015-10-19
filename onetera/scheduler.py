# -*- coding: utf-8 -*-
from frontera.contrib.scrapy.schedulers.frontier import FronteraScheduler
from kafka import KafkaClient, SimpleConsumer, SimpleProducer
from scrapy import Request
from twisted.internet import task
from json import loads, dumps
import logging
import traceback, sys
from collections import deque
from time import time


logger = logging.getLogger("onetera.scheduler")


class PeriodCounter(object):
    def __init__(self, period):
        self.observations = deque()
        self.period = period

    def _purge(self):
        if not self.observations:
            return
        now = time()
        while True:
            o_ts, o_val = self.observations[0]
            if now - o_ts > self.period:
                self.observations.popleft()
            else:
                break
            if not self.observations:
                break

    def add(self, timestamp, value):
        self.observations.append((timestamp, value))

    def sum(self):
        self._purge()
        return sum([x[1] for x in self.observations])

    def reset(self):
        self.observations.clear()


class OneteraScheduler(FronteraScheduler):

    def __init__(self, crawler):
        super(OneteraScheduler, self).__init__(crawler)
        self.job_config = {}
        self.is_active = False
        self.results = []
        self.results_sent = 0
        self.last_result_iteration = None
        self.current_state = 'STOPPED'

        settings = self.frontier.manager.settings
        self.results_topic = settings.get('ONETERA_RESULTS_TOPIC')
        kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self.consumer = SimpleConsumer(kafka,
                                          settings.get('ONETERA_GROUP'),
                                          settings.get('ONETERA_INCOMING_TOPIC'),
                                          buffer_size=262144,
                                          max_buffer_size=10485760,
                                          auto_commit_every_n=1)
        self.producer = SimpleProducer(kafka)
        self.results_task = task.LoopingCall(self._send_results)

        self.discovery_rate = PeriodCounter(5 * 60)
        self.status_updates_interval = settings.get('ONETERA_STATUS_UPDATES_INTERVAL')
        self.status_updates_topic = settings.get('ONETERA_STATUS_UPDATES_TOPIC')
        self.status_update_task = task.LoopingCall(self._send_status_updates)
        self.stats = crawler.stats

        self.pagesprev = 0
        self.multiplier = 60.0 / self.status_updates_interval

    def result_callback(self, result):
        self.results.append(result)

    def open(self, spider):
        super(OneteraScheduler, self).open(spider)
        spider.set_result_callback(self.result_callback)
        self.status_update_task.start(self.status_updates_interval)
        self.results_task.start(20)

    def has_pending_requests(self):
        if not self.is_active:
            return False
        return super(OneteraScheduler, self).has_pending_requests()

    def next_request(self):
        if not self.is_active:
            self._check_incoming()
        if self.is_active:
            return super(OneteraScheduler, self).next_request()
        return None

    def process_spider_output(self, response, result, spider):
        self._check_finished()
        return super(OneteraScheduler, self).process_spider_output(response, result, spider)

    def process_exception(self, request, exception, spider):
        super(OneteraScheduler, self).process_exception(request, exception, spider)
        self._check_finished()

    def _check_finished(self):
        if not self.is_active:
            return
        if self.results_sent > self.job_config['nResults']:
            logger.info("Crawler reached the number of requested results. Crawling is stopping.")
            self.is_active = False
            self.current_state = 'FINISHED'
        if self.last_result_iteration and self.frontier.manager.iteration - self.last_result_iteration > 10:
            logger.info("It looks like crawler got stuck. Stopping crawling.")
            self.is_active = False
            self.current_state = 'GOTSTUCK'

    def _check_incoming(self):
        consumed = 0
        try:
            for m in self.consumer.get_messages(count=1):
                try:
                    msg = loads(m.message.value)
                except ValueError, ve:
                    logger.error("Decoding error %s, message %s" % (ve, m.message.value))
                else:
                    logger.info("Got incoming message %s from incoming topic." % m.message.value)

                    self.frontier.manager.backend.cleanup()
                    self._pending_requests.clear()
                    self.results = []
                    self.results_sent = 0
                    self.last_result_iteration = None
                    self.discovery_rate.reset()

                    self.job_config = {
                        'workspace': msg['workspace'],
                        'nResults': msg['nResults'],
                        'excluded': msg['excluded'],
                        'included': msg['included'],
                        'relevantUrl': msg['relevantUrl'],
                        'irrelevantUrl': msg['irrelevantUrl'],
                    }
                    requests = [Request(url, meta={'score': 1.0}) for url in msg['relevantUrl']]
                    if not requests:
                        raise Exception("Empty seeds list, can't bootstrap crawler.")
                    self.frontier.add_seeds(requests)
                    self.frontier.spider.configure(self.job_config)
                    self.is_active = True

                    self.current_state = 'CRAWLING'
                finally:
                    consumed += 1
        except Exception, e:
            # if we have any exception, don't activate the crawler
            self.is_active = False
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.fatal(traceback.format_tb(exc_traceback))
            logger.fatal("Got exception %s" % str(e))

    def _send_results(self):
        produced = 0
        if not self.results:
            return
        for result in self.results:
            msg = {
                "score": result['score'],
                "url": result['url'],
                "urlDesc": result['title'],
                "desc": result['descr'],
                "workspace": self.job_config['workspace'],
                "provider": "Onetera"
            }
            self.producer.send_messages(self.results_topic, dumps(msg))
            produced += 1
        self.results = []
        self.results_sent += produced

        if produced > 0:
            logger.info("Wrote %d results to output topic.", produced)
            self.last_result_iteration = self.frontier.manager.iteration
            self.discovery_rate.add(time(), produced)

    def _send_status_updates(self):
        pages = self.stats.get_value('response_received_count', 0)
        prate = (pages - self.pagesprev) * self.multiplier
        self.pagesprev = pages
        msg = {
            'discovered_last_5_min': self.discovery_rate.sum(),
            'download_rate': prate,
            'state': self.current_state
        }
        self.producer.send_messages(self.status_updates_topic, dumps(msg))

