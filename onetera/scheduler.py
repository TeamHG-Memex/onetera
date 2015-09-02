# -*- coding: utf-8 -*-
from frontera.contrib.scrapy.schedulers.frontier import FronteraScheduler
from kafka import KafkaClient, SimpleConsumer, SimpleProducer
from scrapy import log
from json import loads, dumps


class OneteraScheduler(FronteraScheduler):

    def __init__(self, crawler):
        super(OneteraScheduler, self).__init__(crawler)
        self.job_config = {}
        self.is_active = False
        self.results = []
        self.results_sent = 0

        settings = self.frontier.manager.settings
        self.results_topic = settings.get("ONETERA_RESULTS_TOPIC")
        kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self.consumer = SimpleConsumer(kafka,
                                          settings.get('FRONTERA_GROUP'),
                                          settings.get('ONETERA_INCOMING_TOPIC'),
                                          buffer_size=262144,
                                          max_buffer_size=10485760,
                                          auto_commit_every_n=1)
        self.producer = SimpleProducer(kafka)

    def result_callback(self, result):
        self.results.append(result)

    def open(self, spider):
        super(OneteraScheduler, self).open(spider)
        spider.set_result_callback(self.result_callback)

    def _check_incoming(self):
        if self.is_active:
            return

        consumed = 0
        try:
            for m in self.consumer.get_messages(count=1):
                try:
                    msg = loads(m.message.value)
                except ValueError, ve:
                    log.msg("Decoding error %s, message %s" % (ve, m.message.value), log.ERROR)
                else:
                    log.msg("Got incoming message %s from incoming topic." % m.message.value, log.INFO)

                    self.frontier.manager.backend.cleanup()
                    self.results = []
                    self.results_sent = 0

                    job_config = {
                        'workspace': msg['workspace'],
                        'nResults': msg['nResults'],
                        'excluded': msg['excluded'],
                        'included': msg['included'],
                        'relevantUrl': msg['relevantUrl'],
                        'irrelevantUrl': msg['irrelevantUrl'],
                    }
                    self.frontier.spider.configure(job_config)
                    requests = [self.frontier.manager.request_model(url) for url in msg['relevantUrl']]
                    if not requests:
                        raise Exception("Empty seeds list, can't bootstrap crawler.")
                    self.frontier.add_seeds(requests)
                    self.is_active = True
                finally:
                    consumed += 1
        except Exception, e:
            # if we have any exception, don't activate the crawler
            self.is_active = False
            log.msg("Got exception %s" % str(e), log.ERROR)
        self.stats['onetera_incoming_consumed'] = consumed

    def _send_results(self):
        produced = 0
        if not self.results:
            return
        items = self.results[0:50]

        for result in items:
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
        self.stats['onetera_outgoing_produced'] = produced
        del self.results[0:50]
        self.results_sent += produced

        if produced > 0:
            log.msg("Wrote %d results to output topic." % produced, log.INFO)

    def has_pending_requests(self):
        return True