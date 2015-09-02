from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.exceptions import DontCloseSpider
from scrapy import signals
from classifier.content_processor import ContentProcessor
from classifier.classifier import TopicClassifier


class ScoreSpider(Spider):
    name = 'score'

    def __init__(self, *args, **kwargs):
        super(ScoreSpider, self).__init__(*args, **kwargs)
        self.contentprocessor = ContentProcessor(skip_text=False)
        self.job_config = {'disabled': True}
        self.classifier = None
        self.result_cb = None

    def set_process_info(self, process_info):
        self.process_info = process_info

    def set_result_callback(self, func):
        self.result_cb = func

    def configure(self, job_config):
        self.job_config = job_config
        if 'disabled' not in job_config:
            self.classifier = TopicClassifier.from_keywords(job_config['included'], job_config['excluded'])

    # stable branch
    def set_crawler(self, crawler):
        super(ScoreSpider, self).set_crawler(crawler)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def spider_idle(self):
        self.log("Spider idle signal caught.")
        raise DontCloseSpider

    def make_requests_from_url(self, url):
        r = super(ScoreSpider, self).make_requests_from_url(url)
        return r

    def parse(self, response):
        pc = self.contentprocessor.process_response(response)
        if not pc:
            return
        if not self.classifier:
            raise Exception("Classifier not configured")

        response.meta['p_score'] = self.classifier.score_paragraphs(pc.paragraphs)
        response.meta['title'] = pc.title
        response.meta['descr'] = pc.meta_description
        response.meta['keywords'] = pc.meta_keywords

        self.result_cb({
            'score': response.meta['p_score'],
            'url': response.url,
            'title': response.meta['title'],
            'descr': response.meta['descr'],
            'keywords': response.meta['keywords']
        })

        for link in pc.links:
            r = Request(url=link.url)
            r.meta.update(link_text=link.text)
            yield r