from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.exceptions import DontCloseSpider
from scrapy import signals
from scrapy.utils.httpobj import urlparse_cached
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

    def spider_idle(self):
        self.log("Spider idle signal caught.")
        raise DontCloseSpider

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def make_requests_from_url(self, url):
        r = super(ScoreSpider, self).make_requests_from_url(url)
        r.meta['score'] = self.get_score(r)
        return r

    def get_score(self, r):
        url_parts = urlparse_cached(r)
        path_parts = url_parts.path.split('/')
        return 1.0 / (len(path_parts) + 0.05*len(r.url))

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

        if response.meta['p_score'] > 0:
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
            r.meta['score'] = self.get_score(r)
            yield r