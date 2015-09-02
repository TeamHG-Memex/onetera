# -*- coding: utf-8 -*-
from frontera.contrib.scrapy.schedulers.frontier import FronteraScheduler


class OneteraScheduler(FronteraScheduler):

    def __init__(self, crawler):
        super(OneteraScheduler, self).__init__(crawler)

    def result_callback(self, result):
        pass

    def open(self, spider):
        super(OneteraScheduler, self).open(spider)
        spider.set_result_callback(self.result_callback)