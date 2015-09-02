from frontera.contrib.backends.memory import MemoryBaseBackend
from frontera.contrib.scrapy.overusedbuffer import OverusedBufferScrapy
from frontera.contrib.backends.sqlalchemy import SQLiteBackend, Page, Base
from sqlalchemy import Column, Float


class MemoryScoreBackend(MemoryBaseBackend):
    component_name = 'Score-based memory backend'

    def __init__(self, manager):
        super(MemoryScoreBackend, self).__init__(manager)
        self._requests_buffer = OverusedBufferScrapy(super(MemoryScoreBackend, self).get_next_requests,
                                               manager.logger.manager.debug)

    def _compare_pages(self, first, second):
        return cmp(second.meta['score'], first.meta['score'])

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._requests_buffer.get_next_requests(max_n_requests, **kwargs)

    def cleanup(self):
        self.requests.clear()
        self.heap.heap = []


class ScoredPage(Page):
    score = Column(Float, nullable=False, index=True)


class RDBMSScoreBackend(SQLiteBackend):
    component_name = 'RDBMS Score-based backend.'

    def __init__(self, manager):
        super(RDBMSScoreBackend, self).__init__(manager)
        self._requests_buffer = OverusedBufferScrapy(super(RDBMSScoreBackend, self).get_next_requests,
                                               manager.logger.manager.debug)

    def _get_order_by(self, query):
        return query.order_by(self.page_model.score.desc())

    def _create_page(self, obj):
        db_page = super(RDBMSScoreBackend, self)._create_page(obj)
        db_page.score = obj.meta['score']
        return db_page

    def _create_request(self, db_page):
        return self.manager.request_model(url=db_page.url, meta={'score': db_page.score})

    def cleanup(self):
        for name, table in Base.metadata.tables.items():
            self.session.execute(table.delete())

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._requests_buffer.get_next_requests(max_n_requests, **kwargs)
