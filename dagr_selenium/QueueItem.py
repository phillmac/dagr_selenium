from .functions import rip, update_bookmarks
from asyncio import Event


class QueueItem():
    def __init__(self, **kwargs) -> None:
        self.__params = kwargs
        self.complete = Event()

    @property
    def params(self):
        return {k: v for k, v in self.__params.items()}

    @property
    def mode(self):
        return self.__params.get('mode').lower()

    @property
    def deviant(self):
        return self.__params.get('deviant')

    @property
    def mval(self):
        return self.__params.get('mval')

    @property
    def resolved(self):
        return self.__params.get('resolved')

    @property
    def priority(self):
        return self.__params.get('priority')

    @property
    def full_crawl(self):
        return self.__params.get('full_crawl')

    @property
    def config_options(self):
        return self.__params.get('config_options')

    def __lt__(self, other):
        return self.priority < other.priority

    def process(self):
        if self.mode is None:
            return
        handler = {
            'gallery': lambda: rip(**self.__params),
            'gallery_html': lambda: rip(**self.__params),
            'favs': lambda: rip(**self.__params),
            'favs_html': lambda: rip(**self.__params),
            'scraps': lambda: rip(**self.__params),
            'collection': lambda: rip(**self.__params),
            'collection_html': lambda: rip(**self.__params),
            'album': lambda: rip(**self.__params),
            'album_html': lambda: rip(**self.__params),
            'favs_featured': lambda: rip(**self.__params),
            'gallery_featured': lambda: rip(**self.__params),
            'search': lambda: rip(**self.__params),
            'search_html': lambda: rip(**self.__params),
            'tag': lambda: rip(**self.__params),
            'tag_html': lambda: rip(**self.__params),
            'art': lambda: update_bookmarks('art', self.deviant, self.mval)
        }.get(self.mode)
        if handler is None:
            raise NotImplementedError(f"Mode {self.mode} not available")
        handler()
