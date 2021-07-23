import asyncio


class BulkCache():

    @staticmethod
    def create_item(mode, deviant=None, mval=None):
        return tuple((k,v) for k,v in {
            'deviant': deviant.lower(),
            'mode': mode.lower(),
            'mval': mval
        }.items() if v is not None)

    def __init__(self, storage):
        self.__slug = 'dagr_bulk_cache'
        self.__storage = storage
        self.__contents = dict()
        self.__load_items()

    def __load_items(self, items=None):
        for i in items or self.__storage.query(self.__slug):
            item = dict(i)
            mode = item['mode']
            if not mode in self.__contents:
                self.__contents[mode] = set()
            self.__contents[mode].update([i])

    async def add_item (self, mode, deviant=None, mval=None):
        item = BulkCache.create_item(mode, deviant, mval)
        self.__storage.update(self.__slug, set([item]))
        if not mode in self.__contents:
                self.__contents[mode] = set()
        self.__contents[mode].update([item])

    async def add(self, items):
        self.__storage.update(self.__slug, items)
        self.__load_items(items)


    async def get_items(self):
        for item in self.__storage.query(self.__slug):
            yield dict(item)

    def query(self, mode):
        return (dict(i) for i in self.__contents.get(mode, []))

    async def flush(self):
        self.__storage.flush(self.__slug)
        self.__load_items()

    def count(self, mode):
        return len(self.__contents.get(mode, []))






