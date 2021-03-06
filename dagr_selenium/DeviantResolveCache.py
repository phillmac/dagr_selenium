import asyncio
import logging
from time import time
from random import randint

from dagr_revamped.exceptions import DagrException

logger = logging.getLogger(__name__)


class DeviantResolveCache():

    def __init__(self, storage):
        self.__slug = 'deviant_resolver_cache'
        self.__storage = storage
        self.__contents = dict()

        self.__load_contents()

    def __load_contents(self):
        t_now = time()
        logger.info('Loading deviant resolver cache contents')
        for entry in (dict(i) for i in self.__storage.query(self.__slug)):
            if t_now < entry['expiry']:
                d_lower = entry['resolved'].lower()
                if not d_lower in self.__contents:
                    self.__contents[d_lower] = entry
                elif entry['expiry'] > self.__contents[d_lower]['expiry']:
                    self.__contents[d_lower] = entry

    def query_raw(self, deviant):
        return self.__contents.get(deviant.lower(), None)

    def query(self, deviant):
        entry = self.__contents.get(deviant.lower(), None)
        if entry:
            logger.info('Resolve cache hit')
            if entry.get('deactivated'):
                raise DagrException('Deviant is deactivated')
            if entry:
                return entry['resolved']
        logger.info('Resolve cache miss')
        return None

    async def prune(self):
        t_now = time()
        prune_items = set()
        for e in self.__storage.query(self.__slug):
            entry = dict(e)
            if t_now > (entry['expiry']):
                deviant = entry['resolved'].lower()
                prune_items.update([e])
                if deviant in self.__contents:
                    del self.__contents[deviant]
                logger.info(f"Pruning expired {e} entry")
            await asyncio.sleep(0)
        self.remove(prune_items)

    def add(self, deviant, deactivated=False):
        entry = {
            'resolved': deviant,
            'expiry': time() + randint(0, 86400) + 86400 * (9 if deactivated else 4)
        }

        if deactivated:
            entry['deactivated'] = True

        self.__contents[deviant.lower()] = entry
        self.__storage.update(self.__slug, set([tuple(entry.items())]))

    def remove(self, items):
        remove_count = len(items)
        if remove_count > 0:
            logger.log(level=15, msg=f"Removing {remove_count} items")
            self.__storage.remove(self.__slug, items)

    def purge(self, deviant):
        d_lower = deviant.lower()
        remove_items = set()
        logger.log(level=15, msg=f"Purging {deviant} items")
        for e in self.__storage.query(self.__slug):
            entry = dict(e)
            if entry['resolved'].lower() == d_lower:
                remove_items.update([e])
        self.remove(remove_items)

    async def flush(self):
        self.__load_contents()
        await self.prune()
        self.__storage.flush(self.__slug)

