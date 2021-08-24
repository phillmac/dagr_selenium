import asyncio
from dagr_selenium.utils import get_urls, sort_queue_galleries
import logging
from os import environ, truncate

import dagr_revamped.version as dagr_revamped_version
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession

print('Dagr version:', dagr_revamped_version)

resort = (environ['RESORT'] if environ.get('RESORT')
          else input('Resort?: ')).lower().startswith('y')

manager = DAGRManager()
config = manager.get_config()

env_level = environ.get('dagr.sort_trash.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None


manager.set_mode('sort_trash')
manager.init_logging(level_mapped)
endpoint = get_urls(config)['enqueue']

with manager.get_dagr():
    session = TCPKeepAliveSession()
    cache = manager.get_cache()
    pages = set()
    try:
        pages.update(cache.query('trash_urls'))
        # for p in list(pages):
        #     if isinstance(p, tuple):
        #         print(p)
        #         pages.remove(p)
        #         cache.remove('trash_urls', [p])
        asyncio.run(sort_queue_galleries(manager=manager, session=session, endpoint=endpoint, pages=pages, resort=resort))
    except:
        logging.getLogger(__name__).error(
            'Error while sorting trash', exc_info=True)
