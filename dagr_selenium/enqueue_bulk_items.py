import asyncio
import logging
from os import environ

from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dagr_revamped.utils import http_post_raw
from requests.exceptions import HTTPError

from dagr_selenium.BulkCache import BulkCache
from dagr_selenium.utils import get_urls


async def __main__():
    logger = logging.getLogger(__name__)
    manager = DAGRManager()
    config = manager.get_config()

    env_level = environ.get('dagr.enqueue_bulk_items.logging.level', None)
    level_mapped = config.map_log_level(
        int(env_level)) if not env_level is None else None

    manager.set_mode('enqueue_bulk_items')
    manager.init_logging(level_mapped)
    urls = get_urls(config)
    enqueue_url = urls['enqueue']
    waiting_url = urls['waiting']
    bulk_cache = BulkCache(manager.get_cache())
    with TCPKeepAliveSession() as session:
        async for item in bulk_cache.get_items():
            waiting = 0
            while waiting <= 1:
                try:
                    waiting = session.get(waiting_url).json()['waiting']
                except:
                    logger.exception('Unable to get waiting count')
                await asyncio.sleep(30)
            logger.info(
                f"{item.get('mode')} {item.get('deviant')} {item.get('mval')}")
            try:
                http_post_raw(session, enqueue_url, json=[item])
            except HTTPError:
                logger.exception('Failed to enqueue item')
    logger.info('Finished')

if __name__ == '__main__':
    asyncio.run(__main__())
