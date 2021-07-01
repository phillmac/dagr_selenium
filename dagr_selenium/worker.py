import asyncio
import logging
from copy import copy
from os import environ
from pprint import pformat
from time import sleep


# from dagr_revamped.DAGRDeviationProcessorFNS import DAGRDeviationProcessorFNS
from .functions import check_stop_file, config, flush_errors_to_queue, manager, session
from .QueueItem import QueueItem

queueman_fetch_url = environ.get('QUEUEMAN_FETCH_URL', None) or config.get('dagr.plugins.selenium', 'queueman_fetch_url', key_errors=False) or 'http://127.0.0.1:3005/item'

queueman_enqueue_url = environ.get('QUEUEMAN_ENQUEUE_URL', None) or config.get('dagr.plugins.selenium', 'queueman_enqueue_url', key_errors=False) or 'http://127.0.0.1:3005/items'



env_level = environ.get('dagr.worker.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('worker')
manager.init_logging(level_mapped)

logger = logging.getLogger(__name__)

logger.info('Urls:', pformat({
    'queueman_fetch_url':  queueman_fetch_url,
    'queueman_enqueue_url': queueman_enqueue_url
}))

async def fetch_item():
    try:
        resp = session.get(queueman_fetch_url)
        resp.raise_for_status()
        return QueueItem(**(resp.json()))
    except:
        logger.exception('Error while fetching work item')


async def process_item(item):
    try:
        item.process()
        http_errors = manager.get_dagr().report_http_errors()
        if http_errors.get(400, 0) > 1:
            raise Exception('Detected 400 error(s)')
    except:
        logger.exception('Error while processing item')
        try:
            manager.get_cache().update('error_items', item.params)
        except:
            logger.exception('Error while saving error item')
        try:
            flush_errors_to_queue(queueman_enqueue_url)
        except:
            pass


async def __main__():
    manager.set_stop_check(check_stop_file)
    with manager.get_dagr() as dagr:
        logger.info('Flushing previous errors')
        flush_errors_to_queue(queueman_enqueue_url)
        logger.info("Worker ready")
        while not check_stop_file('STOP_WORKER'):
            logger.info("Fetching work item")
            item = await fetch_item()
            if not item is None:
                logger.info(f"Got work item {item.params}")
                await process_item(item)
                dagr.print_errors()
                dagr.print_dl_total()
                dagr.reset_stats()
            else:
                logger.warning('Unable to fetch workitem')
                sleep(30)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(__main__())
    logging.shutdown()
