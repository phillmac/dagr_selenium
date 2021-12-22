import asyncio
import logging
from json import dumps
from os import environ
from pathlib import Path
from pprint import pformat
from threading import Event

from dagr_revamped.dagr_logging import do_shutdown_tasks
from aiofiles.os import exists
from selenium.common.exceptions import (InvalidSessionIdException,
                                        WebDriverException)

from .BackgroundTask import BackgroundTask
from .functions import (config, flush_errors_to_queue, manager,
                        queueman_fetch_url, session)
from .QueueItem import QueueItem

env_level = environ.get('dagr.worker.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('worker')
manager.init_logging(level_mapped)

logger = logging.getLogger(__name__)

stop_event = Event()


async def fetch_item():
    try:
        resp = session.get(queueman_fetch_url, timeout=900)
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

    except Exception as ex:
        if isinstance(ex, InvalidSessionIdException) or isinstance(ex, WebDriverException):
            logger.info('Caught fatal exception')
            manager.session_bad()
        logger.exception('Error while processing item')
        try:
            manager.get_cache().update('error_items', item.params)
        except:
            logger.exception('Error while saving error item')
        try:
            flush_errors_to_queue()
        except:
            pass


async def check_stop_file():
    checkfile = Path('~/worker.dagr.stop').expanduser()
    logger.info('Checkfilepath is %s', str(checkfile))
    count = 0
    while not stop_event.is_set():
        await asyncio.sleep(1)
        count += 1
        if count >= 60:
            count = 0
            if await exists(checkfile):
                stop_event.set()
                logger.info('Found stop-file')
    logger.info('Stop-file monitoring shutdown')


async def __main__():
    bg_tsk = BackgroundTask()
    await bg_tsk.run(check_stop_file, ())
    manager.set_stop_check(stop_event.is_set)
    with manager.get_dagr() as dagr:
        logger.info('Flushing previous errors')
        flush_errors_to_queue()
        logger.info("Worker ready")
        while manager.session_ok and not stop_event.is_set():
            logger.info("Fetching work item")
            item = await fetch_item()
            if not item is None:
                logger.info('Got work item %s', dumps(item.params))
                await process_item(item)
                dagr.print_errors()
                dagr.print_dl_total()
                dagr.reset_stats()
            else:
                logger.warning('Unable to fetch workitem')
                await asyncio.sleep(30)
    if not stop_event.is_set():
        stop_event.set()
    await asyncio.sleep(30)


if __name__ == '__main__':
    asyncio.run(__main__())
    logger.info('Worker shutting down')
    do_shutdown_tasks()
    logging.shutdown()
