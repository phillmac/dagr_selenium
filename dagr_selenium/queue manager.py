import asyncio
import logging
import re
from os import environ
from pathlib import Path, PurePosixPath
from pprint import pformat
from threading import Lock
from time import sleep

from aiohttp import web
from aiohttp.web_response import json_response
from dagr_revamped.lib import DagrException
from dagr_revamped.utils import artist_from_url, convert_queue

from functions import config, load_bulk, manager, update_bulk_galleries
from QueueItem import QueueItem

queue = asyncio.PriorityQueue()

queue_lock = Lock()

env_level = environ.get('dagr.queueman.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('queueman')

manager.init_logging(level_mapped)

logger = logging.getLogger(__name__)

regexes = {k: re.compile(v)
           for k, v in config.get('deviantart.regexes').items()}

queue_slug = 'queue'
cache = manager.get_cache()


class waitingCount():
    def __init__(self):
        self.__value = 0

    def inc(self):
        self.__value += 1

    def dec(self):
        self.__value -= 1

    @property
    def value(self):
        return self.__value


waiting_count = waitingCount()


async def add_to_queue(mode, deviant=None, mval=None, priority=100, full_crawl=False, resolved=False, disable_filter=False, verify_exists=None, verify_best=None, no_crawl=None, crawl_offset=None):
    item = QueueItem(mode=mode, deviant=deviant, mval=mval,
                     priority=priority, full_crawl=full_crawl, resolved=resolved,
                     disable_filter=disable_filter, verify_exists=verify_exists,
                     verify_best=verify_best, no_crawl=no_crawl,
                     crawl_offset=crawl_offset)
    params = item.params
    logger.info(f"Adding {params} to queue")
    bg_task = BackgroundTask()
    await bg_task.run(update_queue_cache, (queue_slug, params))
    await queue.put(item)
    logger.info('Finished adding item')


def detect_mode(url):
    for mode in regexes:
        if regexes[mode].match(url):
            return mode
    return None


def detect_mval(mode, url):
    parts = PurePosixPath(url).parts
    slice_count = {'gallery': False, 'favs': False, 'gallery_featured': False,
                   'favs_featured': False, 'art': 1, 'album': 2, 'collection': 2}.get(mode)
    if slice_count is False:
        return None
    if slice_count is None:
        raise NotImplementedError(f"Mode {mode} is not implemented")
    return str(PurePosixPath(*parts[len(parts) - slice_count:]))


async def resolve_deviant(deviant):
    try:
        deviant, _group = manager.get_dagr().resolve_deviant(deviant)
        return deviant
    except DagrException:
        logger.warning(f"Unable to resolve deviant {deviant}")
        raise web.HTTPBadRequest(reason='not ok: unable to resolve deviant')


async def add_url(request):
    post_contents = await request.post()

    url = post_contents.get('url')
    priority = post_contents.get('priority', 100)
    full_crawl = post_contents.get('full_crawl', False)

    if url is None:
        return web.HTTPBadRequest(reason='not ok: url missing')

    try:
        mode = detect_mode(url)
        if mode is None:
            raise NotImplementedError(f"Unable to get mode for url {url}")

        _artist_url_p, deviant, _shortname = artist_from_url(url, mode)

        if not deviant:
            return web.HTTPBadRequest(reason='not ok: deviant missing')

        deviant = await resolve_deviant(deviant)

        mval = detect_mval(mode, url)
        await add_to_queue(
            mode, deviant, mval=mval, priority=priority, full_crawl=full_crawl, resolved=True)
        bg_task = BackgroundTask()
        await bg_task.run(flush_queue_cache, ())
        logger.info('Finished add_url request')
        return web.Response(text='ok')
    except NotImplementedError:
        logger.warn('Unable to handle url:', exc_info=True)
        return web.HTTPBadRequest(reason='not ok: unable to handle url')


async def update_queue_cache(queue_slug, params):
    with queue_lock:
        try:
            cache.update(queue_slug, params)
        except:
            logger.exception('Error while updating queue cache')


async def flush_queue_cache():
    with queue_lock:
        try:
            cache.flush(queue_slug)
        except:
            logger.exception('Error while flushing queue cache')


async def remove_queue_cache_item(params):
    with queue_lock:
        try:
            cache.remove(queue_slug, params)
        except:
            logger.exception('Error while removing item from cache')


async def add_items(request):
    for item in await request.json():
        if (not 'resolved' in item) or (not item['resolved']):
            item['deviant'] = await resolve_deviant(item['deviant'])
            item['resolved'] = True
        await add_to_queue(**item)
        sleep(7)
    bg_task = BackgroundTask()
    await bg_task.run(flush_queue_cache, ())
    logger.info('Finished add_items request')
    return web.Response(text='ok')


async def add_bulk_galleries(request):
    try:
        added = update_bulk_galleries(await request.json())
        return json_response({'status': 'ok', 'added': added})
    except:
        logger.exception('Unable to add to bulk galleries list')


async def get_item(request):
    waiting_count.inc()
    item = await queue.get()
    waiting_count.dec()
    queue.task_done()
    params = item.params
    logger.info(f"Dequed item {params}")
    bg_task = BackgroundTask()
    await bg_task.run(remove_queue_cache_item, [params])
    logger.info('Finished get_item request')
    return json_response(params)


async def load_cached_queue():
    loaded = [QueueItem(**dict(i)) for i in cache.query(queue_slug)]
    logger.info(f"Adding {len(loaded)} items to queue")
    for d in loaded:
        await queue.put(d)
    logger.info(f"Done")


def run_app():
    app = web.Application()
    app.router.add_post('/url', add_url)
    app.router.add_get('/item', get_item)
    app.router.add_post('/items', add_items)
    app.router.add_get(
        '/count', lambda request: json_response({'count': queue.qsize()}))
    app.router.add_get(
        '/waiting', lambda request: json_response({'waiting': waiting_count.value}))
    app.router.add_get(
        '/contents', lambda request: json_response([qi for qi in cache.query(queue_slug)]))
    app.router.add_get(
        '/bulk', lambda request: json_response(convert_queue(config, load_bulk('.dagr_bulk.json'))))
    app.router.add_post(
        '/bulk/gallery', add_bulk_galleries)
    asyncio.get_event_loop().run_until_complete(load_cached_queue())
    web.run_app(app, host='0.0.0.0', port=3002)


class BackgroundTask:
    async def run(self, coro, args, callback=None):
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, self.task_runner, coro, args, callback)

    def task_runner(self, coro, args, callback):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        fut = asyncio.ensure_future(coro(*args))
        if callback is not None:
            fut.add_done_callback(callback)

        loop.run_until_complete(fut)
        loop.close()


if __name__ == '__main__':
    with manager.get_dagr():
        manager.get_browser().do_login()
        run_app()
