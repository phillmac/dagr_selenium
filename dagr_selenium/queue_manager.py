import asyncio
import logging
import re
from os import environ
from pathlib import Path, PurePosixPath
from pprint import pformat

from aiohttp import web
from aiohttp.web_response import json_response
from dagr_revamped.lib import DagrException
from dagr_revamped.utils import artist_from_url, convert_queue

from dagr_selenium.DeviantResolveCache import DeviantResolveCache
from dagr_selenium.functions import (config, load_bulk, manager,
                                     update_bulk_galleries)
from dagr_selenium.JSONHTTPBadRequest import JSONHTTPBadRequest
from dagr_selenium.QueueItem import QueueItem
from dagr_selenium.utils import resolve_deviant

queue = asyncio.PriorityQueue()

queue_lock = asyncio.Lock()

env_level = environ.get('dagr.queueman.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('queueman')

manager.init_logging(level_mapped)

logger = logging.getLogger(__name__)

regexes = {k: re.compile(v)
           for k, v in config.get('deviantart.regexes').items()}

regex_priorities = config.get('deviantart.regexes.priorities')

regex_max_priority = config.get('deviantart.regexes.params', 'maxpriority')

nd_modes = config.get('deviantart', 'ndmodes').split(',')

queue_slug = 'queue'
watchlist_slug = 'watch_urls'
cache = manager.get_cache()
resolve_cache = DeviantResolveCache(cache)
resolve_cache.flush()


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


async def add_to_queue(mode, deviant=None, mval=None, priority=100, full_crawl=False, resolved=False, disable_filter=False, verify_exists=None, verify_best=None, no_crawl=None, crawl_offset=None, load_more=None, dump_html=None):
    item = QueueItem(mode=mode, deviant=deviant, mval=mval, priority=priority,          full_crawl=full_crawl, resolved=resolved, disable_filter=disable_filter,
                     verify_exists=verify_exists, verify_best=verify_best, no_crawl=no_crawl, crawl_offset=crawl_offset, load_more=load_more, dump_html=dump_html)
    params = item.params
    logger.info(f"Adding {params} to queue")
    await queue.put(item)
    asyncio.create_task(update_queue_cache(params))
    logger.info('Finished adding item')


def detect_mode(url):
    for p in range(regex_max_priority):
        logger.log(level=15, msg=f"Trying regex priority {p}")
        for mode in regexes:
            if regex_priorities[mode] == p:
                logger.log(level=15, msg=f"Trying regex for {mode}")
                if regexes[mode].match(url):
                    return mode
    return None


def detect_mval(mode, url):
    parts = PurePosixPath(url).parts
    slice_count = {'tag': 0, 'gallery': False, 'favs': False, 'gallery_featured': False,
                   'favs_featured': False, 'art': 1, 'album': 2, 'collection': 2}.get(mode)
    if slice_count is False:
        return None
    if slice_count is None:
        raise NotImplementedError(f"Mode {mode} is not implemented")
    return str(PurePosixPath(*parts[len(parts) - slice_count:]))


async def add_url(request):
    post_contents = await request.post()

    url = post_contents.get('url')
    priority = post_contents.get('priority', 100)
    full_crawl = post_contents.get('full_crawl', False)
    deviant = None

    if url is None:
        return JSONHTTPBadRequest(reason='not ok: url missing')

    try:
        mode = detect_mode(url)
        if mode is None:
            raise NotImplementedError(f"Unable to get mode for url {url}")

        if not mode in nd_modes:
            _artist_url_p, deviant, _shortname = artist_from_url(url, mode)
            if deviant is None:
                return JSONHTTPBadRequest(reason='not ok: deviant missing')
            try:
                deviant = await resolve_deviant(manager, deviant, resolve_cache)
            except DagrException:
                raise JSONHTTPBadRequest(
                    reason='not ok: unable to resolve deviant')

        mval = detect_mval(mode, url)
        await add_to_queue(
            mode, deviant, mval=mval, priority=priority, full_crawl=full_crawl, resolved=True)
        asyncio.create_task(flush_queue_cache())
        logger.info('Finished add_url request')
        return web.Response(text='ok')
    except NotImplementedError:
        logger.warning('Unable to handle url:', exc_info=True)
        return JSONHTTPBadRequest(reason='not ok: unable to handle url')


async def update_queue_cache(params):
    async with queue_lock:
        try:
            cache.update(queue_slug, params)
        except:
            logger.exception('Error while updating queue cache')


async def flush_queue_cache():
    async with queue_lock:
        try:
            cache.flush(queue_slug)
        except:
            logger.exception('Error while flushing queue cache')


async def remove_queue_cache_item(params):
    async with queue_lock:
        try:
            cache.remove(queue_slug, params)
        except:
            logger.exception('Error while removing item from cache')


async def add_items(request):
    for item in await request.json():
        if (not 'resolved' in item) or (not item['resolved']):
            try:
                item['deviant'] = await resolve_deviant(
                    manager, item['deviant'], resolve_cache)
            except DagrException:
                raise JSONHTTPBadRequest(
                    reason='not ok: unable to resolve deviant')

            item['resolved'] = True
        else:
            logger.info('Deviant already resolved')

        await add_to_queue(**item)

    asyncio.create_task(flush_queue_cache())
    logger.info('Finished add_items request')
    return json_response('ok')


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
    asyncio.create_task(remove_queue_cache_item(params))
    logger.info('Finished get_item request')
    return json_response(params)


async def flush_watchlist_cache():
    cache.flush(watchlist_slug)


async def update_watchlist_cache(request):
    urls = await request.json()
    cache.update(watchlist_slug, urls)
    asyncio.create_task(flush_watchlist_cache())
    return json_response('ok', headers={
        'Access-Control-Allow-Origin': '*'
    })


async def resolve(request):
    params = await request.json()

    deviant = params.get('deviant', None)

    if deviant is None:
        return JSONHTTPBadRequest(reason='not ok: deviant missing')
    try:
        resolved = await resolve_deviant(manager, deviant, resolve_cache)
        return json_response({'deviant': deviant, 'resolved': resolved})
    except DagrException:
        raise JSONHTTPBadRequest(
            reason='not ok: unable to resolve deviant')


async def query_resolve_cache(request):
    params = await request.json()

    deviant = params.get('deviant', None)

    if deviant is None:
        return JSONHTTPBadRequest(reason='not ok: deviant missing')

    return json_response({'result': resolve_cache.query_raw(deviant)})

async def flush_resolve_cache(request):
    resolve_cache.flush()
    return json_response('ok')


async def reload_queue(request):
    await load_cached_queue()
    return json_response('ok')


async def load_cached_queue():
    loaded = [QueueItem(**dict(i)) for i in cache.query(queue_slug)]
    logger.info(f"Adding {len(loaded)} items to queue")
    for d in loaded:
        await queue.put(d)
    logger.info(f"Done")


def run_app():
    app = web.Application()
    app.router.add_get('/ping', lambda: json_response('pong'))
    app.router.add_post('/reload', reload_queue)
    app.router.add_post('/url', add_url)
    app.router.add_get('/item', get_item)
    app.router.add_post('/items', add_items)
    app.router.add_get('/resolve', resolve)
    app.router.add_get('/resolve/cache/query', query_resolve_cache)
    app.router.add_post('/resolve/cache/flush', flush_resolve_cache)
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
    app.router.add_post('/watchlist/items', update_watchlist_cache)
    asyncio.get_event_loop().run_until_complete(load_cached_queue())
    web.run_app(app, host='0.0.0.0', port=environ.get(
        'QUEUEMAN_LISTEN_PORT', 3005))


if __name__ == '__main__':
    with manager.get_dagr():
        run_app()
