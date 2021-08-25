import asyncio
import logging
import re
from os import environ
from pathlib import Path, PurePosixPath
from pprint import pformat

from aiohttp import web
from aiohttp.web_response import json_response
from aiojobs.aiohttp import setup, spawn
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.lib import DagrException
from dagr_revamped.utils import artist_from_url, convert_queue
from dotenv import load_dotenv

from dagr_selenium.BulkCache import BulkCache
from dagr_selenium.DeviantResolveCache import DeviantResolveCache
from dagr_selenium.JSONHTTPBadRequest import JSONHTTPBadRequest
from dagr_selenium.QueueItem import QueueItem
from dagr_selenium.SleepMgr import SleepMgr
from dagr_selenium.utils import check_stop_file, resolve_deviant

logger = logging.getLogger(__name__)


class WaitingCount():
    def __init__(self):
        self.__value = 0

    def __enter__(self):
        self.__value += 1

    def __exit__(self, type, value, tb):
        self.__value -= 1

    @property
    def value(self):
        return self.__value


async def add_to_queue(queue, mode, deviant=None, mval=None, priority=100, full_crawl=False, resolved=False, disable_filter=False, verify_exists=None, verify_best=None, no_crawl=None, crawl_offset=None, load_more=None, dump_html=None):
    item = QueueItem(mode=mode, deviant=deviant, mval=mval, priority=priority,          full_crawl=full_crawl, resolved=resolved, disable_filter=disable_filter,
                     verify_exists=verify_exists, verify_best=verify_best, no_crawl=no_crawl, crawl_offset=crawl_offset, load_more=load_more, dump_html=dump_html)
    params = item.params
    logger.info(f"Adding {params} to queue")
    await queue.put(item)
    logger.info('Finished adding item')
    return params


def detect_mode(app, url):
    regex_max_priority = app['regex_max_priority']
    regexes = app['regexes']
    regex_priorities = app['regex_priorities']

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

    app = request.app
    manager = app['manager']
    resolve_cache = app['resolve_cache']
    queue = app['queue']

    nd_modes = app['nd_modes']

    url = post_contents.get('url')
    priority = post_contents.get('priority', 100)
    full_crawl = post_contents.get('full_crawl', False)
    deviant = None

    if url is None:
        return JSONHTTPBadRequest(reason='not ok: url missing')

    try:
        mode = detect_mode(app, url)
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
        params = await add_to_queue(queue=queue, mode=mode, deviant=deviant, mval=mval, priority=priority, full_crawl=full_crawl, resolved=True)

        await spawn(request, update_queue_cache(app, params))
        await spawn(request, flush_queue_cache(app))

        logger.info('Finished add_url request')
        return json_response('ok')
    except NotImplementedError:
        logger.warning('Unable to handle url:', exc_info=True)
        return JSONHTTPBadRequest(reason='not ok: unable to handle url')


async def update_queue_cache(app, params):
    queue_lock = app['queue_lock']
    crawler_cache = app['crawler_cache']
    queue_slug = app['queue_slug']
    async with queue_lock:
        try:
            crawler_cache.update(queue_slug, params)
        except:
            logger.exception('Error while updating queue cache')


async def flush_queue_cache(app):
    queue_lock = app['queue_lock']
    crawler_cache = app['crawler_cache']
    queue_slug = app['queue_slug']

    async with queue_lock:
        try:
            crawler_cache.flush(queue_slug)
        except:
            logger.exception('Error while flushing queue cache')


async def remove_queue_cache_item(app, params):
    queue_lock = app['queue_lock']
    crawler_cache = app['crawler_cache']
    queue_slug = app['queue_slug']

    logger.info('Waiting for queue lock')
    async with queue_lock:
        logger.info(f"Removing {params} from queue cache")
        try:
            crawler_cache.remove(queue_slug, params)
        except:
            logger.exception('Error while removing item from cache')


async def add_items(request):
    app = request.app
    manager = app['manager']
    resolve_cache = app['resolve_cache']
    queue = app['queue']

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

        params = await add_to_queue(queue=queue, **item)
        await spawn(request, update_queue_cache(app, params))
        await asyncio.sleep(0)

    await spawn(request, flush_queue_cache(app))

    logger.info('Finished add_items request')
    return json_response('ok')


async def get_item(request):
    app = request.app
    queue = app['queue']
    waiting_count = app['waiting_count']
    with waiting_count:
        try:
            item = await asyncio.wait_for(queue.get(), 30)
            queue.task_done()
            params = item.params
            logger.info(f"Dequed item {params}")
            await spawn(request, remove_queue_cache_item(app, item.raw_params))
            logger.info('Finished get_item request')
            return json_response(params)
        except asyncio.TimeoutError:
            logger.log(level=15, msg='Timout waiting to dequeue work item')
            return json_response({'mode': None})


async def resolve(request):
    params = await request.json()

    app = request.app
    manager = app['manager']
    resolve_cache = app['resolve_cache']

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
    resolve_cache = request.app['resolve_cache']

    deviant = params.get('deviant', None)

    if deviant is None:
        return JSONHTTPBadRequest(reason='not ok: deviant missing')

    return json_response({'result': resolve_cache.query_raw(deviant)})


async def flush_resolve_cache(request):
    resolve_cache = request.app['resolve_cache']
    await resolve_cache.flush()
    return json_response('ok')


async def bulk_get_items(request):
    bulk_cache = request.app['bulk_cache']
    items = []
    async for i in bulk_cache.get_items():
        items.append(i)

    return json_response(items)


async def reload_queue(request):
    await load_cached_queue(request.app)
    return json_response('ok')


def shutdown_app(request):
    request.app['shutdown'].set()
    request.app['sleepmgr'].cancel_sleep()
    return json_response('ok')

async def load_cached_queue(app):
    crawler_cache = app['crawler_cache']
    queue = app['queue']
    queue_slug = app['queue_slug']
    loaded = [QueueItem(**dict(i)) for i in crawler_cache.query(queue_slug)]
    logger.info(f"Adding {len(loaded)} items to queue")
    for d in loaded:
        await queue.put(d)
    logger.info(f"Done")


async def start_background_tasks(app):
    pass


async def cleanup_background_tasks(app):
    for t in app['tasks']:
        t.cancel()


async def cleanup_caches(app):
    sessions_cache = app['sessions']

    for _k, v in app['sessions'].items():
        v.close()

    sessions_cache.clear()

    app['crawler_cache'].flush()


async def run_app():
    load_dotenv()

    manager = DAGRManager()
    config = manager.get_config()

    env_level = environ.get('dagr.queueman.logging.level', None)
    level_mapped = config.map_log_level(
        int(env_level)) if not env_level is None else None

    manager.set_mode('queueman')
    manager.init_logging(level_mapped)

    crawler_cache = manager.get_cache()
    resolve_cache = DeviantResolveCache(crawler_cache)
    bulk_cache = BulkCache(crawler_cache)

    queue = asyncio.PriorityQueue()
    waiting_count = WaitingCount()

    app = web.Application()
    app.router.add_get('/ping', lambda request: json_response('pong'))
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
        '/contents', lambda request: json_response([*app['crawler_cache'].query(app['queue_slug'])]))
    app.router.add_get(
        '/bulk/all', bulk_get_items)
    app.router.add_post('/shutdown', shutdown_app)

    setup(app)

    app['queue_slug'] = 'queue'

    app['tasks'] = {}
    app['manager'] = manager
    app['queue'] = queue
    app['queue_lock'] = asyncio.Lock()
    app['shutdown'] = asyncio.Event()
    app['sleepmgr'] = SleepMgr(app, 300)
    app['waiting_count'] = waiting_count
    app['crawler_cache'] = crawler_cache
    app['resolve_cache'] = resolve_cache
    app['bulk_cache'] = bulk_cache
    app['dagr_config'] = config

    app['regexes'] = {k: re.compile(v)
                      for k, v in config.get('deviantart.regexes').items()}

    app['regex_priorities'] = config.get('deviantart.regexes.priorities')

    app['regex_max_priority'] = config.get(
        'deviantart.regexes.params', 'maxpriority')

    app['nd_modes'] = config.get('deviantart', 'ndmodes').split(',')

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.on_cleanup.append(cleanup_caches)

    await load_cached_queue(app)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port=environ.get(
        'QUEUEMAN_LISTEN_PORT', 3005))
    await site.start()

    names = sorted(str(s.name) for s in runner.sites)
    print(
        "======== Running on {} ========\n"
        "(Press CTRL+C to quit)".format(", ".join(names))
    )

    while not app['shutdown'].is_set() and not await check_stop_file(manager, 'STOP_QUEUEMAN'):
        await resolve_cache.flush()
        await bulk_cache.flush()
        await app['sleepmgr'].sleep()

    print('Shutting down')

    await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print('KeyboardInterrupt')

    logging.shutdown()
