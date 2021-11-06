import asyncio
from dagr_selenium.functions import resolve_deviant
import logging
from os import environ
from pprint import pformat

from aiohttp import ClientSession, web
from aiohttp.web_response import json_response
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.exceptions import DagrCacheLockException
from dagr_revamped.utils import get_remote_io
from dotenv import load_dotenv

from dagr_selenium.JSONHTTPErrors import JSONHTTPBadRequest
from dagr_selenium.utils import (flush_errors_to_queue,
                                 get_urls)

from .QueueItem import QueueItem


async def fetch_item(app):
    session = app['sessions']['queueman']
    endpoint = app['urls']['fetch']
    try:
        resp = await session.get(endpoint)
        return QueueItem(**(await resp.json()))
    except:
        app['logger'].exception('Error while fetching work item')

from dagr_revamped.DAGRCache import DAGRCache

def create_crawl_task():


async def process_item(app, manager, item):
    item_key = tuple(sorted([(k, v) for k, v in item.params.items()]))

    app['work_items'][item_key] = item

    deviant = item.deviant if item.resolved else resolve_deviant(manager, item.deviant)

    task_queue = asyncio.Queue()
    app['tasks'][item_key] = task_queue

    with DAGRCache.get_cache(dagr_io=manager.get_dagr(
    ).io, config=manager.get_config(), mode=item.mode, deviant=deviant, mval=item.mval) as cache:
        app['caches'][item_key] = cache

        await item.complete.wait()

    del app['work_items'][item_key]
    del app['caches'][item_key]
    del app['tasks'][item_key]


def shutdown_app(request):
    request.app['shutdown'].set()
    request.app['sleepmgr'].cancel_sleep()
    return json_response('ok')


async def fetch_task(request):
    pass


async def start_background_tasks(app):
    pass


async def cleanup_background_tasks(app):
    pass


async def cleanup_caches(app):
    sessions_cache = app['sessions']

    for _k, v in app['sessions'].items():
        await v.close()

    sessions_cache.clear()


async def run_app():
    load_dotenv()

    manager = DAGRManager()
    config = manager.get_config()

    env_level = environ.get('dagr.remote_worker.logging.level', None)
    level_mapped = config.map_log_level(
        int(env_level)) if not env_level is None else None

    manager.set_mode('remote_worker')
    manager.init_logging(level_mapped)

    logger = logging.getLogger(__name__)

    sessions = {
        'queueman': ClientSession(raise_for_status=True),
        'fn_cache': ClientSession(raise_for_status=True),
        'fn_server': ClientSession(raise_for_status=True)
    }

    urls = get_urls(config)

    flush_errors_to_queue(manager, sessions['queueman'], urls['enqueue'])

    app = web.Application(client_max_size=1024**2 * 100)
    app.router.add_post('/shutdown', shutdown_app)
    app.router.add_get('/task', fetch_task)

    app['shutdown'] = asyncio.Event()
    app['sessions'] = sessions
    app['work_items'] = dict()
    app['caches'] = dict()
    app['tasks'] = dict()
    app['signals'] = dict()

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.on_cleanup.append(cleanup_caches)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 3006)
    await site.start()

    logger.info("Worker ready")
    while not app['shutdown'].is_set():
        logger.info("Fetching work item")
        item = await fetch_item(app)
        if not item is None:
            logger.info(f"Got work item {item.params}")
            await process_item(app, manager, item)
        else:
            logger.warning('Unable to fetch workitem')
            await asyncio.sleep(30)


    print('Shutting down')

    await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
        logging.shutdown()
    except KeyboardInterrupt:
        print(KeyboardInterrupt)
