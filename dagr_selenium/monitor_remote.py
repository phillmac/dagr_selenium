import asyncio
from dagr_selenium.DeviantResolveCache import DeviantResolveCache
import logging
from os import environ, truncate

import dagr_revamped.version as dagr_revamped_version
from aiohttp import web
from aiohttp.web_response import json_response
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dotenv import load_dotenv

from dagr_selenium.SleepMgr import SleepMgr
from dagr_selenium.utils import get_urls, sort_all

print('Dagr version:', dagr_revamped_version)


async def flush_cache(cache, slug):
    cache.flush(slug)


def shutdown_app(request):
    request.app['shutdown'].set()
    request.app['sleepmgr'].cancel_sleep()
    return json_response('ok')


async def update_cache(request, slug):
    urls = await request.json()
    cache = request.app['crawler_cache']

    c_before = len(cache.query(slug))

    cache.update(slug, urls)

    c_after = len(cache.query(slug))

    if c_after != c_before:
        logging.info('Added %s items', c_after - c_before)
        request.app['stale'][slug] = True

    return json_response('ok', headers={
        'Access-Control-Allow-Origin': '*'
    })


async def purge_resolve_cache_items(request):
    deviants = await request.json()
    crawler_cache = request.app['crawler_cache']
    resolve_cache = DeviantResolveCache(crawler_cache)

    if not isinstance(deviants, list):
        deviants = [deviants]

    for d in deviants:
        resolve_cache.purge(d)

    return json_response('ok')


async def start_background_tasks(app):
    pass


async def cleanup_background_tasks(app):
    pass


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

    env_level = environ.get('dagr.monitor_remote.logging.level', None)
    level_mapped = config.map_log_level(
        int(env_level)) if not env_level is None else None

    manager.set_mode('monitor_remote')
    manager.init_logging(level_mapped)

    with manager.get_dagr():
        queueman_session = TCPKeepAliveSession()
        sessions = {
            'queueman': queueman_session
        }

        enqueue_url = get_urls(config)['enqueue']

        app = web.Application(client_max_size=1024**2 * 100)
        app.router.add_post('/shutdown', shutdown_app)
        app.router.add_post(
            '/watchlist/items', lambda request: update_cache(request, 'watch_urls'))
        app.router.add_post(
            '/trash/items', lambda request: update_cache(request, 'trash_urls'))
        app.router.add_delete('/resolve/cache/items',
                              purge_resolve_cache_items)
        app.router.add_post('/sort/all', lambda _request: sort_all(manager, queueman_session, enqueue_url))
        app.router.add_post('/shutdown', shutdown_app)


        app['shutdown'] = asyncio.Event()
        app['sessions'] = sessions
        app['sleepmgr'] = SleepMgr(app, 300)
        app['crawler_cache'] = manager.get_cache()
        app['stale'] = {}

        app.on_startup.append(start_background_tasks)
        app.on_cleanup.append(cleanup_background_tasks)
        app.on_cleanup.append(cleanup_caches)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 3006)
        await site.start()

        names = sorted(str(s.name) for s in runner.sites)
        print(
            "======== Running on {} ========\n"
            "(Press CTRL+C to quit)".format(", ".join(names))
        )

        while not app['shutdown'].is_set():
            await app['sleepmgr'].sleep()
            for slug, is_stale in app['stale'].items():
                if is_stale:
                    asyncio.create_task(flush_cache(app['crawler_cache'], slug))
                    app['stale'][slug] = False
            await sort_all(manager, queueman_session, enqueue_url)

        print('Shutting down')

        await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print('KeyboardInterrupt')

    logging.shutdown()
