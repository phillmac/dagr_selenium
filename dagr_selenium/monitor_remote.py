import asyncio
import logging
from os import environ

import dagr_revamped.version as dagr_revamped_version
from aiohttp import web
from aiohttp.web_response import json_response
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dotenv import load_dotenv

from dagr_selenium.SleepMgr import SleepMgr
from dagr_selenium.utils import check_stop_file, get_urls, sort_watchlist

print('Dagr version:', dagr_revamped_version)

async def flush_cache(cache, slug):
    cache.flush(slug)

def shutdown_app(request):
    request.app['shutdown'].set()
    return json_response('ok')

async def update_cache(request, slug):
    urls = await request.json()
    cache = request.app['crawler_cache']

    cache.update(slug, urls)
    asyncio.create_task(flush_cache(cache, slug))
    return json_response('ok', headers={
        'Access-Control-Allow-Origin': '*'
    })


async def start_background_tasks(app):
    pass


async def cleanup_background_tasks(app):
    pass


async def cleanup_caches(app):
    sessions_cache = app['sessions']

    for _k, v in app['sessions'].items():
        await v.close()

    sessions_cache.clear()

    app['crawler_cache'].flush()


async def run_app():
    load_dotenv()

    manager = DAGRManager()
    config = manager.get_config()


    env_level = environ.get('dagr.monitor_watchlist_remote.logging.level', None)
    level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

    manager.set_mode('monitor_watchlist_remote')
    manager.init_logging(level_mapped)

    with manager.get_dagr():
        queueman_session = TCPKeepAliveSession()
        sessions = {
            'queueman': queueman_session
        }

        enqueue_url = get_urls(config)['enqueue']

        app = web.Application(client_max_size=1024**2 * 100)
        app.router.add_post('/shutdown', shutdown_app)
        app.router.add_post('/watchlist/items', lambda request: update_cache(request, 'watch_urls'))
        app.router.add_post('/trash/items', lambda request: update_cache(request, 'trash_urls'))

        app['shutdown'] = asyncio.Event()
        app['sessions'] = sessions
        app['work_items'] = dict()
        app['sleepmgr'] = SleepMgr(app, 300)
        app['crawler_cache'] = manager.get_cache()

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

        while not app['shutdown'].is_set() and not await check_stop_file(manager, 'STOP_REMOTE_MON'):
            await app['sleepmgr'].sleep()
            await sort_watchlist(manager, queueman_session, enqueue_url)

        print('Shutting down')

        await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print('KeyboardInterrupt')

    logging.shutdown()
