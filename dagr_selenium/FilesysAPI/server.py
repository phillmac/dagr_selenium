import asyncio
import json
import mimetypes
from os import environ
from pathlib import Path
from threading import Event

from aiohttp import ClientSession, web
from aiohttp.web_response import json_response
from dotenv import load_dotenv

from dagr_selenium.SleepMgr import SleepMgr
from dagr_selenium.version import version
from .api import APIManager


def shutdown_app(request):
    request.app['shutdown'].set()
    request.app['sleepmgr'].cancel_sleep()
    return json_response('ok')


async def prune_locks(app):
    locks_cache = app['locks_cache']
    while True:
        for k, v in locks_cache.items():
            print(f"Checking lock expiry for {k}")
            if v.expired():
                print(f"Pruning expired lock for {k}")
                v.release()
                del locks_cache[k]
            else:
                print(f"{k} is current")
        await asyncio.sleep(300)


async def start_background_tasks(app):
    app['prune_locks'] = asyncio.create_task(prune_locks(app))


async def cleanup_background_tasks(app):
    app['prune_locks'].cancel()


async def cleanup_caches(app):
    dirs_cache = app['dirs_cache']
    loggers_cache = app['loggers_cache']
    locks_cache = app['locks_cache']
    sessions_cache = app['sessions']

    dirs_cache.clear()

    for _k, v in loggers_cache.items():
        v.flush()
        v.close()
    loggers_cache.clear()

    for _k, v in locks_cache.items():
        v.release()
    locks_cache.clear()

    for _k, v in app['sessions'].items():
        await v.close()

    sessions_cache.clear()


async def run_app():
    load_dotenv()
    mimetypes.init()

    app = web.Application(client_max_size=1024**2 * environ.get(
        'CLIENT_MAX_SIZE', 200))
    app.router.add_get('/ping', lambda request: json_response('pong'))
    app.router.add_get(
        '/json', lambda request: APIManager.handle_request(request, 'fetch_json'))
    app.router.add_get(
        '/file_contents', lambda request: APIManager.handle_request(request, 'fetch_contents'))
    app.router.add_get(
        '/file_contents_b', lambda request: APIManager.handle_request(request, 'fetch_contents_b'))
    app.router.add_get(
        '/file/exists', lambda request: APIManager.handle_request(request, 'file_exists'))
    app.router.add_get(
        '/file/stat', lambda request: APIManager.handle_request(request, 'file_stat'))
    app.router.add_get(
        '/dir', lambda request: APIManager.handle_request(request, 'list_dir'))
    app.router.add_get(
        '/dir/exists', lambda request: APIManager.handle_request(request, 'dir_exists'))
    app.router.add_get(
        '/dir/lock', lambda request: APIManager.handle_request(request, 'query_lock'))
    app.router.add_get(
        '/locks', lambda request: json_response(dict((k, request.app['locks_cache'][k].details) for k in request.app['locks_cache'])))
    app.router.add_post(
        '/dir', lambda request: APIManager.handle_request(request, 'mk_dir'))
    app.router.add_delete(
        '/dir', lambda request: APIManager.handle_request(request, 'rm_dir'))
    app.router.add_delete(
        '/dir/lock', lambda request: APIManager.handle_request(request, 'release_lock'))
    app.router.add_patch(
        '/dir', lambda request: APIManager.handle_request(request, 'rename_dir'))
    app.router.add_post(
        '/file/utime', lambda request: APIManager.handle_request(request, 'update_time'))
    app.router.add_post(
        '/dir/lock', lambda request: APIManager.handle_request(request, 'aquire_lock'))
    app.router.add_patch(
        '/dir/lock', lambda request: APIManager.handle_request(request, 'refresh_lock'))
    app.router.add_post(
        '/json', lambda request: APIManager.handle_request(request, 'update_json'))
    app.router.add_post(
        '/json_gz', lambda request: APIManager.handle_request(request, 'update_json_gz'))
    app.router.add_post(
        '/file', lambda request: APIManager.handle_request(request, 'write_file'))
    app.router.add_post(
        '/replace', lambda request: APIManager.handle_request(request, 'replace_item'))
    app.router.add_post(
        '/logger/create', lambda request: APIManager.handle_request(request, 'create_logger'))
    app.router.add_post(
        '/logger/append', lambda request: APIManager.handle_request(request, 'handle_logger'))
    app.router.add_post(
        '/logger/remove', lambda request: APIManager.handle_request(request, 'remove_logger'))
    app.router.add_get(
        '/logger/exists', lambda request: APIManager.handle_request(request, 'query_logger'))
    app.router.add_post(
        '/shutdown', lambda request: APIManager.handle_request(request, 'shutdown_app'))

    app['dirs_cache'] = dict()
    app['loggers_cache'] = dict()
    app['locks_cache'] = dict()
    app['cwd'] = Path.cwd()
    app['dirs_cache'][tuple()] = app['cwd']
    app['shutdown'] = Event()
    app['sleepmgr'] = SleepMgr(app)
    app['sessions'] = dict()
    app['sessions']['update'] = ClientSession(raise_for_status=True)

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.on_cleanup.append(cleanup_caches)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port=environ.get(
        'FILENAME_SERVER_LISTEN_PORT', 3002))
    await site.start()

    names = sorted(str(s.name) for s in runner.sites)
    print('DAGR Selenium Filenames Server Version', version)
    print(
        "======== Running on {} ========\n"
        "(Press CTRL+C to quit)".format(", ".join(names))
    )

    while not app['shutdown'].is_set():
        await app['sleepmgr'].sleep()

    print('Shutting down')

    await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print(KeyboardInterrupt)
