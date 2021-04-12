import asyncio
import base64
import gzip
import json
import logging
import os
from io import BytesIO, StringIO
from logging.handlers import RotatingFileHandler
from operator import itemgetter
from pathlib import Path, PurePath
from time import time_ns

import aiofiles
import aiofiles.os as aiofiles_os
from aiohttp import ClientSession, web
from aiohttp.web_response import json_response


class DirsCache():
    def __init__(self):
        self.__dirs_cache = dict()
        self.__dirs_cache[tuple()] = Path.cwd()

    def get_subdir(self, dirpath):
        if not isinstance(dirpath, Path):
            dirpath = Path(dirpath)
        cache_item = None
        for pnum in range(0, len(dirpath.parts)+1):
            pslice = dirpath.parts[0: pnum]
            # print('pslice:', pslice)
            fetched_cache_item = self.__dirs_cache.get(pslice, None)
            if fetched_cache_item is None:
                dirname = dirpath.parts[pnum - 1]
                # print('dirname:', dirname)
                fetched_cache_item = next((d for d in cache_item.iterdir(
                ) if d.name == dirname and d.is_dir()))
                self.__dirs_cache[pslice] = fetched_cache_item
                cache_item = fetched_cache_item.resolve()
            else:
                cache_item = fetched_cache_item.resolve()
            # print('cache_item:', cache_item)
        return cache_item


dirs_cache = DirsCache()


class LoggersCache():
    def __init__(self):
        self.__loggers_cache = dict()

    async def create(self, request):
        params = await request.json()
        print(params)
        hostMode, maxBytes, backupCount, frmt = itemgetter('hostMode', 'maxBytes', 'backupCount', 'frmt')(params)
        if hostMode in self.__loggers_cache:
            raise web.HTTPBadRequest(reason='Logger already open')
        fn = f"{hostMode}.dagr.log.txt"
        handler = RotatingFileHandler(filename=fn, maxBytes=maxBytes, backupCount=backupCount)
        handler.setFormatter(logging.Formatter(frmt))
        self.__loggers_cache[hostMode] = handler
        return json_response('ok')


    async def handle(self, request):
        params = await request.json()
        hostMode = params['hostMode']
        if not hostMode in self.__loggers_cache:
            raise web.HTTPBadRequest(reason='Logger not open')
        handler = self.__loggers_cache[hostMode]
        recordParams = params['record']
        record = logging.makeLogRecord(recordParams)
        handler.handle(record)
        return json_response('ok')


    async def remove(self, request):
        params = await request.json()
        print(params)
        hostMode = params['hostMode']
        if not hostMode in self.__loggers_cache:
            raise web.HTTPBadRequest(reason='Logger not open')
        handler = self.__loggers_cache[hostMode]
        handler.flush()
        handler.close
        del self.__loggers_cache[hostMode]
        return json_response('ok')

logger_cache = LoggersCache()

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

async def get_fileslist(request):
    params = await request.json()
    path_param = params.get('path', None)
    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    try:
        subdir = dirs_cache.get_subdir(path_param)
        print('param:', path_param, 'subdir:', subdir)
        return json_response([f.name for f in os.scandir(subdir) if f.is_file()])
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

async def get_file_exists(request):
    params = await request.json()
    print(params)
    path_param = params.get('path', None)
    filename = params.get('filename', None)
    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    subdir = None
    exists = False

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        print('Subdir does not exist')
        return json_response({'exists': False})
    t_now = time_ns()
    fnamepath = PurePath(filename)
    exists = subdir.joinpath(fnamepath.name).exists()
    t_spent = (time_ns() - t_now) / 1e6
    print('exists', exists, 'time:', '{:.2f}'.format(t_spent)+'ms')
    if exists:
        if not params.get('update_cache', None) is False:
            await BackgroundTask().run(check_update_fn_cache, (params, subdir, path_param))
    return json_response({'exists': exists})

async def check_update_fn_cache(params, subdir, path_param, session=None):
    url = 'http://127.0.0.1:3003/file'

    try:
        if session is None:
            async with ClientSession(raise_for_status=True) as session:
                async with session.get(url, json=params) as resp:
                    if not (await resp.json())['exists']:
                        await update_fn_cache(subdir, path_param)
        else:
            async with session.get(url, json=params) as resp:
                if not (await resp.json())['exists']:
                    await update_fn_cache(subdir, path_param)
    except Exception as ex:
        print('Failed to check/update cache', ex)

async def update_fn_cache(subdir, path_param, session = None):
    t_now = time_ns()
    filenames = [f.name for f in os.scandir(subdir) if f.is_file()]
    t_spent = (time_ns() - t_now) / 1e6
    print('Time loading filenames list', '{:.2f}'.format(t_spent) + 'ms')

    url ='http://127.0.0.1:3003/files'
    data = {'path': path_param, 'filenames': filenames}

    t_now = time_ns()
    if session is None:
        async with ClientSession(raise_for_status=True) as session:
            await session.post(url, json=data)
    else:
        await session.post(url, json=data)
    t_spent = (time_ns() - t_now) / 1e6

    print('Time updating fn cache', '{:.2f}'.format(t_spent) + 'ms')

async def fetch_contents(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePath(filename).name)
    if not dest.exists():
        raise web.HTTPNotFound(reason='not ok: filename does not exist')
    async with aiofiles.open(dest, 'r') as fh:
        resp = web.Response(text=await fh.read())
        resp.enable_compression()
        return resp



async def fetch_contents_b(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePath(filename).name)
    if not dest.exists():
        raise web.HTTPNotFound(reason='not ok: filename does not exist')
    async with aiofiles.open(dest, 'rb') as fh:
        resp = web.Response(body=await fh.read())
        resp.enable_compression()
        return resp

async def update_json(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    content = params.get('content', None)

    print({'path': path_param, 'filename': filename, 'content': len(content)})

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    if content is None:
        raise web.HTTPBadRequest(reason='not ok: content param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePath(filename).name)
    await save_json(dest, content)
    return json_response('ok')

async def update_json_gz(request):
    if request.content_type == 'application/json':
        params = await request.json()

        path_param = params.get('path', None)
        filename = params.get('filename', None)
        content_b64 = params.get('content_gz', None)
        buffer = BytesIO(base64.b64decode(content_b64))
        decompressor = gzip.GzipFile(fileobj=buffer, mode='rb')
        content = json.load(decompressor)

    elif request.content_type == 'application/gzip':
        buffer = BytesIO(await request.content.read())
        decompressor = gzip.GzipFile(fileobj=buffer, mode='rb')
        params = json.load(decompressor)
        path_param = params.get('path', None)
        filename = params.get('filename', None)
        content = params.get('content', None)

    print({'path': path_param, 'filename': filename, 'content': len(content)})

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    if content is None:
        raise web.HTTPBadRequest(reason='not ok: content param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePath(filename).name)
    await save_json(dest, content)
    return json_response('ok')

async def fetch_json(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePath(filename).name)
    if not dest.exists():
        raise web.HTTPNotFound(reason='not ok: filename not found')
    resp =  json_response(await load_json(dest))
    resp.enable_compression()
    return resp

async def buffered_file_write(fpath, content):
    buffer = StringIO()
    json.dump(content, buffer, indent=4, sort_keys=True)
    buffer.seek(0)
    temp = fpath.with_suffix('.tmp')
    async with aiofiles.open(temp, 'w') as fh:
        await fh.write(buffer.read())
    await aiofiles_os.rename(temp, fpath)

async def backup_cache_file(fpath):
    backup = fpath.with_suffix('.bak')
    if fpath.exists():
        if backup.exists():
            await aiofiles_os.remove(backup)
        await aiofiles_os.rename(fpath, backup)

async def save_json(fpath, data, do_backup=True):
    if do_backup:
        await backup_cache_file(fpath)
    await buffered_file_write(fpath, data)

async def load_json(fpath):
    async with aiofiles.open(fpath, 'r') as fh:
        buffer = StringIO(await fh.read())
        return json.load(buffer)

async def replace(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    new_filename = params.get('new_filename', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise web.HTTPBadRequest(reason='not ok: filename param missing')

    if new_filename is None:
        raise web.HTTPBadRequest(reason='not ok: new_filename param missing')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='not ok: path does not exist')

    oldfn = subdir.joinpath(PurePath(filename).name)
    newfn = subdir.joinpath(PurePath(new_filename).name)

    if oldfn.exists():
        await aiofiles_os.remove(oldfn)

    await aiofiles_os.rename(newfn, oldfn)

    return json_response('ok')








def run_app():
    app = web.Application()
    app.router.add_get('/json', fetch_json)
    app.router.add_get('/file_contents', fetch_contents)
    app.router.add_get('/file_contents_b', fetch_contents_b)
    app.router.add_get('/files_list', get_fileslist)
    app.router.add_get('/file_exists', get_file_exists)
    app.router.add_post('/json', update_json)
    app.router.add_post('/replace', replace)
    app.router.add_post('/json_gz', update_json_gz)
    app.router.add_post('/logger/create', logger_cache.create)
    app.router.add_post('/logger/append', logger_cache.handle)
    app.router.add_post('/logger/remove', logger_cache.remove)
    web.run_app(app, host='0.0.0.0', port=3002)


if __name__ == '__main__':
    run_app()
