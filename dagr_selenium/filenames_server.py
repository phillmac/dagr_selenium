from email.utils import parsedate
from time import mktime
import asyncio
import base64
import gzip
import json
import logging
import mimetypes
import os
from io import BytesIO, StringIO
from logging.handlers import RotatingFileHandler
from operator import itemgetter
from os import utime
from pathlib import Path, PurePath
from shutil import copyfileobj
from tempfile import TemporaryFile
from time import time_ns

import aiofiles
from aiofiles.os import exists, rename, remove, mkdir
from aiohttp import ClientSession, web
from aiohttp.web_response import json_response
from dotenv import load_dotenv

load_dotenv()

mimetypes.init()


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
                fetched_cache_item = Path(next((d for d in os.scandir(
                    cache_item) if d.name == dirname and d.is_dir() and not d.is_symlink())))
                self.__dirs_cache[pslice] = fetched_cache_item
                cache_item = fetched_cache_item
            else:
                cache_item = fetched_cache_item
            # print('cache_item:', cache_item)
        return cache_item


dirs_cache = DirsCache()


class LoggersCache():
    def __init__(self):
        self.__loggers_cache = dict()

    async def create(self, request):
        params = await request.json()
        print(params)
        hostMode, maxBytes, backupCount, frmt = itemgetter(
            'hostMode', 'maxBytes', 'backupCount', 'frmt')(params)
        if hostMode in self.__loggers_cache:
            raise web.HTTPBadRequest(reason='Logger already open')
        fn = f"{hostMode}.dagr.log.txt"
        handler = RotatingFileHandler(
            filename=fn, maxBytes=maxBytes, backupCount=backupCount)
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

    async def exists(self, request):
        params = await request.json()
        hostMode = params['hostMode']
        return json_response({'exists': hostMode in self.__loggers_cache})


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


async def fileslist(request):
    params = await request.json()
    path_param = params.get('path', None)
    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    try:
        subdir = dirs_cache.get_subdir(path_param)
        print('param:', path_param, 'subdir:', subdir)
        return json_response([f.name for f in os.scandir(subdir) if f.is_file()])
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')


async def file_exists(request):
    params = await request.json()
    print(params)
    path_param = params.get('path', None)
    itemname = params.get('itemname', None)
    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if itemname is None:
        raise web.HTTPBadRequest(reason='"not ok: itemname param missing"')

    subdir = None
    result = False

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        print('Subdir does not exist')
        return json_response({'exists': False})
    t_now = time_ns()
    fnamepath = PurePath(itemname)
    result = await exists(subdir.joinpath(fnamepath.name))
    t_spent = (time_ns() - t_now) / 1e6
    print('exists', result, 'time:', '{:.2f}'.format(t_spent)+'ms')
    if result:
        if not params.get('update_cache', None) is False:
            await BackgroundTask().run(check_update_fn_cache, (params, subdir, path_param))
    return json_response({'exists': result})



async def dir_exists(request):
    params = await request.json()
    print(params)

    path_param = params.get('path', None)
    itemname = params.get('itemname', None)

    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if itemname is None:
        raise web.HTTPBadRequest(reason='"not ok: itemname param missing"')
    try:
        dirs_cache.get_subdir(Path(path_param, PurePath(itemname).name))
    except StopIteration:
        print('Subdir does not exist')
        return json_response({'exists': False})
    return json_response({'exists': True})


async def check_update_fn_cache(params, subdir, path_param, session=None):
    url = os.environ.get('FETCH_CACHE_URL', 'http://127.0.0.1:3003/file')

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


async def update_fn_cache(subdir, path_param, session=None):
    t_now = time_ns()
    filenames = [f.name for f in os.scandir(subdir) if f.is_file()]
    t_spent = (time_ns() - t_now) / 1e6
    print('Time loading filenames list', '{:.2f}'.format(t_spent) + 'ms')

    url = os.environ.get('UPDATE_CACHE_URL', 'http://127.0.0.1:3003/files')

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
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    dest = subdir.joinpath(PurePath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='"not ok: filename does not exist"')
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
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    dest = subdir.joinpath(PurePath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='"not ok: filename does not exist"')
    async with aiofiles.open(dest, 'rb') as fh:
        ct, _enc = mimetypes.guess_type(dest)

        headers = {
            'Content-Disposition': f'inline; filename="{dest.name}"'
        }
        resp = web.Response(body=await fh.read(), content_type=ct, headers=headers)
        resp.enable_compression()
        return resp


async def update_json(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    content = params.get('content', None)

    print({'path': path_param, 'filename': filename, 'content': len(content)})

    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    if content is None:
        raise web.HTTPBadRequest(reason='"not ok: content param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

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
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    if content is None:
        raise web.HTTPBadRequest(reason='"not ok: content param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    dest = subdir.joinpath(PurePath(filename).name)
    await save_json(dest, content)
    return json_response('ok')


async def mk_dir(request):
    params = await request.json()
    print(params)

    path_param = params.get('path', None)
    dir_name = params.get('dir_name', None)

    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if dir_name is None:
        raise web.HTTPBadRequest(reason='"not ok: dir_name param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    try: 
        await mkdir(subdir.joinpath(PurePath(dir_name).name))
    except FileExistsError:
        raise web.HTTPBadRequest(reason='"not ok: dir already exists"')
    return json_response('ok')

async def update_time(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    mtime = params.get('mtime', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    if mtime is None:
        raise web.HTTPBadRequest(reason='"not ok: mtime param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    utime(
        subdir.joinpath(PurePath(filename).name),
        mktime(parsedate(mtime))
    )
    return json_response('ok')


async def write_file(request):
    with TemporaryFile(dir='/dev/shm') as tmp:

        reader = await request.multipart()

        params = dict()
        params['size'] = 0

        async def set_params(f):
            params.update(await f.json())

        async def set_content(f):
            while chunk := await field.read_chunk():
                params['size'] += len(chunk)
                tmp.write(chunk)
            tmp.seek(0)

        field_actions = {
            'params': set_params,
            'content': set_content
        }

        while field := await reader.next():
            await field_actions[field.name](field)

        path_param = params.get('path', None)
        filename = params.get('filename', None)

        print({'path': path_param, 'filename': filename,
              'size': params['size']})

        if path_param is None:
            raise web.HTTPBadRequest(reason='"not ok: path param missing"')

        if filename is None:
            raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

        subdir = None

        try:
            subdir = dirs_cache.get_subdir(path_param)
        except StopIteration:
            raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

        with subdir.joinpath(PurePath(filename).name).open('wb') as dest:
            copyfileobj(tmp, dest)

        return json_response('ok')


async def fetch_json(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print(params)

    if path_param is None:
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    dest = subdir.joinpath(PurePath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='"not ok: filename not found"')
    resp = json_response(await load_json(dest))
    resp.enable_compression()
    return resp


async def buffered_file_write(fpath, content):
    buffer = StringIO()
    json.dump(content, buffer, indent=4, sort_keys=True)
    buffer.seek(0)
    temp = fpath.with_suffix('.tmp')
    async with aiofiles.open(temp, 'w') as fh:
        await fh.write(buffer.read())
    await rename(temp, fpath)


async def backup_cache_file(fpath):
    backup = fpath.with_suffix('.bak')
    if await exists(fpath):
        if await exists(backup):
            await remove(backup)
        await rename(fpath, backup)


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
        raise web.HTTPBadRequest(reason='"not ok: path param missing"')

    if filename is None:
        raise web.HTTPBadRequest(reason='"not ok: filename param missing"')

    if new_filename is None:
        raise web.HTTPBadRequest(reason='"not ok: new_filename param missing"')

    subdir = None

    try:
        subdir = dirs_cache.get_subdir(path_param)
    except StopIteration:
        raise web.HTTPBadRequest(reason='"not ok: path does not exist"')

    oldfn = subdir.joinpath(PurePath(filename).name)
    newfn = subdir.joinpath(PurePath(new_filename).name)

    if await exists(newfn):
        if await exists(oldfn):
            await remove(oldfn)

        await rename(newfn, oldfn)

        return json_response('ok')
    raise web.HTTPBadRequest(reason='"not ok: filename does not exist"')


def run_app():
    app = web.Application(client_max_size=1024**2 * 100)
    app.router.add_get('/json', fetch_json)
    app.router.add_get('/file_contents', fetch_contents)
    app.router.add_get('/file_contents_b', fetch_contents_b)
    app.router.add_get('/file/exists', file_exists)
    app.router.add_get('/dir', fileslist)
    app.router.add_get('/dir/exists', dir_exists)
    app.router.add_post('/dir', mk_dir)
    app.router.add_post('/file/utime', update_time)
    app.router.add_post('/json', update_json)
    app.router.add_post('/json_gz', update_json_gz)
    app.router.add_post('/file', write_file)
    app.router.add_post('/replace', replace)
    app.router.add_post('/logger/create', logger_cache.create)
    app.router.add_post('/logger/append', logger_cache.handle)
    app.router.add_post('/logger/remove', logger_cache.remove)
    app.router.add_get('/logger/exists', logger_cache.exists)
    web.run_app(app, host='0.0.0.0', port=os.environ.get('LISTEN_PORT', 3002))


if __name__ == '__main__':
    run_app()
