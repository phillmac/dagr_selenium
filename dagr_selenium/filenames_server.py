import asyncio
import base64
import gzip
import hashlib
import json
import logging
import mimetypes
from email.utils import parsedate
from io import BytesIO, StringIO
from logging.handlers import RotatingFileHandler
from operator import itemgetter
from os import environ
from os import path as os_path
from os import utime
from pathlib import Path, PosixPath, PurePosixPath
from shutil import copyfileobj
from tempfile import TemporaryFile
from threading import Event
from time import mktime, time, time_ns

import aiofiles
import portalocker
from aiofiles.os import (abspath, exists, makedirs, remove, rename, replace,
                         rmdir, scandir)
from aiohttp import ClientSession, web
from aiohttp.web_response import json_response
from dotenv import load_dotenv

from dagr_selenium.JSONHTTPBadRequest import JSONHTTPBadRequest
from dagr_selenium.SleepMgr import SleepMgr


class LockEntry():
    def __init__(self, diritem):
        self.__diritem = diritem
        self.__lockfile = diritem.joinpath('.lock')
        self.__lock = portalocker.Lock(
            self.__lockfile, fail_when_locked=True, flags=portalocker.LOCK_EX)
        self.__expiry = time() + 300
        self.__created = time()

        print(f"Lockfile {str(self.__lockfile)} exists:",
              self.__lockfile.exists())

    @property
    def details(self):
        return {
            'path': str(self.__diritem),
            'lockfile': str(self.__lockfile),
            'expiry': self.__expiry,
            'created': self.__created
        }

    def expired(self):
        return time() > self.__expiry

    def refresh(self):
        self.__expiry = time + 300

    def release(self):
        self.__lock.release()

        if self.__lockfile.exists():
            self.__lockfile.unlink()
        else:
            print(f"Lockfile {str(self.__lockfile)} already removed")


load_dotenv()
mimetypes.init()


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.2f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


async def get_subdir(app, dirpath):
    if isinstance(dirpath, str):
        dirpath = PurePosixPath(dirpath)
    cache_item = None
    dirs_cache = app['dirs_cache']
    for pnum in range(0, len(dirpath.parts)+1):
        pslice = dirpath.parts[0: pnum]
        # print('pslice:', pslice)
        fetched_cache_item = dirs_cache.get(pslice, None)
        if fetched_cache_item is None:
            dirname = dirpath.parts[pnum - 1]
            # print('dirname:', dirname)
            fetched_cache_item = Path(await (d async for d in await scandir(
                cache_item) if d.name == dirname and d.is_dir(follow_symlinks=False)).__anext__())
            dirs_cache[pslice] = fetched_cache_item
            cache_item = fetched_cache_item
        else:
            cache_item = fetched_cache_item
        # print('cache_item:', cache_item)
    return cache_item


async def create_logger(request):
    params = await request.json()

    print('POST /logger/create', params)

    hostMode, maxBytes, backupCount, frmt = itemgetter(
        'hostMode', 'maxBytes', 'backupCount', 'frmt')(params)

    loggers_cache = request.app['loggers_cache']

    if hostMode in loggers_cache:
        raise JSONHTTPBadRequest(reason='Logger already open')

    fn = f"{hostMode}.dagr.log.txt"
    handler = RotatingFileHandler(
        filename=fn, maxBytes=maxBytes, backupCount=backupCount)
    handler.setFormatter(logging.Formatter(frmt))
    loggers_cache[hostMode] = handler
    return json_response('ok')


async def handle_logger(request):
    params = await request.json()
    loggers_cache = request.app['loggers_cache']
    hostMode = params['hostMode']
    if not hostMode in loggers_cache:
        raise JSONHTTPBadRequest(reason='Logger not open')
    handler = loggers_cache[hostMode]
    recordParams = params['record']
    record = logging.makeLogRecord(recordParams)
    handler.handle(record)
    return json_response('ok')


async def remove_logger(request):
    params = await request.json()
    loggers_cache = request.app['loggers_cache']

    print('POST /logger/remove', params)

    hostMode = params['hostMode']
    if not hostMode in loggers_cache:
        raise JSONHTTPBadRequest(reason='Logger not open')
    handler = loggers_cache[hostMode]
    handler.flush()
    handler.close()
    del loggers_cache[hostMode]
    return json_response('ok')


async def query_logger(request):
    params = await request.json()

    print('GET /logger/exists', params)

    hostMode = params['hostMode']
    return json_response({'exists': hostMode in request.app['loggers_cache']})


async def list_dir(request):
    params = await request.json()

    path_param = params.get('path', None)

    include_dirs = params.get('include_dirs', False)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    try:
        t_now = time_ns()
        subdir = await get_subdir(request.app, path_param)
        resp = json_response([f.name async for f in await scandir(subdir)
                              if include_dirs or f.is_file()])
        t_spent = (time_ns() - t_now) / 1e6
        print('GET /dir', 'param:', path_param, 'subdir:',
              subdir, 'time:', '{:.2f}'.format(t_spent)+'ms')
        return resp
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')


async def file_exists(request):
    params = await request.json()
    print('GET /file/exists', params)
    path_param = params.get('path', None)
    itemname = params.get('itemname', None)
    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if itemname is None:
        raise JSONHTTPBadRequest(reason='not ok: itemname param missing')

    subdir = None
    result = False

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        print('Subdir does not exist')
        return json_response({'exists': False})
    t_now = time_ns()
    fnamepath = PurePosixPath(itemname)
    result = await exists(subdir.joinpath(fnamepath.name))
    t_spent = (time_ns() - t_now) / 1e6
    print('exists', result, 'time:', '{:.2f}'.format(t_spent)+'ms')
    if result:
        if not params.get('update_cache', None) is False:
            session = request.app['sessions']['update']
            asyncio.create_task(check_update_fn_cache(
                params, subdir, path_param, session))
    return json_response({'exists': result})


async def dir_exists(request):
    params = await request.json()
    print('GET /dir/exists', params)

    path_param = params.get('path', None)
    itemname = params.get('itemname', None)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    # if itemname is None:
    #     raise JSONHTTPBadRequest(reason='not ok: itemname param missing')

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        print('Subdir does not exist')
        return json_response({'exists': False})

    dir_item = subdir if itemname is None else subdir.joinpath(
        PurePosixPath(itemname))
    return json_response({'exists': await exists(dir_item)})


async def check_update_fn_cache(params, subdir, path_param, session=None):
    url = environ.get('FETCH_CACHE_URL', 'http://127.0.0.1:3003/file')

    try:
        if session is None:
            async with ClientSession(raise_for_status=True) as session:
                async with session.get(url, json=params) as resp:
                    if not (await resp.json())['exists']:
                        await update_fn_cache(subdir, path_param, session)
        else:
            async with session.get(url, json=params) as resp:
                if not (await resp.json())['exists']:
                    await update_fn_cache(subdir, path_param, session)
    except Exception as ex:
        print('Failed to check/update cache', ex)


async def update_fn_cache(subdir, path_param, session=None):
    t_now = time_ns()
    filenames = [f.name async for f in await scandir(subdir) if f.is_file()]
    t_spent = (time_ns() - t_now) / 1e6
    print('Time loading filenames list', '{:.2f}'.format(t_spent) + 'ms')

    url = environ.get('UPDATE_CACHE_URL', 'http://127.0.0.1:3003/files')

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

    print('GET /file_contents', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePosixPath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='not ok: filename does not exist')
    async with aiofiles.open(dest, 'r') as fh:
        resp = web.Response(text=await fh.read())
        resp.enable_compression()
        return resp


async def fetch_contents_b(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print('GET /file_contnents_b', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePosixPath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='not ok: filename does not exist')
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

    print('POST /json', {'path': path_param,
          'filename': filename, 'content': len(content)})

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    if content is None:
        raise JSONHTTPBadRequest(reason='not ok: content param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePosixPath(filename).name)
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

    print('POST /json_gz', {'path': path_param,
          'filename': filename, 'content': len(content)})

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    if content is None:
        raise JSONHTTPBadRequest(reason='not ok: content param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePosixPath(filename).name)
    await save_json(dest, content)
    return json_response('ok')


async def mk_dir(request):
    params = await request.json()
    print('POST /dir', params)

    path_param = params.get('path', None)
    dir_name = params.get('dir_name', None)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    # if dir_name is None:
    #     raise JSONHTTPBadRequest(reason='not ok: dir_name param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        if dir_name is None:
            dir_item = PosixPath(path_param)
            if not str(PosixPath.cwd()) == os_path.commonpath((Path.cwd(), await abspath(dir_item))):
                raise JSONHTTPBadRequest(
                    reason='not ok: bad relative new dir path')
        else:
            raise JSONHTTPBadRequest(reason='not ok: path does not exist')
    if subdir:
        dir_item = subdir if dir_name is None else subdir.joinpath(
            PurePosixPath(dir_name))
        if not str(subdir) == os_path.commonpath((subdir, await abspath(dir_item))):
            raise JSONHTTPBadRequest(
                reason='not ok: bad relative new dir path')

    try:
        await makedirs(dir_item)
    except FileExistsError:
        raise JSONHTTPBadRequest(reason='not ok: dir already exists')
    return json_response('ok')


async def update_time(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    mtime = params.get('mtime', None)

    print('POST /file/utime', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    if mtime is None:
        raise JSONHTTPBadRequest(reason='not ok: mtime param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')
    try:
        mod_time = mktime(parsedate(mtime))
    except:
        raise JSONHTTPBadRequest(reason='not ok: unable to handle mtime')
    utime(subdir.joinpath(PurePosixPath(filename).name), (mod_time, mod_time))
    return json_response('ok')


async def write_file(request):
    t_now = time_ns()

    with TemporaryFile(dir='/dev/shm') as tmp:

        reader = await request.multipart()

        params = dict()
        result = dict()
        result['size'] = 0

        async def set_params(f):
            params.update(await f.json())

        async def set_content(f):
            while chunk := await field.read_chunk():
                result['size'] += tmp.write(chunk)
            tmp.seek(0)

        field_actions = {
            'params': set_params,
            'content': set_content
        }

        while field := await reader.next():
            await field_actions[field.name](field)

        path_param = params.get('path', None)
        filename = params.get('filename', None)
        integrity = params.get('integrity', None)

        if path_param is None:
            print('POST /file', {'path': path_param, 'filename': filename,
                                 'size': result['size']})
            raise JSONHTTPBadRequest(reason='not ok: path param missing')

        if filename is None:
            print('POST /file', {'path': path_param, 'filename': filename,
                                 'size': result['size']})
            raise JSONHTTPBadRequest(reason='not ok: filename param missing')

        if integrity:
            hash_obj = hashlib.new(integrity['name'])
            block_size = 128 * hash_obj.block_size

            while chunk := tmp.read(block_size):
                hash_obj.update(chunk)

            if hash_obj.hexdigest() != integrity['hexdigest']:
                raise JSONHTTPBadRequest(reason='not ok: integrity mismatch')
            tmp.seek(0)

        subdir = None

        try:
            subdir = await get_subdir(request.app, path_param)
        except StopAsyncIteration:
            raise JSONHTTPBadRequest(reason='not ok: path does not exist')

        with subdir.joinpath(PurePosixPath(filename).name).open('wb') as dest:
            copyfileobj(tmp, dest)

        t_spent = (time_ns() - t_now) / 1e6
        print('POST /file', 'path', path_param, 'filename', filename, 'size',
              sizeof_fmt(result['size']), 'time:', '{:.2f}'.format(t_spent)+'ms')

        return json_response({'status': 'ok', 'result': result})


async def fetch_json(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)

    print('GET /json', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    dest = subdir.joinpath(PurePosixPath(filename).name)
    if not await exists(dest):
        raise web.HTTPNotFound(reason='not ok: filename not found')
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


async def rm_dir(request):
    params = await request.json()

    path_param = params.get('path', None)
    dir_name = params.get('dir_name', None)

    print('DEL /dir', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    # if dir_name is None:
    #     raise JSONHTTPBadRequest(reason='not ok: dir_name param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        if dir_name is None:
            dir_item = PosixPath(path_param)
            if not str(PosixPath.cwd()) == os_path.commonpath((Path.cwd(), await abspath(dir_item))):
                raise JSONHTTPBadRequest(
                    reason='not ok: bad relative rm dir path')
        else:
            raise JSONHTTPBadRequest(reason='not ok: path does not exist')
    if subdir:
        dir_item = subdir if dir_name is None else subdir.joinpath(
            PurePosixPath(dir_name))
        if not str(subdir) == os_path.commonpath((subdir, await abspath(dir_item))):
            raise JSONHTTPBadRequest(
                reason='not ok: bad relative rm dir path')

    try:
        await rmdir(dir_item)
    except FileNotFoundError:
        raise JSONHTTPBadRequest(reason='not ok: dir does not exist')
    return json_response('ok')


async def query_lock(request):
    params = await request.json()

    print('GET /dir/lock', params)

    path_param = params.get('path', None)

    print('query lock', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    if await exists(subdir):
        locked = request.app['locks_cache'].get(str(subdir), None) is not None
        return json_response({'locked': locked})

    raise JSONHTTPBadRequest(reason='not ok: subdir does not exist')


async def aquire_lock(request):
    params = await request.json()

    print('POST /dir/lock', params)

    path_param = params.get('path', None)

    print(params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    if not await exists(subdir):
        raise JSONHTTPBadRequest(reason='not ok: subdir does not exist')
    subdir_str = str(subdir)
    locks_cache = request.app['locks_cache']
    if locks_cache.get(subdir_str, None) is not None:
        raise JSONHTTPBadRequest(reason='not ok: path is already locked')
    locks_cache[subdir_str] = LockEntry(subdir)
    return json_response('ok')


async def release_lock(request):
    params = await request.json()

    path_param = params.get('path', None)

    print('DELETE /dir/lock', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    if await exists(subdir):
        subdir_str = str(subdir)
        locks_cache = request.app['locks_cache']
        if locks_cache.get(subdir_str, None) is None:
            raise JSONHTTPBadRequest(reason='not ok: path is not locked')
        locks_cache[subdir_str].release()
        del locks_cache[subdir_str]
        return json_response('ok')
    raise JSONHTTPBadRequest(reason='not ok: filename does not exist')


async def refresh_lock(request):
    params = await request.json()

    path_param = params.get('path', None)

    print('PATCH /dir/lock', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    if await exists(subdir):
        subdir_str = str(subdir)
        locks_cache = request.app['locks_cache']
        if locks_cache.get(subdir_str, None) is None:
            raise JSONHTTPBadRequest(reason='not ok: path is not locked')
        locks_cache[subdir_str].refresh()
        return json_response('ok')
    raise JSONHTTPBadRequest(reason='not ok: filename does not exist')


async def replace_item(request):
    params = await request.json()

    path_param = params.get('path', None)
    filename = params.get('filename', None)
    new_filename = params.get('new_filename', None)

    print('POST /replace', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if filename is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    if new_filename is None:
        raise JSONHTTPBadRequest(reason='not ok: new_filename param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    oldfn = subdir.joinpath(PurePosixPath(filename).name)
    newfn = subdir.joinpath(PurePosixPath(new_filename).name)

    if await exists(newfn):
        # if await exists(oldfn):
        #     await remove(oldfn)

        # await rename(newfn, oldfn)

        await replace(newfn, oldfn)
        return json_response('ok')
    raise JSONHTTPBadRequest(reason='not ok: filename does not exist')


async def rename_item(item_type, request):
    params = await request.json()

    path_param = params.get('path', None)
    itemname = params.get('itemname', None)
    new_itemname = params.get('new_itemname', None)

    print(f"PATCH /{item_type}", params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if itemname is None:
        raise JSONHTTPBadRequest(reason='not ok: itemname param missing')

    if new_itemname is None:
        raise JSONHTTPBadRequest(reason='not ok: new_itemname param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    newin = subdir.joinpath(new_itemname)

    if not str(subdir) == os_path.commonpath((subdir, await abspath(newin))):
        raise JSONHTTPBadRequest(reason='not ok: bad relative new item path')

    if item_type == 'dir':
        try:
            oldin = await get_subdir(request.app,
                                     Path(path_param).joinpath(itemname))
        except StopAsyncIteration:
            raise JSONHTTPBadRequest(reason='not ok: item does not exist')

    else:
        oldin = subdir.joinpath(PurePosixPath(itemname).name)
        if not oldin.is_file(follow_symlinks=False):
            raise JSONHTTPBadRequest(reason='not ok: item is not a file')

    if not await exists(oldin):
        raise JSONHTTPBadRequest(reason='not ok: item does not exist')

    if await exists(newin):
        raise JSONHTTPBadRequest(
            reason='not ok: new item already exists')

    await rename(oldin, newin)

    return json_response('ok')


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
    app = web.Application(client_max_size=1024**2 * 100)
    app.router.add_get('/ping', lambda request: json_response('pong'))
    app.router.add_get('/json', fetch_json)
    app.router.add_get('/file_contents', fetch_contents)
    app.router.add_get('/file_contents_b', fetch_contents_b)
    app.router.add_get('/file/exists', file_exists)
    app.router.add_get('/dir', list_dir)
    app.router.add_get('/dir/exists', dir_exists)
    app.router.add_get('/dir/lock', query_lock)
    app.router.add_get(
        '/locks', lambda request: json_response(dict((k, request.app['locks_cache'][k].details) for k in request.app['locks_cache'])))
    app.router.add_post('/dir', mk_dir)
    app.router.add_delete('/dir', rm_dir)
    app.router.add_delete('/dir/lock', release_lock)
    app.router.add_patch('/dir', lambda request: rename_item('dir', request))
    app.router.add_post('/file/utime', update_time)
    app.router.add_post('/dir/lock', aquire_lock)
    app.router.add_patch('/dir/lock', refresh_lock)
    app.router.add_post('/json', update_json)
    app.router.add_post('/json_gz', update_json_gz)
    app.router.add_post('/file', write_file)
    app.router.add_post('/replace', replace_item)
    app.router.add_post('/logger/create', create_logger)
    app.router.add_post('/logger/append', handle_logger)
    app.router.add_post('/logger/remove', remove_logger)
    app.router.add_get('/logger/exists', query_logger)
    app.router.add_post('/shutdown', shutdown_app)

    app['dirs_cache'] = dict()
    app['loggers_cache'] = dict()
    app['locks_cache'] = dict()
    app['dirs_cache'][tuple()] = Path.cwd()
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
