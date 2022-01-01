import asyncio
import base64
import gzip
import hashlib
import json
import logging
import mimetypes
from email.utils import parsedate
from io import BytesIO
from logging.handlers import RotatingFileHandler
from operator import itemgetter
from os import path as os_path
from os import utime
from pathlib import Path, PosixPath, PurePosixPath
from shutil import copyfileobj
from tempfile import TemporaryFile
from time import mktime, time_ns

import aiofiles
from aiofiles.os import (abspath, exists, makedirs, rename, replace, rmdir,
                         scandir)
from aiohttp.web import HTTPNotFound, Response
from aiohttp.web_response import json_response
from dagr_selenium.JSONHTTPErrors import JSONHTTPBadRequest, JSONHTTPNotFound

from ..LockEntry import LockEntry
from ..utils import (check_update_fn_cache, get_subdir, load_json, save_json,
                     sizeof_fmt, stat_to_json)


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
    if arguments := recordParams.get('args', None):
        recordParams['args'] = tuple(arguments)
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
        print('Subdir does not exist', path_param)
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


async def file_stat(request):
    params = await request.json()

    print('GET /file/stat', params)

    path_param = params.get('path', None)
    itemname = params.get('itemname', None)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if itemname is None:
        raise JSONHTTPBadRequest(reason='not ok: itemname param missing')

    subdir = None

    try:
        subdir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        print('Subdir does not exist', path_param)
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    try:
        return json_response({
            'stat': await stat_to_json(subdir.joinpath(PurePosixPath(itemname).name))})
    except FileNotFoundError:
        print('Item does not exist', itemname)
        raise JSONHTTPBadRequest(reason='not ok: item does not exist')


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
        print('Subdir does not exist', path_param)
        return json_response({'exists': False})

    dir_item = subdir if itemname is None else subdir.joinpath(
        PurePosixPath(itemname))
    return json_response({'exists': await exists(dir_item)})


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
        raise HTTPNotFound(reason='not ok: filename does not exist')
    async with aiofiles.open(dest, 'r') as fh:
        resp = Response(text=await fh.read())
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
        raise HTTPNotFound(reason='not ok: filename does not exist')
    async with aiofiles.open(dest, 'rb') as fh:
        ct, _enc = mimetypes.guess_type(dest)

        headers = {
            'Content-Disposition': f'inline; filename="{dest.name}"'
        }
        resp = Response(body=await fh.read(), content_type=ct, headers=headers)
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
    app = request.app
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
        subdir = await get_subdir(app, path_param)
    except StopAsyncIteration:
        if dir_name is None:
            dir_item = PosixPath(path_param)
            if not str(app['cwd']) == os_path.commonpath((app['cwd'], await abspath(dir_item))):
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

            hexdigest = hash_obj.hexdigest()
            if hexdigest != integrity['hexdigest']:
                print(
                    f"Failed integrity for {path_param} {filename}. {hexdigest} != {integrity['hexdigest']}")
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
              sizeof_fmt(result['size']), 'integrity:', integrity, 'time:', '{:.2f}'.format(t_spent)+'ms')

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
        raise JSONHTTPNotFound(reason='not ok: filename not found')
    resp = json_response(await load_json(dest))
    resp.enable_compression()
    return resp


async def rm_dir(request):
    app = request.app
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
        subdir = await get_subdir(app, path_param)
    except StopAsyncIteration:
        if dir_name is None:
            dir_item = PosixPath(path_param)
            if not str(app['cwd']) == os_path.commonpath((app['cwd'], await abspath(dir_item))):
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
    dest_subdir = params.get('dest_subdir', None)
    dest_fname = params.get('dest_fname', None)
    src_subdir = params.get('src_subdir', None)
    src_fname = params.get('src_fname', None)

    print('POST /replace', params)

    if path_param is None:
        raise JSONHTTPBadRequest(reason='not ok: path param missing')

    if src_fname is None:
        raise JSONHTTPBadRequest(reason='not ok: filename param missing')

    if dest_fname is None:
        raise JSONHTTPBadRequest(reason='not ok: new_filename param missing')

    subdir = None

    try:
        basedir = await get_subdir(request.app, path_param)
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: path does not exist')

    try:
        dest_path = await get_subdir(request.app, f"{path_param}/{dest_subdir}")
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: dest subdir does not exist')

    try:
        src_path = await get_subdir(request.app, f"{path_param}/{src_subdir}")
    except StopAsyncIteration:
        raise JSONHTTPBadRequest(reason='not ok: src subdir does not exist')

    oldfn = src_path.joinpath(PurePosixPath(src_fname).name)
    newfn = subdir.joinpath(PurePosixPath(dest_fname).name)

    if await exists(newfn):
        await replace(newfn, oldfn)
        return json_response('ok')
    raise JSONHTTPBadRequest(reason='not ok: filename does not exist')


async def rename_file(request):
    return await __rename_item('file', request)


async def rename_dir(request):
    return await __rename_item('dir', request)


async def __rename_item(item_type, request):
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
