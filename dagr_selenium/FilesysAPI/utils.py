import json
from io import StringIO
from pathlib import Path, PosixPath, PurePosixPath

import aiofiles
from aiofiles.os import exists, remove, rename, scandir, stat


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.2f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


async def stat_to_json(fp):
    s_obj = await stat(fp)
    return {k: getattr(s_obj, k) for k in dir(s_obj) if k.startswith('st_')}


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
