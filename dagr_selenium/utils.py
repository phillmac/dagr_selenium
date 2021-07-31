import asyncio
import logging
from itertools import islice
from os import environ
from pathlib import Path
from pprint import pformat
from time import time

from dagr_revamped.DAGRCache import DAGRCache
from dagr_revamped.exceptions import DagrCacheLockException, DagrException
from dagr_revamped.utils import (artist_from_url, get_html_name, get_remote_io,
                                 http_post_raw, sleep)
from selenium.common.exceptions import NoSuchElementException

from dagr_selenium.DeviantResolveCache import DeviantResolveCache
from dagr_selenium.BulkCache import BulkCache

logger = logging.getLogger(__name__)


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def check_stop_file(manager, fname=None):
    if fname is None:
        mode = manager.mode
        if not mode is None:
            fname = f"STOP_{mode.upper()}"
    try:
        filenames = [fname, f"{manager.get_host_mode()}.dagr.stop"]
        foldernames = ['~', manager.get_config().output_dir]

        for dn in foldernames:
            dirp = dn if isinstance(dn, Path) else Path(
                dn).expanduser().resolve()
            for fp in filenames:
                if fp is not None:
                    filep = dirp.joinpath(fp)
                    if filep.exists():
                        logger.info(F"Detected stop file {filep} exists")
                        return True
        return False
    except:
        logger.exception("Unable to check stop file")
        return False


def get_urls(config):
    queueman_fetch_url = environ.get('QUEUEMAN_FETCH_URL', None) or config.get(
        'dagr.plugins.selenium', 'queueman_fetch_url', key_errors=False) or 'http://127.0.0.1:3005/item'

    queueman_enqueue_url = environ.get('QUEUEMAN_ENQUEUE_URL', None) or config.get(
        'dagr.plugins.selenium', 'queueman_enqueue_url', key_errors=False) or 'http://127.0.0.1:3005/items'

    fncache_update_url = environ.get('FNCACHE_UPDATE_URL', None) or config.get(
        'dagr.plugins.selenium', 'fncache_update_url', key_errors=False) or 'http://127.0.0.1:3005/items'

    urls = {
        'fetch':            queueman_fetch_url,
        'enqueue':          queueman_enqueue_url,
        'fncache_update':   fncache_update_url
    }

    logger.info('Queman Urls:')
    logger.info(pformat(urls))

    return urls


async def is_deactivated(deviant, manager):
    with manager.get_browser().get_r_context() as browser:
        if not deviant.lower() in browser.current_url.lower():
            browser.open(f"https://deviantart.com/{deviant}")
        try:
            headline = browser.find_element_by_css_selector('h1.headline')
            return headline.text == 'Deactivated Account' or headline.text == 'Forbidden'
        except NoSuchElementException:
            return False

async def query_resolve_cache(resolve_cache, deviant):
    logger.info(f"Attempting to resolve {deviant}")
    try:
        return resolve_cache.query(deviant)
    except DagrException:
        logger.warning(f"Deviant {deviant} is listed as deactivated")
        raise

async def resolve_query_deviantart(manager, resolve_cache, deviant):

    with manager.get_browser().get_r_context():
        try:
            deviant, _group = manager.get_dagr().resolve_deviant(deviant)
            resolve_cache.add(deviant)
            return deviant
        except DagrException:
            if is_deactivated(deviant, manager):
                logger.warning(f"Deviant {deviant} is deactivated")
                resolve_cache.add(deviant, deactivated=True)
                logger.log(
                    level=15, msg=f"Added {deviant} to deactivated list")
                raise
            logger.warning(f"Unable to resolve deviant {deviant}")
            raise


async def resolve_deviant(manager, deviant, resolve_cache=None):
    if resolve_cache is None:
        resolve_cache = DeviantResolveCache(manager.get_cache())
    if cached_result := await query_resolve_cache(resolve_cache, deviant):
        return cached_result
    return await resolve_query_deviantart(manager, resolve_cache, deviant)

async def resolve_artists(manager, artists, flush=True):
    resolved_artists = {}
    resolve_cache = DeviantResolveCache(manager.get_cache())
    uncached = {}

    for k, v in artists.items():
        try:
            if resolved := await query_resolve_cache(resolve_cache, k):
                resolved_artists[resolved] = v
            else:
                uncached[k] = v
        except DagrException:
            continue
    if len(uncached) > 0:
        with manager.get_browser().get_r_context():
            for k, v in uncached.items():
                try:
                    resolved = await resolve_query_deviantart(manager, resolve_cache, k)
                    resolved_artists[resolved] = v
                except DagrException:
                    continue

    if flush:
        resolve_cache.flush()

    return resolved_artists


async def flush_errors_to_queue(manager, session, endpoint):
    cache = manager.get_cache()
    cache_slug = 'error_items'
    errors = await cache.query(cache_slug)
    items = []
    for e in errors:
        i = dict(e)
        try:
            if (not 'resolved' in i) or (not i['resolved']):
                i['deviant'] = await resolve_deviant(manager, i['deviant'])
                i['resolved'] = True
        except:
            pass
        items.append(i)
    try:
        await http_post_raw(session, endpoint, json=items)
        await cache.remove(cache_slug, errors)
    except:
        logger.exception('Error while enqueueing items')


def collate_artist_pages(pages):
    artists = {}
    for p in pages:
        _artist_url_p, artist_name, _shortname = artist_from_url(p)
        if not artist_name in artists:
            artists[artist_name] = []
        artists[artist_name].append(p)
    return artists


async def enqueue_artists(manager, artists, sorted_pages=set()):
    crawler_cache = manager.get_cache()
    config = manager.get_config()
    batch_enqueued = 0
    dcount = len(artists)
    progress = 0
    queued_artists = []
    pending_slug = 'pending_gallery'

    for deviant, pages in artists.items():
        try:
            addst = time()
            with DAGRCache.with_queue_only(config, 'gallery', deviant) as cache:
                base_dir_exists = cache.cache_io.dir_exists()
                logger.log(
                    level=15, msg=f"Sorting pages into {cache.rel_dir}, dir exists: {base_dir_exists}")
                if not base_dir_exists:
                    cache.cache_io.mkdir()
                    logger.log(level=15, msg=f"Created dir {cache.rel_dir}")
                enqueued = cache.update_queue(pages)
                q_size = len(cache.get_queue())
                logger.log(level=15, msg=f"Queue size is {q_size}")
                if q_size > 0 or enqueued > 0:
                    queued_artists.append(deviant)
                    crawler_cache.update(pending_slug, [deviant])
                progress += 1
                logger.info(
                    f"Adding {enqueued} pages to {deviant} [{progress}/{dcount}] took {'{:.4f}'.format(time() - addst)} seconds")
                batch_enqueued += enqueued
                sorted_pages.update(pages)
        except DagrCacheLockException:
            pass
    logger.info(f"Sorted {batch_enqueued} pages")
    return queued_artists


async def sort_pages(manager, to_sort, **kwargs):
    resort = kwargs.get('resort', False)
    queued_only = kwargs.get('queued_only', True)
    flush = kwargs.get('flush', True)
    disable_resolve = kwargs.get('disable_resolve', False)
    sorted_pages = set()
    crawler_cache = manager.get_cache()
    cache_slug = 'sorted'
    history = crawler_cache.query(cache_slug)
    artists = None

    if not resort:
        sorted_pages.update(history)

    logger.info(f"Loaded {len(sorted_pages)} sorted pages")
    unsorted_pages = [p for p in to_sort if not p in sorted_pages]
    ucount = len(unsorted_pages)
    logger.info(f"Loaded {ucount} unsorted pages")
    if ucount > 0:
        artists = collate_artist_pages(unsorted_pages)
        if not disable_resolve:
            artists = await resolve_artists(manager, artists, flush)

        queued_artists = await enqueue_artists(manager, artists, sorted_pages)

        pcount = len(sorted_pages - history)

        if resort:
            sorted_pages.update(history)
        crawler_cache.update(cache_slug, sorted_pages)
        if flush:
            try:
                crawler_cache.flush(cache_slug)
            except:
                logger.exception('Error while flushing caches')
        logger.info(f"Added {pcount} pages to sorted list")
    return queued_artists if queued_only else list(artists.keys())


async def queue_items(crawler_cache, session, endpoint,  mode, deviants, priority=100, full_crawl=False, resolved=None):
    cache_slug = f"pending_{mode}"
    if not isinstance(deviants, set):
        deviants = set(deviants)
    deviants.update(crawler_cache.query(cache_slug))
    logger.info(pformat(deviants))
    for deviantschunk in chunk(deviants, 5):
        items = [{'mode': mode, 'deviant': d, 'priority': priority,
                  'full_crawl': full_crawl, 'resolved': resolved} for d in deviantschunk]
        logger.info(
            f"Sending {mode} {deviantschunk} to queue manager")
        try:
            http_post_raw(session=session, endpoint=endpoint, json=items)
        except:
            logger.exception('Error while enquing items')
            try:
                crawler_cache.update(cache_slug, deviantschunk)
                crawler_cache.flush(cache_slug)
            except:
                logger.exception('Error while caching pending items')
            await asyncio.sleep(900)
        else:
            logger.info(
                f"Pruning cache; removing {deviantschunk} from {cache_slug}")
            try:
                crawler_cache.remove(cache_slug, deviantschunk)
            except:
                logger.exception('Error while pruning pending items cache')


async def update_bulk_galleries(crawler_cache, deviants, bulk_cache=None):
    if bulk_cache is None:
        bulk_cache = BulkCache(crawler_cache)

    bulk_deviants = set(i['deviant'].lower()
                        for i in bulk_cache.query('gallery'))
    if len(bulk_deviants) > 0:
        bglen = bulk_cache.count('gallery')

        await bulk_cache.add([BulkCache.create_item('gallery', d)
                    for d in deviants if not d.lower() in bulk_deviants])
        delta = bulk_cache.count('gallery') - bglen
        if delta > 0:
            await bulk_cache.flush()
            logger.info(f"Added {delta} deviants to bulk gallery list")
        return delta
    return 0


async def queue_galleries(crawler_cache, session, endpoint, deviants, priority=100, full_crawl=False, resolved=None):
    await queue_items(crawler_cache, session=session, endpoint=endpoint, mode='gallery', deviants=deviants, priority=priority,
                full_crawl=full_crawl, resolved=resolved)


async def queue_favs(crawler_cache, session, endpoint, deviants, priority=100, full_crawl=False):
    await queue_items(crawler_cache, session=session, endpoint=endpoint, mode='favs',
                deviants=deviants, priority=priority, full_crawl=full_crawl)


async def sort_queue_galleries(manager, session, endpoint, pages, resort=False, flush=True):
    deviants_sorted = await sort_pages(manager=manager, to_sort=pages, resort=resort, flush=flush)
    await update_bulk_galleries(crawler_cache=manager.get_cache(), deviants=deviants_sorted)
    await queue_galleries(crawler_cache=manager.get_cache(), session=session, endpoint=endpoint, deviants=deviants_sorted, priority=50, resolved=True)


async def sort_watchlist(manager, session, endpoint, resort=False):
    cache = manager.get_cache()
    cache_slug = 'watch_urls'
    await sort_queue_galleries(manager=manager, session=session, endpoint=endpoint, pages=cache.query(cache_slug), resort=resort)

async def sort_all(manager, session, endpoint, resort=False):
    cache = manager.get_cache()
    pages=set()
    for cache_slug in [ 'watch_urls', 'trash_urls']:
        pages.update(cache.query(cache_slug))
    await sort_queue_galleries(manager=manager, session=session, endpoint=endpoint, pages=pages, resort=resort)
