import logging
import os
from itertools import islice
from pathlib import Path
from pprint import pformat, pprint
from time import sleep, time

from dagr_revamped.dagr_logging import log
from dagr_revamped.DAGRCache import DAGRCache, DagrCacheLockException
from dagr_revamped.DAGRHTTPIo import DAGRHTTPIo
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.lib import DagrException
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dagr_revamped.utils import (artist_from_url, dump_html, load_json,
                                 save_json)
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException)
from selenium.common.exceptions import \
    TimeoutException as SeleniumTimeoutException
from urllib3.util.retry import Retry

click_sleep_time = 0.300
monitor_sleep = 600

session = TCPKeepAliveSession()

manager = DAGRManager()
config = manager.get_config()

logger = logging.getLogger(__name__)

log(__name__, level=logging.INFO, msg=f"Output dir: {config.output_dir}")


def fetch_watchlist_item():
    link_href = None
    content = None
    browser = manager.get_browser()
    try:
        content = browser.find_element_by_css_selector(
            'div[data-hook=content_row-1]').find_element_by_tag_name('div')
    except NoSuchElementException:
        return None
    except:
        logger.exception('Unable to fetch content')
        browser.refresh()
    if content:
        try:
            link = content.find_element_by_css_selector(
                'a[data-hook=deviation_link]')
            link_href = link.get_attribute('href')
        except StaleElementReferenceException:
            logger.error('Failed to get link: stale')
            pprint(content.get_attribute('innerHTML'))
        except:
            logger.exception('Failed to get link')
            browser.refresh()
        try:
            tries = 0
            last = None
            button = None
            browser.move_to_element(content)
            tickbox = find_tickbox_parent(content)
            browser.click_element(tickbox)
            while button is None:
                button = find_remove_bttn(browser)
                sleep(0.3)
            while tries < 4:
                try:
                    browser.click_element(button)
                    browser.wait_stale(tickbox, delay=10)
                except SeleniumTimeoutException as ex:
                    logger.warning(
                        f"Timeout waiting for remove button click tries:{tries}")
                    tries += 1
                    last = ex
                except NoSuchElementException:
                    break
                else:
                    last = None
                    break
            if not last is None:
                raise last
        except:
            logger.exception('Failed to click remove buttn')
            raise
        return link_href
    return None


def find_remove_bttn(context):
    for bttn in context.find_elements_by_tag_name('button'):
        if is_remove_bttn(bttn):
            return bttn


def find_tickbox_parent(context):
    for label in context.find_elements_by_tag_name('label'):
        for inp in label.find_elements_by_tag_name('input'):
            inp_type = inp.get_attribute('type')
            if inp_type == 'checkbox':
                return label

def find_load_comments(context):
    for btn in context.find_elements_by_tag_name('button'):
        if 'load previous comments' in btn.get_attribute('innerText').lower():
            logger.info('Found load comments')
            return btn

def find_load_more(context):
    for btn in context.find_elements_by_tag_name('button'):
        if 'load more' in btn.get_attribute('innerText').lower():
            logger.info('Found load more')
            return btn

def is_remove_bttn(bttn):
    innerHTML = bttn.get_attribute('innerHTML')
    return 'Remove' in innerHTML and not bttn.text.lower() == 'removed'


def crawl_watchlist():
    browser = manager.get_browser()
    browser.open('https://www.deviantart.com/notifications/watch')
    browser.wait_ready()
    cache = manager.get_cache()
    cache_slug = 'watch_urls'
    watch_urls = cache.query(cache_slug)
    last_url = None
    start_count = len(watch_urls)
    while True:
        try:
            page_url = fetch_watchlist_item()
            if last_url == page_url and last_url in watch_urls:
                raise Exception(f"Already got {last_url}")
            last_url = page_url
            if page_url is None:
                break
            watch_urls.add(page_url)
            sleep(click_sleep_time)
            cache.update(cache_slug, watch_urls)
        except:
            logger.exception('Error while crawling watchlist')
    delta = len(watch_urls) - start_count
    if delta > 0:
        cache.flush(cache_slug)
    logger.info(f"Crawled {delta} pages")
    return watch_urls, delta


def sort_watchlist():
    cache = manager.get_cache()
    cache_slug = 'watch_urls'
    sort_pages(cache.query(cache_slug))


def sort_pages(to_sort, resort=False, queued_only=True, flush=True):
    sorted_pages = set()
    crawler_cache = manager.get_cache()
    cache_slug = 'sorted'
    pending_slug = 'pending_gallery'
    history = crawler_cache.query(cache_slug)
    if not resort:
        try:
            sorted_pages.update(history)
        except:
            pass
    logger.info(f"Loaded {len(sorted_pages)} sorted pages")
    unsorted_pages = [p for p in to_sort if not p in sorted_pages]
    logger.info(f"Loaded {len(unsorted_pages)} unsorted pages")
    artists = {}
    for p in unsorted_pages:
        _artist_url_p, artist_name, _shortname = artist_from_url(p)
        if not artist_name in artists:
            artists[artist_name] = []
        artists[artist_name].append(p)
    batch_enqueued = 0
    dcount = len(artists.keys())
    progress = 0
    queued_artists = []
    for deviant, pages in artists.items():
        try:
            # try:
            #     deviant, _group = manager.get_dagr().resolve_deviant(deviant)
            # except DagrException:
            #     logger.warning(f"Unable to resolve deviant {deviant}")
            addst = time()
            with DAGRCache.with_queue_only(config, 'gallery', deviant) as cache:
                base_dir_exists = cache.base_dir.exists()
                logger.log(
                    level=15, msg=f"Sorting pages into {cache.base_dir}, dir exists: {base_dir_exists}")
                if not base_dir_exists:
                    cache.base_dir.make_dirs(parents=True)
                    logger.log(level=15, msg=f"Created dir {cache.base_dir}")
                enqueued = cache.update_queue(pages)
                if enqueued > 0:
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
    pcount = len(sorted_pages - history)
    if pcount > 0:
        if resort:
            sorted_pages.update(history)
        crawler_cache.update(cache_slug, sorted_pages)
        try:
            if flush:
                crawler_cache.flush(cache_slug)
        except:
            logger.exception('Error while flushing cache')
        logger.info(f"Added {pcount} pages to sorted list")
    return queued_artists if queued_only else list(artists.keys())


def cache_info(cache):
    pprint({
        "path": cache.base_dir,
        "queue": len(cache.get_queue()),
        "no link": len(cache.get_nolink()),
        "existing pages": len(cache.existing_pages),
        "artists": len(cache.artists.keys()),
        "last crawled": cache.last_crawled
    })


def cache_stats(mode, deviant, mval=None):
    try:
        with DAGRCache.get_cache(config, mode, deviant, mval) as cache:
            cache_info(cache)
    except DagrCacheLockException:
        pass


def gallery_stats(deviant):
    cache_stats('gallery', deviant)


def crawl_pages(mode, deviant, **kwargs):
    crawler = manager.get_crawler()
    return crawler.crawl('', mode, deviant, **kwargs)


def crawl_trash(full_crawl=False):
    browser = manager.get_browser()
    crawler = manager.get_crawler()
    cache = manager.get_cache()
    cache_slug = 'trash_urls'
    browser.open(
        'https://www.deviantart.com/notifications/watch/deviations?folder=archived'
    )
    if full_crawl:
        pages = None
    else:
        pages = cache.query(cache_slug)
    return crawler.crawl_action(cache_slug, pages=pages)


def rip_trash(full_crawl=False, resort=False):
    trash = crawl_trash(full_crawl=full_crawl)
    deviants = sort_pages(trash, resort=resort, flush=False)
    update_bulk_galleries(deviants)
    queue_galleries(deviants)


def rip_pages(cache, pages, full_crawl=False, disable_filter=False, callback=None, **kwargs):
    dagr = manager.get_dagr()
    logger.info(f"Ripping {len(pages)} pages")
    if full_crawl:
        logger.log(level=15, msg='Full crawl mode')
    if disable_filter:
        logger.log(level=15, msg='Filter disabled')
    dagr.process_deviations(
        cache, pages, disable_filter=disable_filter, callback=callback, **kwargs)
    cache.save_extras(full_crawl)

def rip(mode, deviant, mval=None, full_crawl=False, disable_filter=False, crawl_offset=None, no_crawl=None, **kwargs):
    callback = None

    if crawl_offset:
        logger.log(level=15, msg=f"crawl_offset: {crawl_offset}")

    def load_comments():
        browser = manager.get_browser()
        load_comments = find_load_comments(browser)
        if load_comments:
            browser.click_element(load_comments)
        while load_more := find_load_more(browser):
            browser.click_element(load_more)

    def dump_callback(page, content):
        if kwargs.get('load_more', None):
            load_comments()
        dump_html(cache.base_dir.joinpath('.html'), page, content)

    if '_html' in mode:
        mode = mode.replace('_html', '')
        callback = dump_callback
    try:
        pages = crawl_pages(mode, deviant, mval=mval,
                            full_crawl=full_crawl, crawl_offset=crawl_offset, no_crawl=no_crawl)
        with DAGRCache.with_queue_only(config, mode, deviant, mval, dagr_io=DAGRHTTPIo) as cache:
            if pages:
                enqueued = cache.update_queue(pages)
                logger.info(f"Add {enqueued} pages to {deviant}")
            pages.update(cache.get_queue())
            if no_crawl:
                pages.update(cache.existing_pages)
            exclude = [*cache.get_premium(), *cache.get_httperrors()]
            pages = [p for p in pages if not p in exclude]
            rip_pages(cache, pages, full_crawl,
                      disable_filter=disable_filter, callback=callback, **kwargs)
    except DagrCacheLockException:
        pass


def rip_nolink(mode, deviant, mval=None):
    dagr = manager.get_dagr()
    try:
        with DAGRCache.get_cache(config, mode, deviant, mval, dagr_io=DAGRHTTPIo) as cache:
            pages = cache.get_nolink()
            logger.info(f"Ripping {len(pages)} pages")
            dagr.process_deviations(cache, pages)
            rcount = cache.prune_nolink()
            logger.log(
                level=15, msg=f"Removed {rcount} pages from no-link list")
            cache.save_extras(None)
            dagr.print_errors()
            dagr.print_dl_total()
    except DagrCacheLockException:
        pass


def rip_queue(mode, deviant, mval=None):
    dagr = manager.get_dagr()
    try:
        with DAGRCache.get_cache(config, mode, deviant, mval, dagr_io=DAGRHTTPIo) as cache:
            pages = cache.get_queue()
            logger.info(f"Ripping {len(pages)} pages")
            dagr.process_deviations(cache, pages)
            rcount = cache.prune_queue()
            logger.log(
                level=15, msg=f"Removed {rcount} pages from queue")
            cache.save_extras(None)
            dagr.print_errors()
            dagr.print_dl_total()
    except DagrCacheLockException:
        pass


def rip_gallery(deviant, full_crawl=False):
    rip('gallery', deviant, full_crawl=full_crawl)


def rip_favs(deviant, full_crawl=False):
    rip('favs', deviant, full_crawl=full_crawl)


def rip_galleries(deviants, full_crawl=False):
    dcount = len(deviants)
    completed = 0
    for deviant in deviants:
        rip_gallery(deviant, full_crawl=full_crawl)
        completed += 1
        logger.info(f"[{completed}/{dcount}] completed")


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def queue_items(mode, deviants, priority=100, full_crawl=False):
    cache = manager.get_cache()
    cache_slug = f"pending_{mode}"
    if not isinstance(deviants, set):
        deviants = set(deviants)
    deviants.update(cache.query(cache_slug))
    deviants_filter = cache.query('deviants_filter')
    cache.flush('deviants_filter')
    for deviantschunk in chunk(deviants, 5):
        items = [{'mode': mode, 'deviant': d, 'priority': priority,
                  'full_crawl': full_crawl} for d in deviantschunk if not d.lower() in (df.lower() for df in deviants_filter)]
        logger.info(
            f"Sending {mode} {deviantschunk} to queue manager")
        try:
            r = session.post(
                'http://192.168.20.50:3002/items', json=items)
            r.raise_for_status()
        except:
            logger.exception('Error while enquing items')
            try:
                cache.update(cache_slug, deviantschunk)
                cache.flush(cache_slug)
            except:
                logger.exception('Error while caching pending items')
            sleep(900)
        else:
            logger.info(
                f"Pruning cache; removing {deviantschunk} from {cache_slug}")
            try:
                cache.remove(cache_slug, deviantschunk)
            except:
                logger.exception('Error while pruning pending items cache')
    sleep(180)


def queue_galleries(deviants, priority=100, full_crawl=False):
    queue_items('gallery', deviants, priority=priority, full_crawl=full_crawl)


def queue_favs(deviants, priority=100, full_crawl=False):
    queue_items('favs', deviants, priority=priority, full_crawl=full_crawl)


def flush_errors_to_queue():
    cache = manager.get_cache()
    cache_slug = 'error_items'
    errors = cache.query(cache_slug)
    items = []
    for e in errors:
        i = dict(e)
        try:
            if (not 'resolved' in i) or (not i['resolved']):
                i['deviant'] = resolve_deviant(i['deviant'])
                i['resolved'] = True
        except:
            pass
        items.append(i)
    try:
        r = session.post('http://192.168.20.50:3002/items', json=items)
        r.raise_for_status()
        cache.remove(cache_slug, errors)
    except:
        logger.exception('Error while enqueueing items')


def resolve_deviant(deviant):
    try:
        deviant, _group = manager.get_dagr().resolve_deviant(deviant)
        return deviant
    except DagrException:
        logger.warning(f"Unable to resolve deviant {deviant}")
        raise


def load_bulk(filename='dagr_bulk.json'):
    try:
        return load_json(config.output_dir.joinpath(filename))
    except:
        logger.exception('Failed to load bulk list')
        raise


def update_bulk_galleries(deviants):
    bulk = load_bulk()
    bulk_deviants = [d.lower() for d in bulk.get('gallery', [])]
    bglen = len(bulk['gallery'])
    bulk['gallery'] += [d for d in deviants if not d.lower() in bulk_deviants]
    delta = len(bulk['gallery']) - bglen
    if delta > 0:
        save_json(config.output_dir.joinpath('dagr_bulk.json'), bulk)
        logger.info(f"Added {delta} deviants to bulk gallery list")
    return delta


def rip_galleries_bulk(full_crawl=False):
    deviants = set()
    deviants.update(load_bulk().get('gallery'))
    rip_galleries(deviants, full_crawl=full_crawl)


def monitor_watchlist_action():
    pages = set()
    try:
        pages, npcount = crawl_watchlist()
        if npcount > 0:
            deviants = sort_pages(pages)
            logger.info(pformat(deviants))
            update_bulk_galleries(deviants)
            queue_galleries(deviants, priority=50)
        else:
            logger.info('Watchlist crawl found no new pages')
    except:
        logger.exception('Error while crawling watch list')
    return pages


def check_stop_file(fname=None):
    if fname is None:
        mode = manager.mode
        if not mode is None:
            fname = f"STOP_{mode.upper()}"
    try:
        filenames = [fname, f"{manager.get_host_mode()}.dagr.stop"]
        foldernames = ['~', config.output_dir]

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


def monitor_watchlist():
    while not check_stop_file('STOP_MON_WATCHLIST'):
        crawlst = time()
        monitor_watchlist_action()
        delay_needed = monitor_sleep - (time() - crawlst)
        if delay_needed > 0:
            logger.log(
                level=15, msg=f"Need to sleep for {'{:.2f}'.format(delay_needed)} seconds")
            sleep(delay_needed)
        logger.log(
            level=15, msg=f"Rip watchlist took {'{:.4f}'.format(time() - crawlst)} seconds")


def update_bookmarks(mode, deviant, mval):
    cache = manager.get_cache()
    cache_slug = 'bookmarks'
    cache.query(cache_slug)
    cache.update(cache_slug, [(mode, deviant, mval)])
    cache.flush(cache_slug)
