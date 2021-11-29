import logging
from itertools import islice
from os import environ
from pathlib import Path
from pprint import pformat, pprint
from time import sleep, time
import re

from dagr_revamped.builtin_plugins.classes.DAGRHTTPIo import DAGRHTTPIo
from dagr_revamped.dagr_logging import log
from dagr_revamped.DAGRCache import DAGRCache
from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.exceptions import DagrCacheLockException, DagrException
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dagr_revamped.utils import (artist_from_url, get_html_name, get_remote_io,
                                 http_post_raw, load_json, save_json)
from selenium.common.exceptions import (InvalidSessionIdException,
                                        NoSuchElementException,
                                        StaleElementReferenceException)
from selenium.common.exceptions import \
    TimeoutException as SeleniumTimeoutException
from selenium.common.exceptions import WebDriverException
from urllib3.util.retry import Retry

from .DeviantResolveCache import DeviantResolveCache

click_sleep_time = 0.300
monitor_sleep = environ.get('MONITOR_SLEEP', 300)

rip_trash_sleep = environ.get('RIP_TRASH_SLEEP', 300)

session = TCPKeepAliveSession()

manager = DAGRManager()
config = manager.get_config()

logger = logging.getLogger(__name__)

log(__name__, level=logging.INFO, msg=f"Output dir: {config.output_dir}")


queueman_fetch_url = environ.get('QUEUEMAN_FETCH_URL', None) or config.get(
    'dagr.plugins.selenium', 'queueman_fetch_url', key_errors=False) or 'http://127.0.0.1:3005/item'


queueman_enqueue_url = environ.get('QUEUEMAN_ENQUEUE_URL', None) or config.get(
    'dagr.plugins.selenium', 'queueman_enqueue_url', key_errors=False) or 'http://127.0.0.1:3005/items'


logger.info('Queman Urls:')
logger.info(pformat({
    'queueman_fetch_url':  queueman_fetch_url,
    'queueman_enqueue_url': queueman_enqueue_url
}))


def fetch_content_row():
    browser = manager.get_browser()
    try:
        return browser.find_element_by_css_selector(
            'div[data-hook=content_row-1]').find_element_by_tag_name('div')
    except NoSuchElementException:
        return None
    except:
        logger.exception('Unable to fetch content')
        browser.refresh()
    return


def remove_watchlist_item(content_row):
    browser = manager.get_browser()
    tries = 0
    button = None
    browser.move_to_element(content_row)
    try:
        tickbox = find_tickbox_parent(content_row)
    except NoSuchElementException:
        logger.warning('Unable to find tickbox')
        raise
    browser.click_element(tickbox)
    while button is None:
        try:
            button = find_remove_bttn(browser)
        except NoSuchElementException:
            logger.warning('Unable to find remove button')
            raise
        sleep(0.3)
    while True:
        try:
            browser.click_element(button)
            browser.wait_stale(tickbox, delay=10)
        except StaleElementReferenceException:
            logger.warning(
                f"Remove button is stale")
            browser.refresh()
            break
        except SeleniumTimeoutException:
            logger.warning(
                f"Timeout waiting for remove button click tries:{tries}")
            tries += 1
            if tries >= 4:
                raise
        else:
            break


def fetch_deviation_link(content_row):
    browser = manager.get_browser()
    try:
        link = content_row.find_element_by_css_selector(
            'a[data-hook=deviation_link]')
        return link.get_attribute('href')
    except StaleElementReferenceException:
        logger.error('Failed to get link: stale')
        raise
    except NoSuchElementException:
        for h3 in content_row.find_elements_by_tag_name('h3'):
            if h3.text.lower() == "this notification is no longer available":
                logger.warning('Notification unavailable')
                return None
        raise
    except:
        logger.exception('Failed to get link')
        browser.refresh()
        raise


def fetch_watchlist_item():
    content_row = fetch_content_row()
    if content_row:
        content_inner_html = content_row.get_attribute('innerHTML')
        try:
            link_href = fetch_deviation_link(content_row)
        except:
            pprint(content_inner_html)
        try:
            remove_watchlist_item(content_row)
        except:
            logger.exception('Failed to click remove buttn')
            pprint(content_inner_html)
            manager.get_browser().refresh()
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
        except InvalidSessionIdException:
            raise
        except:
            logger.exception('Error while crawling watchlist')
    delta = len(watch_urls) - start_count
    if delta > 0:
        cache.flush(cache_slug)
    logger.info(f"Crawled {delta} pages")
    return watch_urls, delta


def sort_watchlist(resort=False):
    cache = manager.get_cache()
    cache_slug = 'watch_urls'
    sort_queue_galleries(cache.query(cache_slug), resort=resort)


def sort_pages(to_sort, resort=False, queued_only=True, flush=True, disable_resolve=None):
    sorted_pages = set()
    crawler_cache = manager.get_cache()
    resolve_cache = DeviantResolveCache(crawler_cache)
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
            if not disable_resolve:
                deviant = resolve_deviant(deviant, resolve_cache)
        except DagrException:
            continue
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
    pcount = len(sorted_pages - history)
    if pcount > 0:
        if resort:
            sorted_pages.update(history)
        crawler_cache.update(cache_slug, sorted_pages)
        try:
            if flush:
                crawler_cache.flush(cache_slug)
                # resolve_cache.flush() # needs to be async
        except:
            logger.exception('Error while flushing cachees')
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
    sort_queue_galleries(trash, resort=resort, flush=False)


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


def load_comments():
    browser = manager.get_browser()
    logger.info('Loading comments')
    load_more_st = time()
    click_count = browser.execute_async_script("""
const done = arguments[0];
let clickCount = 0;
(async () => {
  while(true) {
    const loadMore = Array.from(document.getElementsByTagName('button')).find(b=>b.innerText=='Load More');
    if (! loadMore) break
    console.info('Clicking Load More');
    clickCount++;
    loadMore.click();
    await new Promise(r => setTimeout(r, 5000));
  }
  done({clickCount});
})()
    """,
    timeout=900).get('clickCount')
    logger.info('Clicking load more %s times took %.4f seconds', click_count, time() - load_more_st)

def dump_callback(page, content, cache, load_more=None, **kwargs):

    if load_more:
        load_comments()
    try:
        html_name = get_html_name(page).name
        if not cache.cache_io.exists(subdir='.html', fname=html_name, update_cache=False):
            cache.cache_io.write_bytes(
                content, subdir='.html', fname=html_name)
    except:
        logger.exception('Error while dumping html')


def fetch_lit_images(current_page, cache, verify_exists=None):
    dagr = manager.get_dagr()
    for img in current_page.find_all('img', {'src': re.compile('https://images-wixmp-')}):
        logger.log(15, 'Found literature image')
    # processor = dagr.deviation_processor(
    #             dagr, cache, link, verify_exists=verify_exists)


def handle_callbacks(page_type, page_link, current_page, page_content, cache, dump_html, **kwargs):

    if page_type == 'literature':
        fetch_lit_images(current_page, cache)

    if dump_html:
        dump_callback(page_link, page_content, cache, **kwargs)


def rip(mode, deviant=None, mval=None, full_crawl=False, disable_filter=False, crawl_offset=None, no_crawl=None, disable_resolve=None, resolved=None, **kwargs):
    if crawl_offset:
        logger.log(level=15, msg=f"crawl_offset: {crawl_offset}")

    if not deviant is None and not resolved and not disable_resolve:
        try:
            deviant = resolve_deviant(deviant)
        except DagrException:
            pass

    if 'html' in mode:
        kwargs['dump_html'] = True
        mode = mode.replace('_html', '')

    try:
        pages = crawl_pages(mode, deviant, mval=mval,
                            full_crawl=full_crawl, crawl_offset=crawl_offset, no_crawl=no_crawl)
        with DAGRCache.with_queue_only(config, mode, deviant, mval, dagr_io=DAGRHTTPIo) as cache:

            if kwargs.get('dump_html', None) and not cache.cache_io.dir_exists(dir_name='.html'):
                logger.info('Creating .html dir')
                cache.cache_io.mkdir(dir_name='.html')

            if pages:
                enqueued = cache.update_queue(pages)
                logger.info(f"Add {enqueued} pages to {deviant}")
            pages.update(cache.get_queue())
            if no_crawl:
                pages.update(cache.existing_pages)
            exclude = [*cache.get_premium(), *cache.get_httperrors()]
            pages = [p for p in pages if not p in exclude]
            rip_pages(cache, pages, full_crawl,
                      disable_filter=disable_filter, callback=lambda **cbkwargs: handle_callbacks(cache=cache, **cbkwargs, **kwargs), **kwargs)
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


def rip_gallery(deviant, full_crawl=False, disable_resolve=None):
    rip('gallery', deviant, full_crawl=full_crawl,
        disable_resolve=disable_resolve)


def rip_favs(deviant, full_crawl=False, disable_resolve=None):
    rip('favs', deviant, full_crawl=full_crawl, disable_resolve=disable_resolve)


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


def queue_items(mode, deviants, priority=100, full_crawl=False, resolved=None):
    cache = manager.get_cache()
    cache_slug = f"pending_{mode}"
    if not isinstance(deviants, set):
        deviants = set(deviants)
    deviants.update(cache.query(cache_slug))
    logger.info(pformat(deviants))
    for deviantschunk in chunk(deviants, 5):
        items = [{'mode': mode, 'deviant': d, 'priority': priority,
                  'full_crawl': full_crawl, 'resolved': resolved} for d in deviantschunk]
        logger.info(
            f"Sending {mode} {deviantschunk} to queue manager")
        try:
            http_post_raw(session, queueman_enqueue_url, json=items)
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


def queue_galleries(deviants, priority=100, full_crawl=False, resolved=None):
    queue_items('gallery', deviants, priority=priority,
                full_crawl=full_crawl, resolved=resolved)


def queue_favs(deviants, priority=100, full_crawl=False):
    queue_items('favs', deviants, priority=priority, full_crawl=full_crawl)


def flush_errors_to_queue():
    cache = manager.get_cache()
    cache_slug = 'error_items'
    errors = cache.query(cache_slug)
    config = manager.get_config()
    nd_modes = config.get('deviantart', 'ndmodes').split(',')
    items = []
    for e in errors:
        i = dict(e)
        mode = i['mode']
        try:
            if mode not in nd_modes:
                if (not 'resolved' in i) or (not i['resolved']):
                    i['deviant'] = resolve_deviant(i['deviant'])
                    i['resolved'] = True
                else:
                    logger.info(f"Deviant for item {i} already resolved")
            else:
                logger.info(f"Skipping resolve for mode {mode}")
        except:
            pass
        items.append(i)
    try:
        http_post_raw(session, queueman_enqueue_url, json=items)
        cache.remove(cache_slug, errors)
    except:
        logger.exception('Error while enqueueing items')


def resolve_deviant(deviant, resolve_cache=None):
    if resolve_cache is None:
        resolve_cache = DeviantResolveCache(manager.get_cache())
    logger.info(f"Attempting to resolve {deviant}")
    try:
        cached_result = resolve_cache.query(deviant)
        if cached_result:
            return cached_result
    except DagrException:
        logger.warning(f"Deviant {deviant} is listed as deactivated")
        raise
    try:
        deviant, _group = manager.get_dagr().resolve_deviant(deviant)
        resolve_cache.add(deviant)
        return deviant
    except DagrException:
        if is_deactivated(deviant):
            logger.warning(f"Deviant {deviant} is deactivated")
            resolve_cache.add(deviant, deactivated=True)
            logger.log(level=15, msg=f"Added {deviant} to deactivated list")
            raise
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
            sort_queue_galleries(pages)
        else:
            logger.info('Watchlist crawl found no new pages')
    except InvalidSessionIdException:
        raise
    except:
        logger.exception('Error while crawling watch list')
    return pages


def sort_queue_galleries(pages, resort=False, flush=True):
    deviants_sorted = sort_pages(pages, resort=resort, flush=flush)
    update_bulk_galleries(deviants_sorted)
    queue_galleries(deviants_sorted, priority=50, resolved=True)


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
    while manager.session_ok and not check_stop_file('STOP_MON_WATCHLIST'):
        crawlst = time()
        try:
            monitor_watchlist_action()
        except InvalidSessionIdException:
            manager.session_bad()
        delay_needed = monitor_sleep - (time() - crawlst)
        if delay_needed > 0:
            logger.log(
                level=15, msg=f"Need to sleep for {'{:.2f}'.format(delay_needed)} seconds")
            sleep(delay_needed)
        logger.log(
            level=15, msg=f"Rip watchlist took {'{:.4f}'.format(time() - crawlst)} seconds")


def monitor_trash(full_crawl=False, resort=False):
    while manager.session_ok and not check_stop_file('STOP_MON_TRASH'):
        crawlst = time()
        try:
            with manager.get_dagr():
                rip_trash(full_crawl=full_crawl, resort=resort)
        except (InvalidSessionIdException, WebDriverException):
            manager.session_bad()
        delay_needed = rip_trash_sleep - (time() - crawlst)
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


def is_deactivated(deviant):
    browser = manager.get_browser()
    if not deviant.lower() in browser.current_url.lower():
        browser.open(f"https://deviantart.com/{deviant}")
    try:
        headline = browser.find_element_by_css_selector('h1.headline')
        return headline.text == 'Deactivated Account' or headline.text == 'Forbidden'
    except NoSuchElementException:
        return False
