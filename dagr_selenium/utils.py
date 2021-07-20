from dagr_selenium.DeviantResolveCache import DeviantResolveCache
import logging
from os import environ
from pathlib import Path
from pprint import pformat

from dagr_revamped.exceptions import DagrException
from dagr_revamped.utils import (artist_from_url, get_html_name, get_remote_io,
                                 http_post_raw)
from selenium.common.exceptions import NoSuchElementException

logger = logging.getLogger(__name__)

def check_stop_file(manager, fname=None):
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


def is_deactivated(deviant, manager):
    with manager.get_browser().get_r_context() as browser:
        if not deviant.lower() in browser.current_url.lower():
            browser.open(f"https://deviantart.com/{deviant}")
        try:
            headline = browser.find_element_by_css_selector('h1.headline')
            return headline.text == 'Deactivated Account' or headline.text == 'Forbidden'
        except NoSuchElementException:
            return False

def resolve_deviant(deviant, manager, resolve_cache=None):
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
    with manager.get_browser().get_r_context():
        try:
                deviant, _group = manager.get_dagr().resolve_deviant(deviant)
                resolve_cache.add(deviant)
                return deviant
        except DagrException:
            if is_deactivated(deviant, manager):
                logger.warning(f"Deviant {deviant} is deactivated")
                resolve_cache.add(deviant, deactivated=True)
                logger.log(level=15, msg=f"Added {deviant} to deactivated list")
                raise
            logger.warning(f"Unable to resolve deviant {deviant}")
            raise




def flush_errors_to_queue(manager, session, endpoint):
    cache = manager.get_cache()
    cache_slug = 'error_items'
    errors = cache.query(cache_slug)
    items = []
    for e in errors:
        i = dict(e)
        try:
            if (not 'resolved' in i) or (not i['resolved']):
                i['deviant'] = resolve_deviant(manager, i['deviant'])
                i['resolved'] = True
        except:
            pass
        items.append(i)
    try:
        http_post_raw(session, endpoint, json=items)
        cache.remove(cache_slug, errors)
    except:
        logger.exception('Error while enqueueing items')
