import logging

from .functions import config, manager, rip_gallery

from os import environ

deviant = (environ['USERNAME'] if environ.get('USERNAME', None)
              else input('Enter username: '))

full_crawl = (environ['FULL_CRAWL'] if environ.get('FULL_CRAWL', None)
              else input('Full crawl?: ')).lower().startswith('y')

disable_resolve = environ.get('DISABLE_RESOLVE', '').lower().startswith('y')

env_level = environ.get('dagr.rip_gallery.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('rip_gallery')
manager.init_logging(level_mapped)

logging.getLogger(__name__).info(f"Full crawl: {full_crawl}")

with manager.get_dagr():
    manager.get_browser().do_login()
    rip_gallery(deviant, full_crawl, disable_resolve)
logging.shutdown()
