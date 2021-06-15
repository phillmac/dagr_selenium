import logging

from .functions import config, manager, rip

from os import environ

deviant = (environ['USERNAME'] if environ.get('USERNAME', None)
              else input('Enter username: '))
full_crawl = (environ['FULL_CRAWL'] if environ.get('FULL_CRAWL', None)
              else input('Full crawl?: ')).lower().startswith('y')

env_level = environ.get('dagr.rip_scraps.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('rip_scraps_html')
manager.init_logging(level_mapped)

logging.getLogger(__name__).info(f"Full crawl: {full_crawl}")

with manager.get_dagr():
    manager.get_browser().do_login()
    rip('scraps', deviant, full_crawl=full_crawl, load_more=True, disable_filter=True,dump_html=True)
logging.shutdown()
