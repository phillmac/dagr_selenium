import logging
from os import environ

from .functions import config, manager, rip_trash

full_crawl = environ.get('FULL_CRAWL', input('Full crawl?: ')).lower().startswith('y')
resort = environ.get('RESORT', input('Resort?: ')).lower().startswith('y')

env_level = environ.get('dagr.riptrash.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('rip_trash')
manager.init_logging(level_mapped)

with manager.get_dagr():
    rip_trash(full_crawl=full_crawl, resort=resort)
logging.shutdown()
