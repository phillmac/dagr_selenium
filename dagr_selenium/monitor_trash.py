import logging
from os import environ

from .functions import config, manager, monitor_trash

full_crawl = (environ['FULL_CRAWL'] if environ.get('FULL_CRAWL', None) else input('Full crawl?: ')).lower().startswith('y')
resort = (environ['RESORT'] if environ.get('RESORT') else input('Resort?: ')).lower().startswith('y')

manager_mode = (environ.get('MANAGER_MODE', 'monitor_trash' + '_full' if full_crawl else ''))


env_level = environ.get('dagr.monitortrash.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None



manager.set_mode(manager_mode)
manager.init_logging(level_mapped)

with manager.get_dagr():
    monitor_trash(full_crawl=full_crawl, resort=resort)
logging.shutdown()
