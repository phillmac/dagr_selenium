import logging
from os import environ, truncate

import dagr_revamped.version as dagr_revamped_version

from .functions import config, manager, sort_watchlist

print('Dagr version:', dagr_revamped_version)

resort = (environ['RESORT'] if environ.get('RESORT')
          else input('Resort?: ')).lower().startswith('y')

env_level = environ.get('dagr.sort_watchlist.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None


manager.set_mode('sort_watchlist')
manager.init_logging(level_mapped)

with manager.get_dagr():
    try:
        sort_watchlist(resort=resort)
    except:
        logging.getLogger(__name__).error(
            'Error while sorting watchlist', exc_info=True)
