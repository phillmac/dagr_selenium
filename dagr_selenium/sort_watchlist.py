from os import environ

from .functions import config, manager, sort_watchlist


resort = (environ['RESORT'] if environ.get('RESORT') else input('Resort?: ')).lower().startswith('y')

env_level = environ.get('dagr.sort_watchlist.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None


manager.set_mode('sort_watchlist')
manager.init_logging(level_mapped)

with manager.get_dagr():
    sort_watchlist(resort=resort)