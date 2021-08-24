import asyncio
from os import environ

from dagr_revamped.DAGRManager import DAGRManager
from dagr_revamped.utils_cli import DAGRUtils

from dagr_selenium.BulkCache import BulkCache


async def __main__():
    manager = DAGRManager()
    config = manager.get_config()


    env_level = environ.get('dagr.convert_bulk_items.logging.level', None)
    level_mapped = config.map_log_level(
        int(env_level)) if not env_level is None else None

    manager.set_mode('convert_bulk_items')
    manager.init_logging(level_mapped)

    bulk_cache = BulkCache(manager.get_cache())
    bulk_cache_items = []
    utils = DAGRUtils(config=config, filenames=['.dagr_bulk.json'])
    utils.walk_queue(lambda mode, deviant=None, mval=None: bulk_cache_items.append(BulkCache.create_item(mode=mode,deviant=deviant,mval=mval)))
    await bulk_cache.add(bulk_cache_items)
    await bulk_cache.flush()


if __name__ == '__main__':
    asyncio.run(__main__())
