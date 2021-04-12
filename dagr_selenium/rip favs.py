import logging

from functions import manager, rip_favs

deviant = input('Enter username: ')
full_crawl = input('Full crawl?: ').lower().startswith('y')

manager.set_mode('rip_favs')
mamanger.init_logging()


logging.getLogger(__name__).info(f"Full crawl: {full_crawl}")

with manager.get_dagr():
    manager.get_browser().do_login()
    rip_favs(deviant, full_crawl)
logging.shutdown()
