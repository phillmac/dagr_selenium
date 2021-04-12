import logging

from functions import manager, rip_gallery

deviant = input('Enter username: ')
full_crawl = input('Full crawl?: ').lower().startswith('y')

manager.set_mode('rip_gallery')
manager.init_logging()

logging.getLogger(__name__).info(f"Full crawl: {full_crawl}")

with manager.get_dagr():
    manager.get_browser().do_login()
    rip_gallery(deviant, full_crawl)
logging.shutdown()
