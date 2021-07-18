import asyncio

class SleepMgr():
    def __init__(self, app):
        self.__sleep = None
        self.__shutdown = app['shutdown']

    async def sleep(self):
        self.__sleep = asyncio.create_task(asyncio.sleep(3600))
        try:
            await self.__sleep
        except asyncio.CancelledError:
            print('CancelledError')
            if not self.__shutdown.is_set():
                print('Set shutdown')
                self.__shutdown.set()
        except Exception as ex:
            print(ex)

    def cancel_sleep(self):
        if self.__sleep:
            self.__sleep.cancel()
