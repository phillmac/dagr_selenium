if __name__ == '__main__':
    from .FilesysAPI.server import run_app
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print(KeyboardInterrupt)