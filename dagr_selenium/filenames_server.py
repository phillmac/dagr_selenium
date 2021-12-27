if __name__ == '__main__':
    import asyncio
    from dagr_selenium.FilesysAPI.server import run_app

    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print(KeyboardInterrupt)