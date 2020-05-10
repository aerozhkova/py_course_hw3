import asyncio
import aiohttp
import aiofiles
import datetime as dt
from transpose import transpose
import argparse
import logging

LOG_MESSAGE = '[%(asctime)s] %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_MESSAGE)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('-url', action="store", dest="loc_url")
args = parser.parse_args()


async def fetch():
    async with aiohttp.request('GET', 'http://142.93.138.114/images/') as resp:
        assert resp.status == 200
        return await resp.text()


async def get_names():
    async with aiohttp.ClientSession():
        img_list = await fetch()
        imgs = img_list.split('\n')
        return imgs


async def download(queue):
    imgs = await get_names()
    start = dt.datetime.now()
    async with aiohttp.ClientSession() as session:
        for img in imgs:
            async with session.get('http://142.93.138.114/images/' + img) as resp:
                logger.info(f'downloading {img}')
                async with aiofiles.open(args.loc_url + img, 'wb') as f:
                    await f.write(await resp.read())
                    item = img
                    await queue.put(item)
    end = dt.datetime.now()
    logger.info(f"Downloading took {end - start}s")


async def upload(queue):
    while True:
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            item = await queue.get()
            transposed = transpose(item, args.loc_url)
            logger.info(f'... posting {transposed.filename}')
            file = transposed.tobytes(encoder_name='raw')
            async with session.post('http://142.93.138.114/images/',
                                    data=file, raise_for_status=False) as resp:
                logger.info(f'Status {resp.status}')
        queue.task_done()


async def run():
    queue = asyncio.Queue()
    start = dt.datetime.now()
    uploader = asyncio.ensure_future(upload(queue))
    await download(queue)
    await queue.join()
    uploader.cancel()
    end = dt.datetime.now()
    logger.info(f'The whole process took {end - start}s')


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(run())
