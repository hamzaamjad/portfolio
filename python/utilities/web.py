#!/usr/bin/env python
# -*- coding: UTF-8 -*-
""" Various Web Utilities.
"""

# Imports
# -- Standard library imports
import random
import logging
from urllib3.util import Retry
import asyncio
from io import BytesIO
import os

# -- Related third party imports
import requests
import aiohttp
import aiofiles

# -- Local application/library specific imports


# Code

# -- Requests --------------------------------------------------------------------

# TODO
# Add useragent to session creation, provide options for useragent selection to user
def createSession(timeout: int = 5, total_retries:int = 5, proxies = {}, *args: list, **kwargs: dict) -> requests.sessions.Session:
    session = requests.Session()

    session.proxies.update(proxies)

    # Use urllib3's Retry() function to override Retry behavior in Requests
    # Provides more flexibility if we need to adjust Retry behavior
    max_retries = Retry(
            total = total_retries,
            backoff_factor = 0.1,
            status_forcelist = frozenset({403, 413, 429, 500, 502, 503, 504}),
            respect_retry_after_header = False
            )

    class ModifiedHTTPAdapter(requests.adapters.HTTPAdapter):
        """ Wraps `requests.adapters.HTTPAdapter` for easier access to common defaults.

            Wrapper for `requests.adapters.HTTPAdapter` to provide easier
            access to common defaults such as timeout
        """

        def __init__(self, *args, **kwargs):
            # Provide an easy way to override the timeout on Requests
            # Default to 5 seconds, since we don't want hanging requests
            if "timeout" in kwargs:
                self.timeout = kwargs['timeout']
                # Remove 'timeout' from kwargs as requests.adapters.HTTPAdapter
                # does not take 'timeout' as an argument
                del(kwargs['timeout'])
            else:
                self.timeout = 5
            super().__init__(*args, **kwargs)
       
        def send(self, request, **kwargs):
           if kwargs.get('timeout') is None:
              kwargs['timeout'] = self.timeout
           return(super().send(request, **kwargs))

    session.mount('https://', ModifiedHTTPAdapter(timeout = timeout, max_retries = max_retries))
    session.mount('http://', ModifiedHTTPAdapter(timeout = timeout, max_retries = max_retries))

    def assertStatus(response, *args, **kwargs):
        """ Raises error if request is not successful [200]
        """
        return response.raise_for_status()
    
    # Attach the assertStatus hook, to check the response automatically
    session.hooks['response'].append(assertStatus)

    # Return the modified session object
    return session

# -- Async --------------------------------------------------------------------

class AsyncClient():
    def __init__(self, limit: int = 100, logname: str = 'async.log', proxy = None, verify_ssl: bool = True):
        self.conn = aiohttp.TCPConnector(limit = limit, limit_per_host = limit, verify_ssl = verify_ssl)
        self.semaphore = asyncio.BoundedSemaphore(value = limit)
        self.session = aiohttp.ClientSession(connector = self.conn, headers={"Connection": "close"})
        self.timeout = aiohttp.ClientTimeout(total = 75)
        self.proxy = proxy

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler(logname)
        self.fh.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
    
    async def get(self, url, response_format = 'BytesIO'):
        data = None
        while data is None:
            try:
                async with self.semaphore, self.session.get(url, timeout = self.timeout, proxy = self.proxy) as response:
                    try:
                        assert response.status == 200
                        data = 1
                        if response_format == 'BytesIO':
                            output = await response.read()
                            output = BytesIO(output)
                            return(output)
                        elif response_format == 'text':
                            output = await response.text()
                            return(output)
                        elif response_format == 'chunk':
                            output = await response
                            return(output)
                    except Exception as e:
                        if response.status == 500 or response.status == 429:
                            await asyncio.sleep(1)
                        if response.status == 404:
                            await asyncio.sleep(1)
                            data = 1
                        if response.status == 200:
                            print(e)
                        self.logger.error(f'get ({response.status}): {url}')
                await asyncio.sleep(0)
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ServerDisconnectedError):
                generator = random.Random()
                await asyncio.sleep(round(generator.uniform(0.1,4),2))
                self.logger.error(f"get (XXX): {url}")

    async def extract_data(self, url: str, parsefunction: object, response_format: str = 'BytesIO', sleep = 1):
        response = await self.get(url, response_format)
        if response:
            data = parsefunction(response)
            await asyncio.sleep(sleep)
            return data
        else:
            self.logger.error(f"extract_data: {url}")
            await asyncio.sleep(sleep)
    
    async def download_file(self, url: str, output_dir: str, output_file_type: str = '.html', filename_func = None):
        data = None
        while data is None:
            try:
                async with self.semaphore, self.session.get(url, timeout = self.timeout, proxy = self.proxy) as response:
                    if filename_func is None:
                        filename = f'{os.path.basename(url)}{output_file_type}'
                    else:
                        if isinstance(filename_func, str):
                            print(filename_func)
                            filename = filename_func
                        else:
                            filename = filename_func(url)
                    output = os.path.join(output_dir, filename)
                    try:
                        assert response.status == 200
                        data = 1
                        async with aiofiles.open(output, 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                await f.write(chunk)
                        return await response.release()
                    except Exception as e:
                        if response.status == 500 or response.status == 429:
                            await asyncio.sleep(1)
                        if response.status == 404:
                            await asyncio.sleep(1)
                            data = 1
                        if response.status == 200:
                            print(e)
                        self.logger.error(f'download_file ({response.status}): {url}')
                    await asyncio.sleep(0)
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ServerDisconnectedError):
                generator = random.Random()
                await asyncio.sleep(round(generator.uniform(0.1,4),2))
                self.logger.error(f"download_file: {url}")