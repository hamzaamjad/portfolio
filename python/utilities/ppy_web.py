#!/usr/bin/env python3
"""Placeholder
"""

# -- Imports --------------------------------------------------------------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.auth import HTTPBasicAuth

# -- Requests -------------------------------------------------------------------------------

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = 5
        if "timeout" in kwargs:
            self.timeout = kwargs['timeout']
            del(kwargs['timeout'])
        super().__init__(*args, **kwargs)
    
    def send(self, request, **kwargs):
        timeout = kwargs.get('timeout')
        if timeout is None:
            kwargs['timeout'] = self.timeout
        return(super().send(request, **kwargs))

def create_session(timeout:int = 5, retry:int = 10):
    r = requests.Session()
    
    max_retries = Retry(
            total = retry,
            backoff_factor = 0.1,
            method_whitelist = False,
            status_forcelist = frozenset([403, 413, 429, 500, 502, 503, 504]),
            respect_retry_after_header=False
            )

    adapter = TimeoutHTTPAdapter(timeout = timeout, max_retries = max_retries)
    r.mount('https://', adapter)
    r.mount('http://', adapter)
    
    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    r.hooks['response'] = [assert_status_hook]
    return(r)