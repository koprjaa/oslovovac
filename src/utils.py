
"""
Project: Vocative Generator
File: src/utils.py
Description: Utility functions for graceful shutdown handling and aiohttp session management.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
import signal
import asyncio
import logging
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
from contextlib import asynccontextmanager
from .config import HTTP_CONFIG

class GracefulShutdownHandler:
    def __init__(self):
        self.shutdown_requested = False
        self.shutdown_event = asyncio.Event()
        self.logger = logging.getLogger(self.__class__.__name__)

    def handle_signal(self, signum, frame):
        if not self.shutdown_requested:
            self.logger.warning(f"Received signal {signal.Signals(signum).name}. Requesting graceful shutdown...")
            self.shutdown_requested = True
            self.shutdown_event.set()
        else:
            self.logger.warning("Shutdown already in progress.")

@asynccontextmanager
async def create_aiohttp_session():
    # Increased connector limit if MAX_WORKERS is high
    # Limit per host is also important if all requests go to the same server
    connector = TCPConnector(
        ssl=False, # If server doesn't have valid SSL, otherwise True or omit
        limit=HTTP_CONFIG['MAX_WORKERS'] * 2, # More connections for flexibility
        limit_per_host=HTTP_CONFIG['MAX_WORKERS'] # Limit per host
    )
    # Timeout is already in NameService, this is global for session
    timeout_config = ClientTimeout(total=HTTP_CONFIG['TIMEOUT'] + 5) # Slightly higher than per-request timeout
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout_config,
        skip_auto_headers=['User-Agent'] # Manual User-Agent management
    ) as session:
        yield session
