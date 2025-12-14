
"""
Project: Oslovovac
File: src/adapters.py
Description: Adaptive components for dynamically adjusting delays, worker counts, and batch sizes based on performance.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
import time
import logging
import random
import asyncio
from typing import List
from .config import HTTP_CONFIG

# --- CORE ADAPTERS ---
class AdaptiveDelay:
    def __init__(self, initial_delay: float, max_delay: float, min_delay: float):
        self.current_delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.adjustment_interval = HTTP_CONFIG.get('WORKER_SCALE_INTERVAL', 1.0) # Use config value, longer interval
        self.last_adjustment = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_success(self, success_rate: float):
        if time.time() - self.last_adjustment >= self.adjustment_interval:
            old_delay = self.current_delay
            if success_rate >= 0.95: self.current_delay = max(self.min_delay, self.current_delay * 0.85)
            elif success_rate >= 0.9: self.current_delay = max(self.min_delay, self.current_delay * 0.9)
            if old_delay != self.current_delay:
                 self.logger.debug(f"Delay decreased: {old_delay:.3f}s -> {self.current_delay:.3f}s (success: {success_rate:.1%})")
            self.last_adjustment = time.time()

    def on_error(self, success_rate: float):
        if time.time() - self.last_adjustment >= self.adjustment_interval:
            old_delay = self.current_delay
            if success_rate < 0.8: self.current_delay = min(self.max_delay, self.current_delay * 1.8)
            elif success_rate < 0.9: self.current_delay = min(self.max_delay, self.current_delay * 1.4)
            if old_delay != self.current_delay:
                self.logger.info(f"Delay increased: {old_delay:.3f}s -> {self.current_delay:.3f}s (success: {success_rate:.1%})")
            self.last_adjustment = time.time()

    async def wait(self):
        await asyncio.sleep(self.current_delay)

class AdaptiveWorkers:
    def __init__(self, initial_workers: int, max_workers: int, min_workers: int):
        self.current_workers = initial_workers
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.success_count = 0 # Track successes/failures per interval
        self.error_count = 0
        self.adjustment_interval = HTTP_CONFIG.get('WORKER_SCALE_INTERVAL', 3.0)
        self.last_adjustment = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)

    def record_success(self): self.success_count += 1
    def record_error(self): self.error_count += 1

    def adjust(self):
        if time.time() - self.last_adjustment >= self.adjustment_interval:
            old_workers = self.current_workers
            total_ops = self.success_count + self.error_count
            if total_ops == 0: return # No activity

            success_rate = self.success_count / total_ops
            
            if success_rate > 0.9 and self.current_workers < self.max_workers : # High success rate
                self.current_workers = min(self.max_workers, self.current_workers + max(1, int(self.current_workers * 0.1))) # Increase by 10% or at least 1
            elif success_rate < 0.8 and self.current_workers > self.min_workers: # Low success rate
                self.current_workers = max(self.min_workers, self.current_workers - max(1, int(self.current_workers * 0.2))) # Decrease by 20% or at least 1
            
            if old_workers != self.current_workers:
                 self.logger.info(f"Workers adjusted: {old_workers} -> {self.current_workers} (S/E: {self.success_count}/{self.error_count}, Rate: {success_rate:.1%})")
            
            self.success_count = 0
            self.error_count = 0
            self.last_adjustment = time.time()

class AdaptiveBatchSize: # Similar to AdaptiveWorkers
    def __init__(self, initial_size: int, max_size: int, min_size: int):
        self.current_size = initial_size
        self.max_size = max_size
        self.min_size = min_size
        self.success_count = 0
        self.error_count = 0
        self.adjustment_interval = HTTP_CONFIG.get('WORKER_SCALE_INTERVAL', 3.0)
        self.last_adjustment = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)

    def record_success(self): self.success_count += 1
    def record_error(self): self.error_count += 1
    
    def adjust(self):
        if time.time() - self.last_adjustment >= self.adjustment_interval:
            old_size = self.current_size
            total_ops = self.success_count + self.error_count
            if total_ops == 0: return

            success_rate = self.success_count / total_ops

            if success_rate > 0.95 and self.current_size < self.max_size:
                self.current_size = min(self.max_size, int(self.current_size * 1.1))
            elif success_rate < 0.75 and self.current_size > self.min_size:
                 self.current_size = max(self.min_size, int(self.current_size * 0.8))
            
            if old_size != self.current_size:
                self.logger.info(f"Batch size adjusted: {old_size} -> {self.current_size} (S/E: {self.success_count}/{self.error_count}, Rate: {success_rate:.1%})")

            self.success_count = 0
            self.error_count = 0
            self.last_adjustment = time.time()


class UserAgentManager:
    def __init__(self, user_agents: List[str]):
        self.user_agents = user_agents if user_agents else ['DefaultPythonUserAgent/1.0']
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_next_user_agent(self) -> str:
        ua = random.choice(self.user_agents)
        self.logger.debug(f"Using User-Agent: {ua}")
        return ua
