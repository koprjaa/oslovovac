
"""
Project: Vocative Generator
File: src/services.py
Description: Core service layer implementing checkpoint management, API interaction, and batch processing logic.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
import logging
import json
import os
import random
import time
import asyncio
import gc  # Added gc import
import pandas as pd
import aiohttp
from pathlib import Path
from typing import Dict, Optional, List, Tuple, Set
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .config import FILE_CONFIG, API_CONFIG, HTTP_CONFIG
from .models import NameResult
from .adapters import AdaptiveDelay, AdaptiveWorkers, AdaptiveBatchSize, UserAgentManager

# --- SERVICES ---
class CheckpointService:
    def __init__(self):
        self.checkpoint_file = Path(FILE_CONFIG['CHECKPOINT_FILE'])
        self.processed_names: Dict[str, Dict[str, str]] = {}
        self.last_batch_completed_for_current_chunk = 0 # Track batches within current chunk
        self.last_chunk_fully_processed_index = -1 # Index of last fully processed chunk
        self.logger = logging.getLogger(self.__class__.__name__)
        self._load_checkpoint()

    def _load_checkpoint(self) -> None:
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_names = data.get('processed_names', {})
                    # For backward compatibility with old 'last_batch' format
                    self.last_batch_completed_for_current_chunk = data.get('last_batch_completed_for_current_chunk', data.get('last_batch', 0))
                    self.last_chunk_fully_processed_index = data.get('last_chunk_fully_processed_index', -1)
                self.logger.info(f"Checkpoint loaded: {len(self.processed_names)} processed names, "
                                 f"last_chunk_idx: {self.last_chunk_fully_processed_index}, "
                                 f"last_batch_in_chunk: {self.last_batch_completed_for_current_chunk}")
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}", exc_info=True)
            self.processed_names = {}
            self.last_batch_completed_for_current_chunk = 0
            self.last_chunk_fully_processed_index = -1
            if self.checkpoint_file.exists(): # If file is corrupted, attempt to delete
                try:
                    self.checkpoint_file.unlink(missing_ok=True)
                    self.logger.info("Corrupted checkpoint file removed.")
                except OSError as ose:
                    self.logger.error(f"Could not remove corrupted checkpoint file: {ose}")


    def save_checkpoint(self, current_chunk_index: int, batch_number_in_chunk: int, 
                        processed_batch_data: Dict[str, Dict[str, str]], 
                        is_chunk_complete: bool) -> None:
        try:
            # Update global dictionary of processed names
            for name_key, data_val in processed_batch_data.items():
                if name_key and data_val.get('vocative'): # Save only valid data
                    self.processed_names[name_key] = data_val
            
            data_to_save = {
                'last_chunk_fully_processed_index': self.last_chunk_fully_processed_index,
                'last_batch_completed_for_current_chunk': self.last_batch_completed_for_current_chunk,
                'processed_names': self.processed_names # Still saving all names for robustness
            }

            if is_chunk_complete:
                data_to_save['last_chunk_fully_processed_index'] = current_chunk_index
                data_to_save['last_batch_completed_for_current_chunk'] = 0 # Reset for next chunk
            else:
                # If chunk is not complete, save current chunk index for info,
                # but `last_chunk_fully_processed_index` does not change.
                # `last_batch_completed_for_current_chunk` updates to current batch.
                data_to_save['last_batch_completed_for_current_chunk'] = batch_number_in_chunk


            temp_checkpoint_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, separators=(',', ':'))  # No indentation for smaller file
            os.replace(temp_checkpoint_file, self.checkpoint_file) # Atomic move

            self.logger.info(f"Checkpoint saved. Chunk_idx: {current_chunk_index}, batch_in_chunk: {batch_number_in_chunk}, "
                             f"is_chunk_complete: {is_chunk_complete}. Total processed: {len(self.processed_names)}.")
            
            # Update instance state after successful save
            if is_chunk_complete:
                self.last_chunk_fully_processed_index = current_chunk_index
                self.last_batch_completed_for_current_chunk = 0
            else:
                self.last_batch_completed_for_current_chunk = batch_number_in_chunk
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}", exc_info=True)

    def get_resume_info(self, current_chunk_index: int) -> Tuple[int, Dict[str, Dict[str,str]]]:
        """Returns info to resume processing of current chunk."""
        if current_chunk_index <= self.last_chunk_fully_processed_index:
            # This chunk was already fully processed in the past
            return -1, {} # Signals that chunk can be skipped
        
        # If it's the same chunk we ended on last time (and wasn't completed)
        # or if we are starting a new chunk after the last fully completed one
        if current_chunk_index == self.last_chunk_fully_processed_index + 1:
            return self.last_batch_completed_for_current_chunk, self.processed_names
        
        # Starting a completely new chunk that is not immediately after the last completed one
        # (e.g., if checkpoint was deleted or chunks skipped)
        return 0, self.processed_names


    def is_name_globally_processed(self, name: str) -> bool:
        return name in self.processed_names

    def get_globally_processed_name_data(self, name: str) -> Optional[Dict[str,str]]:
        return self.processed_names.get(name)

    def clear_checkpoint_for_new_run(self) -> None: # Optional, if we want to start from scratch
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
            self.processed_names = {}
            self.last_batch_completed_for_current_chunk = 0
            self.last_chunk_fully_processed_index = -1
            self.logger.info("Checkpoint cleared for a new run.")
        except Exception as e:
            self.logger.error(f"Error clearing checkpoint: {e}", exc_info=True)


class NameService:
    def __init__(self, session: aiohttp.ClientSession, user_agent_manager: UserAgentManager):
        self.session = session
        self.user_agent_manager = user_agent_manager
        self.base_url = API_CONFIG['BASE_URL']
        self.endpoint = API_CONFIG['ENDPOINT']
        self.timeout_val = aiohttp.ClientTimeout(total=HTTP_CONFIG['TIMEOUT'])
        self.max_retries = HTTP_CONFIG['MAX_RETRIES']
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Per-request state
        self.current_request_cookies = {} # Cookies specific for one GET->POST cycle
        
        # Rate limiting and backoff state
        self.global_backoff_factor = 1.0
        self.consecutive_successes_since_last_error = 0
        self.last_backoff_factor_increase_time = 0.0
        self.last_error_time = 0.0
        self.total_requests = 0
        self.total_errors = 0

    def _get_headers(self) -> Dict[str, str]:
        base_headers = {
            'User-Agent': self.user_agent_manager.get_next_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'cs,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        return base_headers

    async def _handle_rate_limit_or_server_error_delay(self, error_type: str, attempt: int) -> None:
        """Handles rate limit or server error with adaptive backoff."""
        current_time = time.time()
        self.last_error_time = current_time
        self.consecutive_successes_since_last_error = 0
        self.total_errors += 1

        # Increase backoff factor based on error type
        if error_type == 'rate_limit':
            multiplier = HTTP_CONFIG['RATE_LIMIT_BACKOFF_MULTIPLIER']
        elif error_type == 'server_error':
            multiplier = HTTP_CONFIG['SERVER_ERROR_BACKOFF_MULTIPLIER']
        else:  # connection_error
            multiplier = HTTP_CONFIG['CONNECTION_ERROR_BACKOFF_MULTIPLIER']

        old_factor = self.global_backoff_factor
        self.global_backoff_factor = min(
            self.global_backoff_factor * multiplier,
            HTTP_CONFIG['RATE_LIMIT_MAX_GLOBAL_BACKOFF_FACTOR']
        )
        
        if self.global_backoff_factor > old_factor:
            self.last_backoff_factor_increase_time = current_time
            self.logger.warning(
                f"Backoff factor increased: {old_factor:.1f} -> {self.global_backoff_factor:.1f} "
                f"({error_type}, attempt {attempt})"
            )

        # Calculate delay with jitter
        base_delay = HTTP_CONFIG['RATE_LIMIT_INITIAL_BACKOFF_SECONDS'] * self.global_backoff_factor
        jitter = random.uniform(0, 0.5 * base_delay)
        delay = base_delay + jitter

        self.logger.info(
            f"Rate limit/server error handling: waiting {delay:.2f}s "
            f"(backoff factor: {self.global_backoff_factor:.1f}, "
            f"error type: {error_type}, attempt: {attempt})"
        )
        await asyncio.sleep(delay)

    def _try_reduce_backoff_factor_on_success(self) -> None:
        """Reduces backoff factor on successful operations if conditions are met."""
        current_time = time.time()
        self.consecutive_successes_since_last_error += 1
        self.total_requests += 1

        # Check cooldown period
        if (current_time - self.last_backoff_factor_increase_time) < HTTP_CONFIG['COOLDOWN_AFTER_FACTOR_INCREASE_S']:
            return

        # Check number of consecutive successes
        if self.consecutive_successes_since_last_error >= HTTP_CONFIG['SUCCESSES_TO_REDUCE_BACKOFF']:
            old_factor = self.global_backoff_factor
            self.global_backoff_factor = max(
                1.0,
                self.global_backoff_factor * HTTP_CONFIG['BACKOFF_REDUCTION_ON_SUCCESS_RATIO']
            )
            
            if self.global_backoff_factor < old_factor:
                self.logger.info(
                    f"Backoff factor reduced: {old_factor:.1f} -> {self.global_backoff_factor:.1f} "
                    f"after {self.consecutive_successes_since_last_error} consecutive successes"
                )

    async def _get_form_and_cookies(self, url: str, headers: Dict[str, str], attempt: int) -> bool:
        """Gets CSRF tokens/cookies from form (GET). Returns True on success."""
        try:
            async with self.session.get(url, headers=headers) as response_get: # timeout is handled by session
                self.logger.debug(f"GET {url} status: {response_get.status} (Attempt: {attempt})")
                
                if response_get.status == 429:
                    await self._handle_rate_limit_or_server_error_delay('rate_limit', attempt)
                    return False
                
                if response_get.status >= 500:
                    await self._handle_rate_limit_or_server_error_delay('server_error', attempt)
                    return False

                response_get.raise_for_status()
                self.current_request_cookies.update(response_get.cookies)
                return True

        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout on GET {url} (Attempt: {attempt}).")
            await self._handle_rate_limit_or_server_error_delay('connection_error', attempt)
            return False
            
        except aiohttp.ClientResponseError as e:
            if e.status not in [429, 500, 502, 503, 504]:
                raise  # Raise other errors to outer handler
            return False
            
        except aiohttp.ClientError as e:
            self.logger.warning(f"ClientError on GET {url}: {e} (Attempt: {attempt}).")
            await self._handle_rate_limit_or_server_error_delay('connection_error', attempt)
            return False

    async def _submit_form_and_get_vocative(self, url: str, headers: Dict[str, str], name: str, attempt: int) -> Optional[str]:
        """Submits form (POST) and extracts vocative. Returns vocative or None on error requiring retry."""
        post_headers = headers.copy()
        post_headers.update({
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': self.base_url,
            'Referer': url,
            'Sec-Fetch-Site': 'same-origin',
        })
        if self.current_request_cookies:
            post_headers['Cookie'] = '; '.join([f"{k}={v.value}" for k, v in self.current_request_cookies.items()])

        data = {'inpJmena': name}
        try:
            async with self.session.post(url, data=data, headers=post_headers, cookies=self.current_request_cookies) as response_post:
                self.logger.debug(f"POST {url} for '{name}' status: {response_post.status} (Attempt: {attempt})")
                
                if response_post.status == 429:
                    await self._handle_rate_limit_or_server_error_delay('rate_limit', attempt)
                    return None
                
                if response_post.status >= 500:
                    await self._handle_rate_limit_or_server_error_delay('server_error', attempt)
                    return None

                response_post.raise_for_status()
                self.current_request_cookies.update(response_post.cookies)
                html = await response_post.text()
                vocative = self._extract_vocative(html, name)
                self._try_reduce_backoff_factor_on_success()
                return vocative

        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout on POST for '{name}' (Attempt: {attempt}).")
            await self._handle_rate_limit_or_server_error_delay('connection_error', attempt)
            return None
            
        except aiohttp.ClientResponseError as e:
            if e.status not in [429, 500, 502, 503, 504]:
                raise
            return None
            
        except aiohttp.ClientError as e:
            self.logger.warning(f"ClientError on POST for '{name}': {e} (Attempt: {attempt}).")
            await self._handle_rate_limit_or_server_error_delay('connection_error', attempt)
            return None

    def _extract_vocative(self, html: str, original_name: str) -> str:
        """Extracts vocative from HTML response with improved logging."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', class_='table table-hover table-striped table-bordered')
            if not table:
                table = soup.find('table', class_='table')
            
            if not table:
                self.logger.warning(
                    f"No table found for '{original_name}'. HTML preview (1000 chars):\n{html[:1000]}"
                )
                return ""
            
            rows = table.find_all('tr')
            if len(rows) <= 1:
                self.logger.warning(
                    f"No data rows found for '{original_name}'. Table HTML:\n{table.prettify()}"
                )
                return ""
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    input_name = cells[0].text.strip()
                    vocative_candidate = cells[1].text.strip()
                    
                    self.logger.debug(
                        f"Extracted for '{original_name}': "
                        f"input='{input_name}', vocative='{vocative_candidate}'"
                    )
                    
                    # Check if vocative differs from original
                    if vocative_candidate.lower() != original_name.lower():
                        return vocative_candidate
                    else:
                        self.logger.info(
                            f"Vocative same as original for '{original_name}': '{vocative_candidate}'"
                        )
                        return vocative_candidate
            
            self.logger.warning(
                f"No matching row found for '{original_name}' in results table. "
                f"HTML preview (1000 chars):\n{html[:1000]}"
            )
            return ""
            
        except Exception as e:
            self.logger.error(
                f"Error extracting vocative for '{original_name}': {e}\n"
                f"HTML preview (1000 chars):\n{html[:1000]}",
                exc_info=True
            )
            return ""

    async def process_single_name(self, name: str) -> NameResult:
        """Processes a single name with enhanced error handling and retry logic."""
        url = urljoin(self.base_url, self.endpoint)
        
        for attempt in range(1, self.max_retries + 2):
            self.current_request_cookies = {}
            base_headers = self._get_headers()

            try:
                if not await self._get_form_and_cookies(url, base_headers, attempt):
                    if attempt <= self.max_retries:
                        continue
                    return NameResult.error(name, f"Failed GET after {attempt-1} retries")

                vocative_result = await self._submit_form_and_get_vocative(url, base_headers, name, attempt)
                if vocative_result is None:
                    if attempt <= self.max_retries:
                        continue
                    return NameResult.error(name, f"Failed POST after {attempt-1} retries")
                
                self.logger.info(
                    f"Processed '{name}'. Vocative: '{vocative_result if vocative_result else '(empty)'}'. "
                    f"Success rate: {((self.total_requests - self.total_errors) / max(1, self.total_requests)) * 100:.1f}%"
                )
                return NameResult.from_vocative(name, vocative_result if vocative_result else name)

            except aiohttp.ClientResponseError as e:
                if e.status in [401, 403]:  # Non-recoverable errors
                    self.logger.error(f"Non-recoverable HTTP error for '{name}': {e.status} {e.message}")
                    return NameResult.error(name, f"HTTP {e.status}: {e.message}")
                if attempt <= self.max_retries:
                    continue
                return NameResult.error(name, f"HTTP {e.status}: {e.message}")
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing '{name}': {e}", exc_info=True)
                return NameResult.error(name, f"Unexpected error: {str(e)}")
        
        return NameResult.error(name, f"Failed to process after {self.max_retries + 1} attempts")


class BatchService:
    def __init__(self, name_service: NameService, checkpoint_service: CheckpointService,
                 delay_adapter: AdaptiveDelay, worker_adapter: AdaptiveWorkers, batch_size_adapter: AdaptiveBatchSize,
                 shutdown_event: asyncio.Event):
        self.name_service = name_service
        self.checkpoint_service = checkpoint_service
        self.delay_adapter = delay_adapter
        self.worker_adapter = worker_adapter
        self.batch_size_adapter = batch_size_adapter
        self.shutdown_event = shutdown_event
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Statistics for adaptive mechanisms
        self.current_batch_successes = 0
        self.current_batch_errors = 0
        self.last_adaptation_time = time.time()
        
        # Checkpoint interval tracking
        self.checkpoint_interval = HTTP_CONFIG.get('CHECKPOINT_INTERVAL', 5)
        self.batches_since_last_checkpoint = 0
        self.pending_checkpoint_data = {}  # Accumulate data between checkpoints

    def _update_adaptive_mechanisms(self, batch_results: List[NameResult]) -> None:
        """Updates adaptive mechanisms based on batch results."""
        
        # Reset counters for new batch
        self.current_batch_successes = 0
        self.current_batch_errors = 0
        
        # Count successes and errors
        for result in batch_results:
            if result.success:
                self.current_batch_successes += 1
            else:
                self.current_batch_errors += 1
        
        total_requests = self.current_batch_successes + self.current_batch_errors
        if total_requests > 0:
            success_rate = self.current_batch_successes / total_requests
            
            # Update delay adapter
            if success_rate >= 0.95:
                self.delay_adapter.on_success(success_rate)
            elif success_rate < 0.8:
                self.delay_adapter.on_error(success_rate)
            
            # Update worker adapter
            if success_rate >= 0.9:
                self.worker_adapter.record_success()
            else:
                self.worker_adapter.record_error()
            
            # Update batch size adapter
            if success_rate >= 0.95:
                self.batch_size_adapter.record_success()
            else:
                self.batch_size_adapter.record_error()
            
            # Log statistics
            self.logger.info(
                f"Batch statistics: {self.current_batch_successes}/{total_requests} successful "
                f"({success_rate*100:.1f}%). "
                f"Current delay: {self.delay_adapter.current_delay:.2f}s, "
                f"workers: {self.worker_adapter.current_workers}, "
                f"batch size: {self.batch_size_adapter.current_size}"
            )

    async def process_chunk_data(self, df_chunk: pd.DataFrame, chunk_index: int) -> None:
        start_time_chunk = time.time()
        self.logger.info(f"Starting processing for chunk {chunk_index} with {len(df_chunk)} names.")

        # Save DataFrame and map for use in _process_single_batch
        # Save DataFrame and map for use in _process_single_batch
        self.df_chunk = df_chunk
        self.name_to_idx_map = {name: i for i, name in df_chunk[FILE_CONFIG['INPUT_COLUMN_NAME']].items()}

        last_completed_batch_in_chunk, globally_processed_names = self.checkpoint_service.get_resume_info(chunk_index)
        if last_completed_batch_in_chunk == -1: # Chunk was already fully processed
            self.logger.info(f"Chunk {chunk_index} already fully processed. Skipping.")
            return

        all_names_in_chunk = df_chunk[FILE_CONFIG['INPUT_COLUMN_NAME']].tolist()
        
        # Split into batches according to adaptive size
        initial_batch_size = self.batch_size_adapter.current_size 
        
        # Batch generator
        def chunk_list(data: list, size: int):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        batches_of_names = list(chunk_list(all_names_in_chunk, initial_batch_size))
        total_batches_in_chunk = len(batches_of_names)
        self.logger.info(f"Chunk {chunk_index} split into {total_batches_in_chunk} batches of (up to) {initial_batch_size} names.")

        # Reset checkpoint tracking for new chunk
        self.batches_since_last_checkpoint = 0
        self.pending_checkpoint_data = {}
        
        tasks: Set[asyncio.Task] = set()
        processed_batches_count_in_chunk = 0

        for i, current_batch_names_list in enumerate(batches_of_names):
            batch_number_for_display = i + 1 # 1-indexed
            if self.shutdown_event.is_set():
                self.logger.info("Shutdown requested, stopping batch processing for current chunk.")
                break

            if batch_number_for_display <= last_completed_batch_in_chunk:
                self.logger.info(f"Skipping batch {batch_number_for_display}/{total_batches_in_chunk} in chunk {chunk_index} (already processed).")
                processed_batches_count_in_chunk += 1
                continue
            
            # Pre-process names that are already in checkpoint
            batch_results_from_checkpoint: List[NameResult] = []
            names_needing_api_call: List[str] = []

            for name_in_batch in current_batch_names_list:
                processed_data = self.checkpoint_service.get_globally_processed_name_data(name_in_batch)
                if processed_data:
                    voc = processed_data.get('vocative', name_in_batch)
                    batch_results_from_checkpoint.append(NameResult.from_vocative(name_in_batch, voc))
                else:
                    names_needing_api_call.append(name_in_batch)
            
            if not names_needing_api_call:
                self.logger.info(f"Batch {batch_number_for_display} (chunk {chunk_index}) fully processed from checkpoint.")
                self._update_dataframe_with_results(df_chunk, batch_results_from_checkpoint, self.name_to_idx_map)
                # Prepare checkpoint data with ID from DataFrame
                checkpoint_data = {}
                for r in batch_results_from_checkpoint:
                    if r.original_name in self.name_to_idx_map:
                        idx = self.name_to_idx_map[r.original_name]
                        name_id = str(self.df_chunk.loc[idx, 'ID'])
                        checkpoint_data[r.original_name] = {'vocative': r.vocative, 'id': name_id}
                
                # Accumulate checkpoint data even for checkpoint-only batches
                self.pending_checkpoint_data.update(checkpoint_data)
                self.batches_since_last_checkpoint += 1
                
                # Save checkpoint every N batches or at last batch
                should_save_checkpoint = (
                    self.batches_since_last_checkpoint >= self.checkpoint_interval or
                    batch_number_for_display == total_batches_in_chunk
                )
                
                if should_save_checkpoint and self.pending_checkpoint_data:
                    self.checkpoint_service.save_checkpoint(
                        chunk_index, batch_number_for_display, 
                        self.pending_checkpoint_data,
                        is_chunk_complete=False
                    )
                    self.pending_checkpoint_data = {}
                    self.batches_since_last_checkpoint = 0
                
                processed_batches_count_in_chunk += 1
                self._log_batch_progress(processed_batches_count_in_chunk, total_batches_in_chunk, chunk_index, start_time_chunk)
                continue

            self.logger.info(f"Preparing batch {batch_number_for_display}/{total_batches_in_chunk} (chunk {chunk_index}) "
                             f"with {len(names_needing_api_call)} new names (total {len(current_batch_names_list)}).")
            
            task = asyncio.create_task(self._process_single_batch(
                names_to_process=names_needing_api_call,
                batch_number=batch_number_for_display,
                chunk_idx=chunk_index
            ))
            tasks.add(task)

            # Limit number of running tasks by adaptive worker count
            shutdown_processing = False
            while len(tasks) >= self.worker_adapter.current_workers or \
                  (batch_number_for_display == total_batches_in_chunk and tasks):
                if self.shutdown_event.is_set() and not shutdown_processing:
                    # On shutdown wait for all pending tasks to complete
                    if tasks:
                        self.logger.info(f"Shutdown requested, waiting for {len(tasks)} pending tasks to complete...")
                        shutdown_processing = True
                        try:
                            completed_tasks, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=30)
                            tasks = pending_tasks
                            # Process completed tasks before exit
                            for t in completed_tasks:
                                try:
                                    api_results_for_batch: List[NameResult] = t.result()
                                    full_batch_results = batch_results_from_checkpoint + api_results_for_batch
                                    self._update_dataframe_with_results(df_chunk, full_batch_results, self.name_to_idx_map)
                                    
                                    checkpoint_data_for_batch = {}
                                    for r_api in api_results_for_batch:
                                        if r_api.original_name in self.name_to_idx_map:
                                            idx = self.name_to_idx_map[r_api.original_name]
                                            name_id = str(self.df_chunk.loc[idx, 'ID'])
                                            checkpoint_data_for_batch[r_api.original_name] = {
                                                'vocative': r_api.vocative,
                                                'id': name_id
                                            }
                                        if r_api.success:
                                            self.batch_size_adapter.record_success()
                                            self.worker_adapter.record_success()
                                        else:
                                            self.batch_size_adapter.record_error()
                                            self.worker_adapter.record_error()
                                    
                                    self.pending_checkpoint_data.update(checkpoint_data_for_batch)
                                    processed_batches_count_in_chunk += 1
                                except Exception as e:
                                    self.logger.error(f"Error processing task during shutdown: {e}", exc_info=True)
                        except asyncio.TimeoutError:
                            self.logger.warning(f"Timeout waiting for tasks to complete during shutdown. Cancelling remaining tasks.")
                            for t in tasks:
                                t.cancel()
                    break

                completed_tasks, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = pending_tasks
                
                for t in completed_tasks:
                    try:
                        api_results_for_batch: List[NameResult] = t.result()
                        
                        # Merge API results with checkpoint results for this batch
                        full_batch_results = batch_results_from_checkpoint + api_results_for_batch
                        
                        self._update_dataframe_with_results(df_chunk, full_batch_results, self.name_to_idx_map)
                        
                        # Prepare checkpoint data (only successful new results)
                        checkpoint_data_for_batch = {}
                        for r_api in api_results_for_batch:
                            if r_api.original_name in self.name_to_idx_map:
                                idx = self.name_to_idx_map[r_api.original_name]
                                name_id = str(self.df_chunk.loc[idx, 'ID'])
                                checkpoint_data_for_batch[r_api.original_name] = {
                                    'vocative': r_api.vocative,
                                    'id': name_id
                                }
                            if r_api.success: 
                                self.batch_size_adapter.record_success()
                                self.worker_adapter.record_success()
                            else: 
                                self.batch_size_adapter.record_error()
                                self.worker_adapter.record_error()

                        # Accumulate checkpoint data
                        self.pending_checkpoint_data.update(checkpoint_data_for_batch)
                        self.batches_since_last_checkpoint += 1
                        
                        # Save checkpoint every N batches or at last batch
                        # Note: batch_number_for_display is not reliable here due to parallelism,
                        # but checkpoint_interval ensures correct saving
                        should_save_checkpoint = (
                            self.batches_since_last_checkpoint >= self.checkpoint_interval
                        )
                        
                        if should_save_checkpoint and self.pending_checkpoint_data:
                            # Use batch_number from task (set in _process_single_batch)
                            batch_num_for_checkpoint = int(t.get_name()) if t.get_name().isdigit() else processed_batches_count_in_chunk + 1
                            self.checkpoint_service.save_checkpoint(
                                chunk_index, 
                                batch_num_for_checkpoint,
                                self.pending_checkpoint_data,
                                is_chunk_complete=False 
                            )
                            self.pending_checkpoint_data = {}
                            self.batches_since_last_checkpoint = 0
                        
                        processed_batches_count_in_chunk += 1
                        self._log_batch_progress(processed_batches_count_in_chunk, total_batches_in_chunk, chunk_index, start_time_chunk)

                    except asyncio.CancelledError:
                        self.logger.warning(f"Task for batch (name: {t.get_name()}) was cancelled.")
                        raise 
                    except Exception as e:
                        self.logger.error(f"Error processing result for batch (name: {t.get_name()}): {e}", exc_info=True)
                        self.batch_size_adapter.record_error()
                        self.worker_adapter.record_error()
            
            # Adaptive adjustments after group of tasks or at the end of batch
            self.worker_adapter.adjust()
            self.batch_size_adapter.adjust()
            
            # If it is the last batch in chunk, save checkpoint
            if batch_number_for_display == total_batches_in_chunk and self.pending_checkpoint_data:
                self.checkpoint_service.save_checkpoint(
                    chunk_index,
                    batch_number_for_display,
                    self.pending_checkpoint_data,
                    is_chunk_complete=False
                )
                self.pending_checkpoint_data = {}
                self.batches_since_last_checkpoint = 0

        # After completing all batches in chunk (or interruption)
        if not self.shutdown_event.is_set() and processed_batches_count_in_chunk == total_batches_in_chunk:
            # Save remaining pending checkpoint data before completing chunk
            if self.pending_checkpoint_data:
                self.checkpoint_service.save_checkpoint(
                    chunk_index, 
                    processed_batches_count_in_chunk, 
                    self.pending_checkpoint_data,
                    is_chunk_complete=False
                )
                self.pending_checkpoint_data = {}
            self.checkpoint_service.save_checkpoint(chunk_index, 0, {}, is_chunk_complete=True)
            self.batches_since_last_checkpoint = 0  # Reset for next chunk
            self.logger.info(f"Chunk {chunk_index} fully processed and final checkpoint saved.")
        elif self.shutdown_event.is_set():
            # On shutdown save all pending checkpoint data
            if self.pending_checkpoint_data:
                self.logger.info(f"Saving checkpoint before shutdown. Pending data: {len(self.pending_checkpoint_data)} names.")
                self.checkpoint_service.save_checkpoint(
                    chunk_index,
                    processed_batches_count_in_chunk,
                    self.pending_checkpoint_data,
                    is_chunk_complete=False
                )
                self.pending_checkpoint_data = {}
                self.logger.info(f"Checkpoint saved successfully before shutdown.")
            self.logger.info(f"Chunk {chunk_index} processing interrupted by shutdown signal. "
                             f"{processed_batches_count_in_chunk}/{total_batches_in_chunk} batches processed.")
        else:
            self.logger.warning(f"Chunk {chunk_index} processing finished, but not all batches completed. "
                                f"Processed: {processed_batches_count_in_chunk}/{total_batches_in_chunk}.")

    async def _process_single_batch(self, names_to_process: List[str], batch_number: int, chunk_idx: int) -> List[NameResult]:
        """Processes a single batch of names not in checkpoint."""
        asyncio.current_task().set_name(str(batch_number))

        results: List[NameResult] = []
        for name in names_to_process:
            if self.shutdown_event.is_set():
                self.logger.info(f"Shutdown during batch {batch_number} (chunk {chunk_idx}), name '{name}'. Stopping batch.")
                break

            await self.delay_adapter.wait()
            result = await self.name_service.process_single_name(name)
            results.append(result)

        # Update adaptive mechanisms after batch completion
        self._update_adaptive_mechanisms(results)
        return results

    def _update_dataframe_with_results(self, df_chunk: pd.DataFrame, results: List[NameResult], name_to_idx_map: Dict):
        """Updates DataFrame chunk with results."""
        new_results_df = pd.DataFrame(columns=df_chunk.columns)
        
        for res in results:
            if res.original_name in name_to_idx_map:
                idx = name_to_idx_map[res.original_name]
                df_chunk.loc[idx, 'Vocative'] = res.vocative
                df_chunk.loc[idx, 'Vocative First Name'] = res.first_name
                df_chunk.loc[idx, 'Vocative Last Name'] = res.surname
                if res.error_message:
                    if 'Error' not in df_chunk.columns: df_chunk['Error'] = pd.NA
                    df_chunk.loc[idx, 'Error'] = res.error_message
                
                new_row = df_chunk.loc[idx].copy()
                new_results_df = pd.concat([new_results_df, pd.DataFrame([new_row])], ignore_index=True)
            else:
                self.logger.warning(f"Name '{res.original_name}' from results not found in current DataFrame chunk for update.")
        
        if not new_results_df.empty:
            try:
                new_results_df.to_csv(FILE_CONFIG['OUTPUT_FILE'], mode='a', header=False, index=False, encoding='utf-8')
                self.logger.debug(f"Updated CSV file with {len(new_results_df)} new results")
            except Exception as e:
                self.logger.error(f"Error saving to CSV file: {e}", exc_info=True)
    
    def _log_batch_progress(self, completed_in_chunk: int, total_in_chunk: int, chunk_idx:int, chunk_start_time: float):
        progress = (completed_in_chunk / total_in_chunk) * 100 if total_in_chunk > 0 else 0
        elapsed_chunk = time.time() - chunk_start_time
        speed_batch_per_s = completed_in_chunk / elapsed_chunk if elapsed_chunk > 0 else 0
        self.logger.info(
            f"Chunk {chunk_idx} Progress: {progress:.1f}% ({completed_in_chunk}/{total_in_chunk} batches). "
            f"Speed: {speed_batch_per_s:.2f} batch/s. "
            f"Delay: {self.delay_adapter.current_delay:.2f}s, "
            f"Workers: {self.worker_adapter.current_workers}, "
            f"BatchSizeCfg: {self.batch_size_adapter.current_size}"
        )
