import asyncio
import logging
import logging.config
import pandas as pd
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
import time
import traceback
from typing import List, Dict, Optional, Tuple, Set
import json
from pathlib import Path
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dataclasses import dataclass
import os
import signal
import sys
import gc
from contextlib import asynccontextmanager

# Create logs directory if it doesn't exist
# os.makedirs('logs', exist_ok=True) # Již existuje nebo není striktně nutné

# --- CONFIGURATION CONSTANTS ---
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Přidáno %(name)s
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'level': 'INFO'
        }
        # Můžete přidat FileHandler pro ukládání logů do souboru
        # 'file': {
        #     'class': 'logging.FileHandler',
        #     'formatter': 'detailed',
        #     'filename': 'logs/name_processor.log', # Ujistěte se, že adresář 'logs' existuje
        #     'level': 'DEBUG'
        # }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console'], # Přidat 'file' pokud je definován
            'level': 'INFO',
            'propagate': True
        },
        'aiohttp': { # Potlačení příliš verbózního logování z aiohttp
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False
        }
    }
}

HTTP_CONFIG = {
    'TIMEOUT': 15,  # Zvýšeno z 10 na 15 pro větší odolnost vůči pomalým odpovědím
    'MAX_RETRIES': 5,  # Zvýšeno z 3 na 5
    'INITIAL_DELAY': 0.01,  # Sníženo z 0.1 na 0.01 pro rychlejší zpracování
    'MAX_DELAY': 5,
    'MIN_DELAY': 0.01,  # Sníženo z 0.05 na 0.01
    'BATCH_SIZE': 2000,  # Zvýšeno z 1000 na 2000
    'MAX_WORKERS': 100,  # Zvýšeno z 50 na 100
    'MIN_WORKERS': 20,  # Zvýšeno z 10 na 20
    'INITIAL_WORKERS': 30,  # Zvýšeno z 15 na 30
    'CHECKPOINT_INTERVAL': 5,  # Počet batchů mezi checkpoint ukládáním
    'WORKER_SCALE_INTERVAL': 3,
    'WORKER_SCALE_DOWN_INTERVAL': 3,
    'CHUNK_SIZE': 100000, # Zpracování 100k řádků CSV najednou
    'MEMORY_LIMIT': 0.8, # Není aktivně použito v kódu pro řízení
    
    # Nové konstanty pro rate limiting a backoff
    'RATE_LIMIT_INITIAL_BACKOFF_SECONDS': 1.0,
    'RATE_LIMIT_BACKOFF_FACTOR_MULTIPLIER': 1.8,
    'RATE_LIMIT_MAX_GLOBAL_BACKOFF_FACTOR': 20.0,
    'SUCCESSES_TO_REDUCE_BACKOFF': 50,  # Počet úspěšných operací v řadě pro snížení backoffu
    'COOLDOWN_AFTER_FACTOR_INCREASE_S': 60,  # 1 minuta cooldown po zvýšení faktoru
    'BACKOFF_REDUCTION_ON_SUCCESS_RATIO': 0.85,  # Snížení faktoru o 15% při úspěchu
    'CONNECTION_ERROR_BACKOFF_MULTIPLIER': 1.5,  # Mírnější backoff pro chyby připojení
    'SERVER_ERROR_BACKOFF_MULTIPLIER': 1.8,  # Agresivnější backoff pro serverové chyby
    'RATE_LIMIT_BACKOFF_MULTIPLIER': 2.0,  # Nejagresivnější backoff pro rate limiting
}

USER_AGENTS: List[str] = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    # ... (můžete přidat více, pokud je potřeba)
]

API_CONFIG = {
    'BASE_URL': 'https://sklonuj.cz',
    'ENDPOINT': '/generator-osloveni/',
}

FILE_CONFIG = {
    'INPUT_FILE': 'jmena.csv',
    'OUTPUT_FILE': 'jmena_s_oslovenim.csv',
    'CHECKPOINT_FILE': 'checkpoint.json'
}

# --- DATA MODELS ---
@dataclass
class NameResult:
    original_name: str
    vocative: str
    first_name: str
    surname: str
    success: bool
    error_message: Optional[str] = None

    @classmethod
    def from_vocative(cls, original_name: str, vocative: str) -> 'NameResult':
        first_name, surname = cls.split_vocative(vocative)
        # Úspěch je, pokud je vokativ jiný než originál a není prázdný
        success = bool(vocative and vocative.strip().lower() != original_name.strip().lower())
        return cls(
            original_name=original_name,
            vocative=vocative.strip() if vocative else original_name, # Zajistit, že vocative není None
            first_name=first_name,
            surname=surname,
            success=success
        )

    @classmethod
    def error(cls, original_name: str, error_message: str) -> 'NameResult':
        return cls(
            original_name=original_name,
            vocative=original_name, # V případě chyby vrátit originál
            first_name='',
            surname='',
            success=False,
            error_message=error_message
        )

    @staticmethod
    def split_vocative(vocative: str) -> Tuple[str, str]:
        if not vocative:
            return "", ""
        parts = vocative.strip().split()
        if not parts: # Pokud je vocative jen mezery
            return "", ""
        if len(parts) <= 1:
            return parts[0], ""
        surname = parts[-1]
        first_names = " ".join(parts[:-1])
        return first_names, surname

# --- CORE ADAPTERS ---
class AdaptiveDelay:
    def __init__(self, initial_delay: float, max_delay: float, min_delay: float):
        self.current_delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.adjustment_interval = HTTP_CONFIG.get('WORKER_SCALE_INTERVAL', 1.0) # Použít z configu, delší interval
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
        self.success_count = 0 # Sledovat úspěchy/neúspěchy za interval
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
            if total_ops == 0: return # Nic se nedělo

            success_rate = self.success_count / total_ops
            
            if success_rate > 0.9 and self.current_workers < self.max_workers : # Vysoká úspěšnost
                self.current_workers = min(self.max_workers, self.current_workers + max(1, int(self.current_workers * 0.1))) # Přidat 10% nebo alespoň 1
            elif success_rate < 0.8 and self.current_workers > self.min_workers: # Nízká úspěšnost
                self.current_workers = max(self.min_workers, self.current_workers - max(1, int(self.current_workers * 0.2))) # Snížit o 20% nebo alespoň 1
            
            if old_workers != self.current_workers:
                 self.logger.info(f"Workers adjusted: {old_workers} -> {self.current_workers} (S/E: {self.success_count}/{self.error_count}, Rate: {success_rate:.1%})")
            
            self.success_count = 0
            self.error_count = 0
            self.last_adjustment = time.time()

class AdaptiveBatchSize: # Podobně jako AdaptiveWorkers
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

# --- SERVICES ---
class CheckpointService:
    def __init__(self):
        self.checkpoint_file = Path(FILE_CONFIG['CHECKPOINT_FILE'])
        self.processed_names: Dict[str, Dict[str, str]] = {}
        self.last_batch_completed_for_current_chunk = 0 # Sledování batchů v rámci aktuálního chunku
        self.last_chunk_fully_processed_index = -1 # Index posledního plně zpracovaného chunku
        self.logger = logging.getLogger(self.__class__.__name__)
        self._load_checkpoint()

    def _load_checkpoint(self) -> None:
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_names = data.get('processed_names', {})
                    # Pro kompatibilitu se starým formátem, kde byl jen 'last_batch'
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
            if self.checkpoint_file.exists(): # Pokud je soubor poškozen, zkusit smazat
                try:
                    self.checkpoint_file.unlink(missing_ok=True)
                    self.logger.info("Corrupted checkpoint file removed.")
                except OSError as ose:
                    self.logger.error(f"Could not remove corrupted checkpoint file: {ose}")


    def save_checkpoint(self, current_chunk_index: int, batch_number_in_chunk: int, 
                        processed_batch_data: Dict[str, Dict[str, str]], 
                        is_chunk_complete: bool) -> None:
        try:
            # Aktualizovat globální slovník zpracovaných jmen
            for name_key, data_val in processed_batch_data.items():
                if name_key and data_val.get('vocative'): # Ukládat jen platná data
                    self.processed_names[name_key] = data_val
            
            data_to_save = {
                'last_chunk_fully_processed_index': self.last_chunk_fully_processed_index,
                'last_batch_completed_for_current_chunk': self.last_batch_completed_for_current_chunk,
                'processed_names': self.processed_names # Stále ukládáme všechna jména pro robustnost
            }

            if is_chunk_complete:
                data_to_save['last_chunk_fully_processed_index'] = current_chunk_index
                data_to_save['last_batch_completed_for_current_chunk'] = 0 # Reset pro další chunk
            else:
                # Pokud chunk není kompletní, ukládáme aktuální chunk index jen pro informaci,
                # ale `last_chunk_fully_processed_index` se nemění.
                # `last_batch_completed_for_current_chunk` se aktualizuje na aktuální batch.
                data_to_save['last_batch_completed_for_current_chunk'] = batch_number_in_chunk


            temp_checkpoint_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, separators=(',', ':'))  # Bez indentace pro menší soubor
            os.replace(temp_checkpoint_file, self.checkpoint_file) # Atomický přesun

            self.logger.info(f"Checkpoint saved. Chunk_idx: {current_chunk_index}, batch_in_chunk: {batch_number_in_chunk}, "
                             f"is_chunk_complete: {is_chunk_complete}. Total processed: {len(self.processed_names)}.")
            
            # Aktualizovat stav instance po úspěšném uložení
            if is_chunk_complete:
                self.last_chunk_fully_processed_index = current_chunk_index
                self.last_batch_completed_for_current_chunk = 0
            else:
                self.last_batch_completed_for_current_chunk = batch_number_in_chunk
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}", exc_info=True)

    def get_resume_info(self, current_chunk_index: int) -> Tuple[int, Dict[str, Dict[str,str]]]:
        """Vrátí info pro obnovení zpracování aktuálního chunku."""
        if current_chunk_index <= self.last_chunk_fully_processed_index:
            # Tento chunk byl již plně zpracován v minulosti
            return -1, {} # Signalizuje, že chunk lze přeskočit
        
        # Pokud je to stejný chunk, na kterém jsme skončili minule (a nebyl dokončen)
        # nebo pokud začínáme nový chunk po posledním plně dokončeném
        if current_chunk_index == self.last_chunk_fully_processed_index + 1:
            return self.last_batch_completed_for_current_chunk, self.processed_names
        
        # Začínáme úplně nový chunk, který není bezprostředně za posledním dokončeným
        # (např. pokud se smazal checkpoint nebo přeskočily chunky)
        return 0, self.processed_names


    def is_name_globally_processed(self, name: str) -> bool:
        return name in self.processed_names

    def get_globally_processed_name_data(self, name: str) -> Optional[Dict[str,str]]:
        return self.processed_names.get(name)

    def clear_checkpoint_for_new_run(self) -> None: # Volitelné, pokud chceme začít od nuly
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
        self.timeout_val = ClientTimeout(total=HTTP_CONFIG['TIMEOUT'])
        self.max_retries = HTTP_CONFIG['MAX_RETRIES']
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Per-request state
        self.current_request_cookies = {} # Cookies specifické pro jeden cyklus GET->POST
        
        # Rate limiting a backoff state
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
        """Zpracuje rate limit nebo serverovou chybu s adaptivním backoffem."""
        current_time = time.time()
        self.last_error_time = current_time
        self.consecutive_successes_since_last_error = 0
        self.total_errors += 1

        # Zvýšit backoff faktor podle typu chyby
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

        # Vypočítat delay s jitterem
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
        """Sníží backoff faktor při úspěšných operacích, pokud jsou splněny podmínky."""
        current_time = time.time()
        self.consecutive_successes_since_last_error += 1
        self.total_requests += 1

        # Kontrola cooldown periody
        if (current_time - self.last_backoff_factor_increase_time) < HTTP_CONFIG['COOLDOWN_AFTER_FACTOR_INCREASE_S']:
            return

        # Kontrola počtu úspěšných operací v řadě
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
        """Získá CSRF tokeny/cookies z formuláře (GET). Vrací True při úspěchu."""
        try:
            async with self.session.get(url, headers=headers, timeout=self.timeout_val) as response_get:
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
                raise  # Pro ostatní chyby vyvolat a nechat vnější handler zpracovat
            return False
            
        except aiohttp.ClientError as e:
            self.logger.warning(f"ClientError on GET {url}: {e} (Attempt: {attempt}).")
            await self._handle_rate_limit_or_server_error_delay('connection_error', attempt)
            return False

    async def _submit_form_and_get_vocative(self, url: str, headers: Dict[str, str], name: str, attempt: int) -> Optional[str]:
        """Odešle formulář (POST) a extrahuje vokativ. Vrací vokativ nebo None při chybě vyžadující retry."""
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
            async with self.session.post(url, data=data, headers=post_headers, cookies=self.current_request_cookies, timeout=self.timeout_val) as response_post:
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
        """Extrahuje vokativ z HTML odpovědi s vylepšeným logováním."""
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
                    
                    # Kontrola, zda je vokativ jiný než originál
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
        """Zpracuje jedno jméno s vylepšeným error handlingem a retry logikou."""
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
                if e.status in [401, 403]:  # Nerecovery chyby
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
        
        # Statistiky pro adaptivní mechanismy
        self.current_batch_successes = 0
        self.current_batch_errors = 0
        self.last_adaptation_time = time.time()
        
        # Checkpoint interval tracking
        self.checkpoint_interval = HTTP_CONFIG.get('CHECKPOINT_INTERVAL', 5)
        self.batches_since_last_checkpoint = 0
        self.pending_checkpoint_data = {}  # Akumulovat data mezi checkpointy

    def _update_adaptive_mechanisms(self, batch_results: List[NameResult]) -> None:
        """Aktualizuje adaptivní mechanismy na základě výsledků batch."""
        current_time = time.time()
        
        # Resetovat počítadla pro nový batch
        self.current_batch_successes = 0
        self.current_batch_errors = 0
        
        # Počítat úspěchy a chyby
        for result in batch_results:
            if result.success:
                self.current_batch_successes += 1
            else:
                self.current_batch_errors += 1
        
        total_requests = self.current_batch_successes + self.current_batch_errors
        if total_requests > 0:
            success_rate = self.current_batch_successes / total_requests
            
            # Aktualizovat delay adapter
            if success_rate >= 0.95:
                self.delay_adapter.on_success(success_rate)
            elif success_rate < 0.8:
                self.delay_adapter.on_error(success_rate)
            
            # Aktualizovat worker adapter
            if success_rate >= 0.9:
                self.worker_adapter.record_success()
            else:
                self.worker_adapter.record_error()
            
            # Aktualizovat batch size adapter
            if success_rate >= 0.95:
                self.batch_size_adapter.record_success()
            else:
                self.batch_size_adapter.record_error()
            
            # Logovat statistiky
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

        # Uložit DataFrame a mapu pro použití v _process_single_batch
        self.df_chunk = df_chunk
        self.name_to_idx_map = {name: i for i, name in df_chunk['Trade_Name'].items()}

        last_completed_batch_in_chunk, globally_processed_names = self.checkpoint_service.get_resume_info(chunk_index)
        if last_completed_batch_in_chunk == -1: # Chunk byl již plně zpracován
            self.logger.info(f"Chunk {chunk_index} already fully processed. Skipping.")
            return

        all_names_in_chunk = df_chunk['Trade_Name'].tolist()
        
        # Rozdělení na batche podle adaptivní velikosti
        initial_batch_size = self.batch_size_adapter.current_size 
        
        # Generátor batchů
        def chunk_list(data: list, size: int):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        batches_of_names = list(chunk_list(all_names_in_chunk, initial_batch_size))
        total_batches_in_chunk = len(batches_of_names)
        self.logger.info(f"Chunk {chunk_index} split into {total_batches_in_chunk} batches of (up to) {initial_batch_size} names.")

        # Reset checkpoint tracking pro nový chunk
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
            
            # Předzpracování jmen, která jsou již v checkpointu
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
                # Připravit data pro checkpoint s ID z DataFrame
                checkpoint_data = {}
                for r in batch_results_from_checkpoint:
                    if r.original_name in self.name_to_idx_map:
                        idx = self.name_to_idx_map[r.original_name]
                        name_id = str(self.df_chunk.loc[idx, 'ID'])
                        checkpoint_data[r.original_name] = {'vocative': r.vocative, 'id': name_id}
                
                # Akumulovat checkpoint data i pro checkpoint-only batche
                self.pending_checkpoint_data.update(checkpoint_data)
                self.batches_since_last_checkpoint += 1
                
                # Ukládat checkpoint každých N batchů nebo při posledním batch
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

            # Omezit počet běžících tasků adaptivním počtem workerů
            shutdown_processing = False
            while len(tasks) >= self.worker_adapter.current_workers or \
                  (batch_number_for_display == total_batches_in_chunk and tasks):
                if self.shutdown_event.is_set() and not shutdown_processing:
                    # Při shutdownu počkat na dokončení všech běžících tasků
                    if tasks:
                        self.logger.info(f"Shutdown requested, waiting for {len(tasks)} pending tasks to complete...")
                        shutdown_processing = True
                        try:
                            completed_tasks, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=30)
                            tasks = pending_tasks
                            # Zpracovat dokončené tasky před ukončením
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
                        
                        # Spojit výsledky z API s těmi z checkpointu pro tento batch
                        full_batch_results = batch_results_from_checkpoint + api_results_for_batch
                        
                        self._update_dataframe_with_results(df_chunk, full_batch_results, self.name_to_idx_map)
                        
                        # Připravit data pro checkpoint (pouze úspěšné nové výsledky)
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

                        # Akumulovat checkpoint data
                        self.pending_checkpoint_data.update(checkpoint_data_for_batch)
                        self.batches_since_last_checkpoint += 1
                        
                        # Ukládat checkpoint každých N batchů nebo při posledním batch
                        # Poznámka: batch_number_for_display v této části není spolehlivý kvůli paralelismu,
                        # ale checkpoint_interval zajišťuje správné ukládání
                        should_save_checkpoint = (
                            self.batches_since_last_checkpoint >= self.checkpoint_interval
                        )
                        
                        if should_save_checkpoint and self.pending_checkpoint_data:
                            # Použít batch_number z tasku (nastaveno v _process_single_batch)
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
            
            # Adaptivní úpravy po zpracování skupiny tasků nebo na konci batche
            self.worker_adapter.adjust()
            self.batch_size_adapter.adjust()
            
            # Pokud je to poslední batch v chunku, uložit checkpoint
            if batch_number_for_display == total_batches_in_chunk and self.pending_checkpoint_data:
                self.checkpoint_service.save_checkpoint(
                    chunk_index,
                    batch_number_for_display,
                    self.pending_checkpoint_data,
                    is_chunk_complete=False
                )
                self.pending_checkpoint_data = {}
                self.batches_since_last_checkpoint = 0

        # Po dokončení všech batchů v chunku (nebo přerušení)
        if not self.shutdown_event.is_set() and processed_batches_count_in_chunk == total_batches_in_chunk:
            # Uložit zbývající pending checkpoint data před dokončením chunku
            if self.pending_checkpoint_data:
                self.checkpoint_service.save_checkpoint(
                    chunk_index, 
                    processed_batches_count_in_chunk, 
                    self.pending_checkpoint_data,
                    is_chunk_complete=False
                )
                self.pending_checkpoint_data = {}
            self.checkpoint_service.save_checkpoint(chunk_index, 0, {}, is_chunk_complete=True)
            self.batches_since_last_checkpoint = 0  # Reset pro další chunk
            self.logger.info(f"Chunk {chunk_index} fully processed and final checkpoint saved.")
        elif self.shutdown_event.is_set():
            # Při shutdownu uložit všechny pending checkpoint data
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
        """Zpracuje jeden batch jmen, která nejsou v checkpointu."""
        asyncio.current_task().set_name(str(batch_number))

        results: List[NameResult] = []
        for name in names_to_process:
            if self.shutdown_event.is_set():
                self.logger.info(f"Shutdown during batch {batch_number} (chunk {chunk_idx}), name '{name}'. Stopping batch.")
                break

            await self.delay_adapter.wait()
            result = await self.name_service.process_single_name(name)
            results.append(result)

        # Aktualizovat adaptivní mechanismy po dokončení batch
        self._update_adaptive_mechanisms(results)
        return results

    def _update_dataframe_with_results(self, df_chunk: pd.DataFrame, results: List[NameResult], name_to_idx_map: Dict):
        """Aktualizuje DataFrame chunk s výsledky."""
        new_results_df = pd.DataFrame(columns=df_chunk.columns)
        
        for res in results:
            if res.original_name in name_to_idx_map:
                idx = name_to_idx_map[res.original_name]
                df_chunk.loc[idx, 'Oslovení'] = res.vocative
                df_chunk.loc[idx, 'Oslovení jméno'] = res.first_name
                df_chunk.loc[idx, 'Oslovení příjmení'] = res.surname
                if res.error_message:
                    if 'Chyba' not in df_chunk.columns: df_chunk['Chyba'] = pd.NA
                    df_chunk.loc[idx, 'Chyba'] = res.error_message
                
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

# --- MAIN APPLICATION LOGIC ---
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__) # Logger pro hlavní modul

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
    # Zvýšený limit konektoru, pokud MAX_WORKERS je vysoké
    # Limit per host je také důležitý, pokud všechny požadavky jdou na stejný server
    connector = TCPConnector(
        ssl=False, # Pokud server nemá validní SSL, jinak True nebo vynechat
        limit=HTTP_CONFIG['MAX_WORKERS'] * 2, # Více spojení pro flexibilitu
        limit_per_host=HTTP_CONFIG['MAX_WORKERS'] # Limit na hosta
    )
    # Timeout je již v NameService, toto je globální pro session
    timeout_config = ClientTimeout(total=HTTP_CONFIG['TIMEOUT'] + 5) # O něco vyšší než per-request timeout
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout_config,
        skip_auto_headers=['User-Agent'] # Ruční správa User-Agent
    ) as session:
        yield session

async def process_all_names_main_task(shutdown_handler: GracefulShutdownHandler):
    start_time_total = time.time()
    logger.info("Application starting name processing...")
    
    input_file = FILE_CONFIG['INPUT_FILE']
    output_file = FILE_CONFIG['OUTPUT_FILE']

    if not Path(input_file).exists():
        logger.error(f"Input file {input_file} not found. Exiting.")
        # Vytvoření dummy souboru pro testování, pokud neexistuje
        logger.info(f"Creating dummy input file {input_file} for testing.")
        dummy_data = {'ID': range(1, 21), 'Trade_Name': [f'Jméno Příjmení {i}' for i in range(1, 21)]}
        pd.DataFrame(dummy_data).to_csv(input_file, index=False, encoding='utf-8')
        # return # Pokud nechceme pokračovat s dummy daty, odkomentovat

    delay_adapter = AdaptiveDelay(
        initial_delay=HTTP_CONFIG['INITIAL_DELAY'],
        max_delay=HTTP_CONFIG['MAX_DELAY'],
        min_delay=HTTP_CONFIG['MIN_DELAY']
    )
    worker_adapter = AdaptiveWorkers(
        initial_workers=HTTP_CONFIG.get('INITIAL_WORKERS', HTTP_CONFIG['MIN_WORKERS']),
        max_workers=HTTP_CONFIG['MAX_WORKERS'],
        min_workers=HTTP_CONFIG['MIN_WORKERS']
    )
    user_agent_manager = UserAgentManager(USER_AGENTS)
    batch_size_adapter = AdaptiveBatchSize(
        initial_size=HTTP_CONFIG['BATCH_SIZE'],
        max_size=HTTP_CONFIG.get('MAX_BATCH_SIZE', HTTP_CONFIG['BATCH_SIZE'] * 2),
        min_size=HTTP_CONFIG.get('MIN_BATCH_SIZE', max(10, HTTP_CONFIG['BATCH_SIZE'] // 4))
    )
    checkpoint_service = CheckpointService()
    
    # Volitelně vyčistit checkpoint pro nový běh
    # checkpoint_service.clear_checkpoint_for_new_run()

    # Uložit checkpoint před začátkem běhu, aby se flushla všechna data
    if checkpoint_service.processed_names:
        logger.info(f"Flushing checkpoint before start: {len(checkpoint_service.processed_names)} processed names.")
        checkpoint_service.save_checkpoint(
            checkpoint_service.last_chunk_fully_processed_index,
            checkpoint_service.last_batch_completed_for_current_chunk,
            {},  # Prázdný dict, protože už všechna data jsou v processed_names
            is_chunk_complete=(checkpoint_service.last_batch_completed_for_current_chunk == 0)
        )
        logger.info("Checkpoint flushed successfully before start.")

    async with create_aiohttp_session() as session:
        name_service = NameService(session, user_agent_manager)
        batch_service = BatchService(
            name_service, checkpoint_service,
            delay_adapter, worker_adapter, batch_size_adapter,
            shutdown_handler.shutdown_event
        )

        chunk_size = HTTP_CONFIG['CHUNK_SIZE']
        total_processed_rows = 0
        current_chunk_index = -1 # Začínáme s -1, první chunk bude 0

        # Zjistit celkový počet řádků pro odhad (může být pomalé pro velmi velké soubory)
        try:
            with open(input_file, 'r', encoding='utf-8') as f_count:
                total_rows_in_file = sum(1 for _ in f_count) -1 # -1 pro hlavičku
            logger.info(f"Total records to process in {input_file}: {total_rows_in_file:,}")
        except Exception as e:
            logger.warning(f"Could not determine total rows in input file: {e}")
            total_rows_in_file = -1 # Neznámý počet

        # Vytvořit/přepsat výstupní soubor s hlavičkou, pokud neobnovujeme nebo je to první chunk
        # Nebo pokud checkpoint říká, že jsme na začátku
        if checkpoint_service.last_chunk_fully_processed_index == -1 and \
           checkpoint_service.last_batch_completed_for_current_chunk == 0:
            logger.info(f"Creating new output file {output_file} with headers.")
            pd.DataFrame(columns=['ID', 'Trade_Name', 'Oslovení', 'Oslovení jméno', 'Oslovení příjmení', 'Chyba']).to_csv(
                output_file, index=False, encoding='utf-8'
            )
        else:
            logger.info(f"Appending to existing output file {output_file} or resuming.")


        try:
            for chunk_df in pd.read_csv(input_file, chunksize=chunk_size, encoding='utf-8'):
                current_chunk_index += 1
                if shutdown_handler.shutdown_requested:
                    logger.info("Shutdown requested, stopping further chunk processing.")
                    break
                
                logger.info(f"\n--- Processing Chunk {current_chunk_index} ({len(chunk_df):,} records) ---")

                if checkpoint_service.last_chunk_fully_processed_index >= current_chunk_index:
                    logger.info(f"Chunk {current_chunk_index} was already fully processed. Skipping.")
                    total_processed_rows += len(chunk_df) # Přičíst k celkovému počtu
                    continue

                if 'Trade_Name' not in chunk_df.columns:
                    logger.error(f"Chunk {current_chunk_index} does not contain 'Trade_Name' column. Skipping.")
                    continue
                if 'ID' not in chunk_df.columns:
                    logger.warning(f"Chunk {current_chunk_index} does not contain 'ID' column. Generating sequential IDs for this chunk.")
                    # Generovat ID relativně k začátku tohoto chunku
                    chunk_df['ID'] = range(total_processed_rows + 1, total_processed_rows + len(chunk_df) + 1)

                # Vyčištění dat v chunku
                original_count_in_chunk = len(chunk_df)
                chunk_df.dropna(subset=['Trade_Name'], inplace=True)
                chunk_df['Trade_Name'] = chunk_df['Trade_Name'].astype(str).str.strip() # Zajistit string a odstranit mezery
                chunk_df = chunk_df[chunk_df['Trade_Name'] != ''] # Odstranit prázdné Trade_Name
                # Duplicity řešit opatrně, pokud ID je důležité. Zde se duplicity v Trade_Name ponechávají,
                # protože mohou mít různá ID. Pokud by se měly odstraňovat, tak s ohledem na ID.
                # chunk_df.drop_duplicates(subset=['Trade_Name'], keep='first', inplace=True)
                
                cleaned_count_in_chunk = len(chunk_df)
                if original_count_in_chunk != cleaned_count_in_chunk:
                    logger.info(f"Cleaned chunk {current_chunk_index}: removed {original_count_in_chunk - cleaned_count_in_chunk} empty/NaN 'Trade_Name's.")

                if cleaned_count_in_chunk == 0:
                    logger.info(f"Chunk {current_chunk_index} is empty after cleaning. Skipping.")
                    continue
                
                # Inicializace výstupních sloupců
                for col in ['Oslovení', 'Oslovení jméno', 'Oslovení příjmení', 'Chyba']:
                    if col not in chunk_df.columns: chunk_df[col] = pd.NA

                await batch_service.process_chunk_data(chunk_df, current_chunk_index)
                
                # Uložit zpracovaný chunk do výstupního souboru
                # Ukládáme pouze pokud jsme tento chunk skutečně zpracovávali (ne přeskočili)
                # A pokud nebyl shutdown uprostřed (to by se mělo řešit v batch_service nebo zde explicitněji)
                if not (checkpoint_service.last_chunk_fully_processed_index >= current_chunk_index and not shutdown_handler.shutdown_requested):
                    logger.info(f"Appending processed chunk {current_chunk_index} to {output_file}...")
                    chunk_df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')
                    logger.info(f"Chunk {current_chunk_index} successfully appended.")

                total_processed_rows += len(chunk_df)
                
                # Statistiky pro chunk
                success_condition = (chunk_df['Oslovení'].notna()) & \
                                    (chunk_df['Oslovení'] != '') & \
                                    (chunk_df['Oslovení'].str.lower() != chunk_df['Trade_Name'].str.lower())
                actual_success_count = success_condition.sum()
                logger.info(f"--- Chunk {current_chunk_index} Finished ---")
                logger.info(f"- Processed in chunk: {len(chunk_df):,} records")
                logger.info(f"- Successfully transformed: {actual_success_count:,} names ({actual_success_count/len(chunk_df)*100:.1f}%)")
                if total_rows_in_file > 0:
                    logger.info(f"- Overall progress: {total_processed_rows:,}/{total_rows_in_file:,} records "
                                f"({(total_processed_rows/total_rows_in_file*100):.1f}%)")
                else:
                    logger.info(f"- Overall processed: {total_processed_rows:,} records")

                del chunk_df
                gc.collect()
                logger.debug("Garbage collection after chunk processing.")

        except asyncio.CancelledError:
            logger.warning("Main processing task was cancelled. Saving checkpoint if possible...")
            # Zde by se měl uložit checkpoint, ale protože task je zrušen,
            # batch_service by měl již uložit checkpoint při detekci shutdown eventu
            # Zde jen logujeme, že k zrušení došlo
            raise # Znovu vyvolat, aby to bylo správně zpracováno v main_wrapper

        except Exception as e:
            logger.critical(f"Critical error during chunk processing: {e}", exc_info=True)
            # Zde by se mohl také uložit aktuální stav, pokud je to možné
            raise # Vyvolat chybu dál

    total_time = time.time() - start_time_total
    logger.info(f"=== Total Processing Finished ===")
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    logger.info(f"Total records processed across all chunks: {total_processed_rows:,}")
    # Zde by se mohly vypsat finální statistiky z checkpointu nebo výstupního souboru

async def main_wrapper():
    """Hlavní wrapper pro spuštění a správu shutdownu."""
    shutdown_handler = GracefulShutdownHandler()
    
    # Nastavení signálních handlerů pro asyncio smyčku
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler.handle_signal, sig, None)

    main_task = None
    try:
        logger.info("Starting main_wrapper...")
        main_task = asyncio.create_task(process_all_names_main_task(shutdown_handler))
        
        # Čekat na dokončení hlavního tasku NEBO na signál k ukončení
        # Použijeme asyncio.shield, pokud chceme, aby shutdown_event.wait() nebyl zrušen spolu s main_task
        # Ale jednodušší je nechat main_task, aby reagoval na CancelledError
        
        done, pending = await asyncio.wait(
            [main_task, shutdown_handler.shutdown_event.wait()],
            return_when=asyncio.FIRST_COMPLETED
        )

        if shutdown_handler.shutdown_event.is_set(): # Pokud byl shutdown event první
            logger.info("Shutdown event triggered. Cancelling main processing task if not done.")
            if not main_task.done():
                main_task.cancel()
                try:
                    await main_task # Počkat na dokončení zrušení
                except asyncio.CancelledError:
                    logger.info("Main processing task successfully cancelled after shutdown signal.")
                # Ostatní výjimky z main_task by se zde projevily, pokud by nebyly CancelledError
        
        # Zkontrolovat výsledek main_task, pokud skončil sám (ne kvůli shutdown_event)
        if main_task in done:
            try:
                main_task.result() # Získá výsledek nebo vyvolá výjimku, pokud task selhal
                logger.info("Main processing task completed.")
            except asyncio.CancelledError:
                # Již zpracováno výše, nebo zrušeno externě
                logger.info("Main processing task was cancelled (as expected or externally).")
            except Exception as e:
                logger.error(f"Main processing task failed with an exception: {e}", exc_info=True)
        
    except asyncio.CancelledError: # Pokud main_wrapper sám je zrušen (např. v testech)
        logger.info("Main_wrapper task itself was cancelled.")
        if main_task and not main_task.done():
            main_task.cancel()
            await asyncio.gather(main_task, return_exceptions=True) # Uklidit
            
    except Exception as e:
        logger.critical(f"Unhandled critical error in main_wrapper: {e}", exc_info=True)
    
    finally:
        logger.info("Main_wrapper finishing. Performing final cleanup of tasks...")
        # Zrušit všechny ostatní běžící tasky (pokud nějaké jsou mimo main_task)
        tasks_to_cancel = [task for task in asyncio.all_tasks(loop=loop) if task is not asyncio.current_task() and not task.done()]
        if tasks_to_cancel:
            logger.info(f"Cancelling {len(tasks_to_cancel)} outstanding tasks...")
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Outstanding tasks processed.")
        else:
            logger.info("No outstanding tasks to cancel.")
        logger.info("Application shutdown sequence complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main_wrapper())
    except KeyboardInterrupt: # I když máme signal handler, toto je fallback
        logger.warning("KeyboardInterrupt caught directly in __main__. Application will exit.")
    except Exception as e:
        logger.critical(f"Critical error at top level: {e}", exc_info=True)
    finally:
        logging.shutdown() # Zajistit flushnutí všech logů
        print("Application has exited.")