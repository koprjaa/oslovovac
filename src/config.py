
"""
Project: Oslovovac
File: src/config.py
Description: Configuration constants for logging, HTTP requests, API endpoints, and file paths.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
from typing import List, Dict

# --- CONFIGURATION CONSTANTS ---
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Added %(name)s
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'level': 'INFO'
        }
        # Optional: Add FileHandler to save logs to a file
        # 'file': {
        #     'class': 'logging.FileHandler',
        #     'formatter': 'detailed',
        #     'filename': 'logs/name_processor.log', # Ensure 'logs' directory exists
        #     'level': 'DEBUG'
        # }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console'], # Add 'file' if defined
            'level': 'INFO',
            'propagate': True
        },
        'aiohttp': { # Suppress verbose aiohttp logging
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False
        }
    }
}

HTTP_CONFIG = {
    'TIMEOUT': 15,  # Increased from 10 to 15 for better resilience against slow responses
    'MAX_RETRIES': 5,  # Increased from 3 to 5
    'INITIAL_DELAY': 0.01,  # Decreased from 0.1 to 0.01 for faster processing
    'MAX_DELAY': 5,
    'MIN_DELAY': 0.01,  # Decreased from 0.05 to 0.01
    'BATCH_SIZE': 2000,  # Increased from 1000 to 2000
    'MAX_WORKERS': 100,  # Increased from 50 to 100
    'MIN_WORKERS': 20,  # Increased from 10 to 20
    'INITIAL_WORKERS': 30,  # Increased from 15 to 30
    'CHECKPOINT_INTERVAL': 5,  # Number of batches between checkpoint saves
    'WORKER_SCALE_INTERVAL': 3,
    'WORKER_SCALE_DOWN_INTERVAL': 3,
    'CHUNK_SIZE': 100000, # Process 100k CSV rows at a time
    'MEMORY_LIMIT': 0.8, # Not actively used for control
    
    # New constants for rate limiting and backoff
    'RATE_LIMIT_INITIAL_BACKOFF_SECONDS': 1.0,
    'RATE_LIMIT_BACKOFF_FACTOR_MULTIPLIER': 1.8,
    'RATE_LIMIT_MAX_GLOBAL_BACKOFF_FACTOR': 20.0,
    'SUCCESSES_TO_REDUCE_BACKOFF': 50,  # Consecutive successes required to reduce backoff
    'COOLDOWN_AFTER_FACTOR_INCREASE_S': 60,  # 1 minute cooldown after factor increase
    'BACKOFF_REDUCTION_ON_SUCCESS_RATIO': 0.85,  # Reduce factor by 15% on success
    'CONNECTION_ERROR_BACKOFF_MULTIPLIER': 1.5,  # Milder backoff for connection errors
    'SERVER_ERROR_BACKOFF_MULTIPLIER': 1.8,  # More aggressive backoff for server errors
    'RATE_LIMIT_BACKOFF_MULTIPLIER': 2.0,  # Most aggressive backoff for rate limiting
}

USER_AGENTS: List[str] = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    # ... (add more if needed)
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
