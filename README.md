# Czech Vocative Generator

## 1. Overview

This tool generates Czech vocatives (5th grammatical case) from proper names in large datasets. It uses an external third-party API to process names asynchronously, enabling automated localization of salutations in communications.

## 2. Motivation

Correct declension of names is essential for professional communication in the Czech language. Manual declension for large contact lists is impractical. This tool automates the process with a focus on reliability, throughput, and respecting remote server constraints through adaptive rate limiting.

## 3. What This Project Does

- Ingests CSV files with a `Name` column containing proper names.
- Sends asynchronous HTTP requests to an external web service (`sklonuj.cz`).
- Scrapes vocative forms from HTML responses.
- Saves results incrementally to output CSV files.
- Handles network instability, rate limits, and server errors automatically.
- Implements checkpointing to resume interrupted processing.

## 4. Architecture

The project follows a modular architecture within the `src/` package:

- **Service Layer** (`src/services.py`): Core business logic. `NameService` handles single-name processing; `BatchService` manages concurrency; `CheckpointService` handles persistency.
- **Adaptive Layer** (`src/adapters.py`): Implements feedback loops to dynamically adjust request delays, concurrency levels, and batch sizes based on success rates.
- **Data Models** (`src/models.py`): Uses Python Dataclasses for type-safe data passing.
- **Configuration** (`src/config.py`): Centralized configuration for easy tuning.
- **Utilities** (`src/utils.py`): Graceful shutdown handling and HTTP session management.

## 5. Tech Stack

- **Language**: Python 3.x
- **Concurrency**: `asyncio`, `aiohttp` for non-blocking I/O.
- **Data Processing**: `pandas` for efficient CSV handling and chunking.
- **Parsing**: `BeautifulSoup4` for HTML extraction.

## 6. Data Sources

- **Input**: CSV file (`names.csv`) containing raw names (configurable via `src/config.py`).
- **Processing**: `https://sklonuj.cz` (External Web Service).
- **Output**: CSV file (`names_with_vocative.csv`) with original names and their vocatives.

## 7. Key Design Decisions

1. **Adaptive Rate Limiting**: Instead of hard-coded limits, the system monitors response codes (429/500) and response times to dynamically adjust the number of concurrent workers and delay between requests. This maximizes throughput while avoiding IP bans.
2. **Checkpointing**: Processing state is saved to `checkpoint.json` after every N batches. This allows the application to resume interrupted jobs without re-processing successful records.
3. **Asynchronous I/O**: Used to handle high-latency network operations efficiently, significantly outperforming synchronous equivalents.
4. **Chunk Processing**: Input files are processed in chunks using `pandas` to maintain constant memory usage regardless of input file size.
5. **Global Backoff Strategy**: Implements exponential backoff with jitter for rate limit and server errors, with automatic recovery when conditions improve.

## 8. Limitations

- **External Dependency**: The application functionality is strictly coupled to the availability and interface of `sklonuj.cz`.
- **Network Reliability**: Performance heavily depends on network latency and the remote server's capacity.
- **Single-Threaded CPU Logic**: While I/O is concurrent, data parsing runs on the main event loop (sufficient for this use case but a theoretical bottleneck).
- **Hardcoded Paths**: File paths are hardcoded in `src/config.py` (can be overridden by modifying the configuration).

## 9. How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Place your input data in `names.csv` (or configure via `src/config.py`).
3. Run the application:
   ```bash
   python main.py
   ```

## 10. Example Usage

Input (`names.csv`):
```csv
ID,Name
1,Jan Novák
2,Petr Svoboda
```

Output (`names_with_vocative.csv`):
```csv
ID,Name,Vocative,Vocative First Name,Vocative Last Name,Error
1,Jan Novák,Jane Nováku,Jane,Nováku,
2,Petr Svoboda,Petře Svobodo,Petře,Svobodo,
```

## 11. Future Improvements

- **Offline Model**: Implement a local NLP model or rule-based engine to remove the dependency on the external API.
- **Proxy Rotation**: Integrate proxy support to scale beyond the rate limits of a single IP address.
- **CLI Arguments**: Replace static configuration with command-line arguments (e.g., `--input`, `--output`, `--workers`).

## 12. Author

Jan Alexandr Kopřiva  
jan.alexandr.kopriva@gmail.com

## 13. License

MIT
