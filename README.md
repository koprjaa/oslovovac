# Overview
This project is an asynchronous high-performance batch processor for generating Czech vocatives (5th grammatical case) from proper names. It utilizes an external third-party API to process large datasets of names, enabling automated localization of salutations in communications.

# Motivation
Correct declension of names is essential for professional communication in the Czech language. Manual declension for large contact lists is impractical. This tool automates the process with a focus on reliability, throughput, and respecting remote server constraints through adaptive rate limiting.

# What This Project Does
The application reads a CSV file containing names, sends asynchronous requests to the `sklonuj.cz` service, scrapes the resulting vocative forms, and saves the output incrementally. It handles network instability, rate limits, and server errors automatically.

# Architecture
The project follows a modular architecture within the `src/` package:
-   **Service Layer** (`src/services.py`): Encapsulates core logic. `NameService` handles single-name processing; `BatchService` manages concurrency; `CheckpointService` handles persistency.
-   **Adaptive Layer** (`src/adapters.py`): Implements feedback loops to dynamically adjust request delays, concurrency levels, and batch sizes based on success rates.
-   **Data Models** (`src/models.py`): Uses Python Dataclasses for type-safe data passing.
-   **Configuration** (`src/config.py`): Centralized configuration for easy tuning.

# Tech Stack
-   **Language**: Python 3.x
-   **Concurrency**: `asyncio`, `aiohttp` for non-blocking I/O.
-   **Data Processing**: `pandas` for efficient CSV handling and chunking.
-   **Parsing**: `BeautifulSoup4` for HTML extraction.

# Data Sources
-   **Input**: CSV file (`jmena.csv`) containing raw names.
-   **Processing**: `https://sklonuj.cz` (External Web Service).
-   **Output**: CSV file (`jmena_s_oslovenim.csv`) with original names and their vocatives.

# Key Design Decisions
1.  **Adaptive Rate Limiting**: Instead of hard-coded limits, the system monitors response codes (429/500) and response times to dynamically adjust the number of concurrent workers and delay between requests. This maximizes throughput while avoiding IP bans.
2.  **Checkpointing**: Processing state is saved to `checkpoint.json` after every N batches. This allows the application to resume interrupted jobs without re-processing successful records.
3.  **Asynchronous I/O**: Used to handle high-latency network operations efficiently, significantly outperforming synchronous equivalents.
4.  **Chunk Processing**: Input files are processed in chunks using `pandas` to maintain constant memory usage regardless of input file size.

# Limitations
-   **External Dependency**: The application functionality is strictly coupled to the availability and interface of `sklonuj.cz`.
-   **Network Reliability**: Performance heavily depends on network latency and the remote server's capacity.
-   **Single-Threaded CPU Logic**: While I/O is concurrent, data parsing runs on the main event loop (sufficient for this use case but a theoretical bottleneck).

# How to Run
1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Place your input data in `jmena.csv` (or configure via `src/config.py`).
3.  Run the application:
    ```bash
    python main.py
    ```

# Example Usage
Input (`jmena.csv`):
```csv
ID,Trade_Name
1,Jan Novák
2,Petr Svoboda
```

Output (`jmena_s_oslovenim.csv`):
```csv
ID,Trade_Name,Oslovení,Oslovení jméno,Oslovení příjmení,Chyba
1,Jan Novák,Jane Nováku,Jane,Nováku,
2,Petr Svoboda,Petře Svobodo,Petře,Svobodo,
```

# Future Improvements
-   **Offline Model**: Implement a local NLP model or rule-based engine to remove the dependency on the external API.
-   **Proxy Rotation**: Integrate proxy support to scale beyond the rate limits of a single IP address.
-   **CLI Arguments**: Replace static configuration with command-line arguments (e.g., `--input`, `--output`, `--workers`).

# Author
Jan Alexandr Kopřiva
jan.alexandr.kopriva@gmail.com

# License
MIT
