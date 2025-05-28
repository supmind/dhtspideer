# DHT Metainfo Crawler

## Overview

DHT Metainfo Crawler is a Python-based application designed to crawl the BitTorrent Distributed Hash Table (DHT) network to discover infohashes. For each discovered infohash, it attempts to download the corresponding torrent metainfo (the content of the `.torrent` file) from peers using the `ut_metadata` extension (BEP 9). Finally, the downloaded and verified metainfo is parsed and stored in an Elasticsearch instance for analysis and searching.

The project is built using `asyncio` for concurrent network operations, making it efficient in handling multiple DHT interactions and downloads simultaneously.

## Features

*   **DHT Crawling**: Joins the BitTorrent DHT network, listens for `announce_peer` and `get_peers` messages to discover infohashes.
*   **Peer Discovery for Infohashes**: For a given infohash, it actively queries the DHT to find peers that have announced for that infohash.
*   **Metainfo Download (BEP 9)**: Connects to discovered peers and attempts to download the torrent metadata using the `ut_metadata` extension (BEP 9 over BEP 10).
*   **Metadata Verification**: Verifies the downloaded metadata by checking if the SHA1 hash of its `info` dictionary matches the original infohash.
*   **Elasticsearch Storage**: Stores parsed torrent metainfo into a configured Elasticsearch index. The document ID is the infohash.
*   **Asynchronous Operations**: Built with `asyncio` for high concurrency and non-blocking I/O.
*   **Configurable**: Key operational parameters (Elasticsearch connection, concurrency, DHT port, logging) are configurable via a `config.json` file.
*   **Structured Logging**: Provides configurable logging to console and/or a file, with options for different log levels.
*   **Unit Tests**: Includes a suite of unit tests for key components.
*   **Graceful Shutdown**: Handles `SIGINT` and `SIGTERM` signals for a clean shutdown process.

## Project Structure

```
.
├── main_crawler.py        # Main orchestration script to run the crawler.
├── dht_crawler.py         # Handles DHT network interaction and infohash discovery.
├── metainfo_downloader.py # Downloads torrent metainfo from peers using BEP9.
├── es_loader.py           # Parses metainfo and stores it in Elasticsearch.
├── es_mapping.py          # Script to define and create the Elasticsearch index mapping.
├── config.json            # Configuration file for the application.
├── requirements.txt       # Python dependencies.
├── README.md              # This file.
└── tests/                   # Directory containing unit tests.
    ├── test_dht_crawler.py
    ├── test_es_loader.py
    ├── test_main_crawler.py
    └── test_metainfo_downloader.py
```

*   `main_crawler.py`: The main entry point to start the application. It initializes and coordinates all other modules.
*   `dht_crawler.py`: Implements the DHT node logic, including joining the network, discovering infohashes, and finding peers for a given infohash.
*   `metainfo_downloader.py`: Responsible for connecting to peers and fetching the torrent metadata using the `ut_metadata` protocol (BEP 9).
*   `es_loader.py`: Parses the downloaded bencoded metadata and stores it as a structured JSON document in Elasticsearch.
*   `es_mapping.py`: A utility script to set up the necessary index and mapping in Elasticsearch before running the crawler for the first time.
*   `config.json`: JSON file for configuring various aspects of the crawler.
*   `tests/`: Contains unit tests for different modules.

## Prerequisites

*   **Python**: Python 3.8 or higher is recommended.
*   **Elasticsearch**: A running Elasticsearch instance (version 7.x or 8.x should be compatible, client tested with Elasticsearch Python client for ES 8.x/9.x).
*   **Pip**: For installing Python packages.

## Setup and Installation

1.  **Clone the Repository**:
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create Elasticsearch Index and Mapping**:
    Before running the crawler for the first time, you need to create the `torrent_metainfo` index in Elasticsearch with the correct mapping.
    Ensure your Elasticsearch instance is running and accessible. Then, from the project directory, run:
    ```bash
    python es_mapping.py
    ```
    This script will attempt to connect to Elasticsearch (defaulting to `http://localhost:9200` unless modified in the script) and create the index.

3.  **Install Dependencies**:
    It's recommended to use a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
    Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration (`config.json`)

The `config.json` file allows you to configure the crawler's behavior. Create this file in the root directory of the project if it doesn't exist.

**Example `config.json`:**

```json
{
  "elasticsearch": {
    "host": "localhost",
    "port": 9200,
    "scheme": "http"
  },
  "crawler": {
    "dht_port": 6881,
    "max_concurrent_downloads": 10,
    "max_peers_per_infohash": 50
  },
  "logging": {
    "level": "INFO",
    "file_path": "dht_crawler.log"
  }
}
```

**Configuration Options:**

*   **`elasticsearch`**:
    *   `host` (string): Hostname or IP address of your Elasticsearch server.
    *   `port` (integer): Port number for Elasticsearch (e.g., 9200).
    *   `scheme` (string): Connection scheme ("http" or "https").
*   **`crawler`**:
    *   `dht_port` (integer): UDP port for the DHT node to listen on (default: 6881).
    *   `max_concurrent_downloads` (integer): Maximum number of metainfo download tasks to run concurrently.
    *   `max_peers_per_infohash` (integer): Maximum number of peers to fetch from the DHT for a single infohash when attempting to download its metadata.
*   **`logging`**:
    *   `level` (string): Logging level (e.g., "DEBUG", "INFO", "WARNING", "ERROR").
    *   `file_path` (string | null): Optional path to a log file. If `null` or omitted, logs will only go to the console. If a path is provided, logs go to both console and the file.

## Running the Crawler

Once configured, you can run the crawler from the project's root directory:

```bash
python main_crawler.py
```

The crawler will start, join the DHT network, discover infohashes, download metainfo, and store it in Elasticsearch. Log messages will be displayed on the console and, if configured, written to the log file.

To stop the crawler, press `Ctrl+C`. It will attempt a graceful shutdown.

## Running Tests

The project includes unit tests for various components. To run the tests, navigate to the project's root directory and execute:

```bash
python -m unittest discover tests
```

This command will automatically discover and run all tests within the `tests` directory. Make sure all dependencies, including those for testing (like `unittest.mock` which is standard), are available in your environment.
