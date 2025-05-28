# DHT 元信息爬虫

## 概述

DHT 元信息爬虫是一个基于 Python 的应用程序，旨在爬取 BitTorrent 分布式哈希表 (DHT) 网络以发现 infohash。对于每个发现的 infohash，它会尝试使用 `ut_metadata` 扩展 (BEP 9) 从节点下载相应的种子元信息（`.torrent` 文件的内容）。最后，下载并经过验证的元信息会被解析并存储在 Elasticsearch 实例中，以供分析和搜索。

该项目使用 `asyncio` 进行并发网络操作，能够高效地同时处理多个 DHT 交互和下载任务。

## 功能特性

*   **DHT 爬取**: 加入 BitTorrent DHT 网络，监听 `announce_peer` 和 `get_peers` 消息以发现 infohash。
*   **Infohash 的节点发现**: 对于给定的 infohash，它会主动查询 DHT 网络以查找已为该 infohash 进行宣告 (announce) 的节点。
*   **元信息下载 (BEP 9)**: 连接到发现的节点，并尝试使用 `ut_metadata` 扩展（BEP 9，通过 BEP 10 实现）下载种子元数据。
*   **元数据验证**: 通过检查其 `info` 字典的 SHA1 哈希是否与原始 infohash 匹配来验证下载的元数据。
*   **Elasticsearch 存储**: 将解析后的种子元信息存储到配置的 Elasticsearch 索引中。文档 ID 即为 infohash。
*   **异步操作**: 使用 `asyncio` 构建，实现高并发和非阻塞 I/O。
*   **可配置**: 关键操作参数（Elasticsearch 连接、并发数、DHT 端口、日志记录）可通过 `config.json` 文件进行配置。
*   **结构化日志**: 提供可配置的日志记录到控制台和/或文件，并可选择不同的日志级别。
*   **单元测试**: 包含关键组件的单元测试套件。
*   **优雅关闭**: 处理 `SIGINT` 和 `SIGTERM` 信号以实现干净的关闭过程。

## 项目结构

```
.
├── main_crawler.py        # 运行爬虫的主要脚本，负责协调。
├── dht_crawler.py         # 处理 DHT 网络交互和 infohash 发现。
├── metainfo_downloader.py # 使用 BEP9 从节点下载种子元信息。
├── es_loader.py           # 解析元信息并将其存储在 Elasticsearch 中。
├── es_mapping.py          # 定义和创建 Elasticsearch 索引映射的脚本。
├── config.json            # 应用程序的配置文件。
├── requirements.txt       # Python 依赖项。
├── README.md              # 本文件。
└── tests/                   # 包含单元测试的目录。
    ├── test_dht_crawler.py
    ├── test_es_loader.py
    ├── test_main_crawler.py
    └── test_metainfo_downloader.py
```

*   `main_crawler.py`: 应用程序的主要入口点。它初始化并协调所有其他模块。
*   `dht_crawler.py`: 实现 DHT 节点逻辑，包括加入网络、发现 infohash 以及为给定 infohash 查找节点。
*   `metainfo_downloader.py`: 负责连接到节点并使用 `ut_metadata` 协议 (BEP 9) 获取种子元数据。
*   `es_loader.py`: 解析下载的 bencode 编码的元数据，并将其作为结构化 JSON 文档存储在 Elasticsearch 中。
*   `es_mapping.py`: 一个实用工具脚本，用于在首次运行爬虫之前在 Elasticsearch 中设置必要的索引和映射。
*   `config.json`: 用于配置爬虫各个方面的 JSON 文件。
*   `tests/`: 包含不同模块的单元测试。

## 先决条件

*   **Python**: 推荐使用 Python 3.8 或更高版本。
*   **Elasticsearch**: 一个正在运行的 Elasticsearch 实例（应兼容 7.x 或 8.x 版本，客户端已用适用于 ES 8.x/9.x 的 Elasticsearch Python 客户端测试过）。
*   **中文分词器 (Elasticsearch 插件)**: 为了获得最佳的中文内容搜索效果，建议在 Elasticsearch 中安装并配置中文分词插件（例如 [IK Analyzer](https://github.com/medcl/elasticsearch-analysis-ik)）。`es_mapping.py` 已配置为使用 `ik_smart` 分析器处理相关文本字段。
*   **Pip**: 用于安装 Python 包。

## 设置与安装

1.  **克隆仓库**:
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **创建 Elasticsearch 索引和映射**:
    首次运行爬虫前，您需要在 Elasticsearch 中创建 `torrent_metainfo` 索引并配置正确的映射。
    确保您的 Elasticsearch 实例正在运行且可访问。然后，在项目根目录中运行：
    ```bash
    python es_mapping.py
    ```
    请注意：如果您希望启用并获得最佳的中文全文检索功能，请确保在运行 `python es_mapping.py` 之前，您的 Elasticsearch 服务已安装并正确配置了所选的中文分词插件（如 IK Analyzer）。`es_mapping.py` 文件中定义的映射依赖此插件进行中文分词。
    此脚本将尝试连接到 Elasticsearch（默认连接 `http://localhost:9200`，除非在脚本中修改了该地址）并创建索引。

3.  **安装依赖**:
    建议使用虚拟环境：
    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows 系统请使用: venv\Scripts\activate
    ```
    安装所需的 Python 包：
    ```bash
    pip install -r requirements.txt
    ```

## 配置 (`config.json`)

`config.json` 文件允许您配置爬虫的行为。如果项目根目录中不存在此文件，请创建它。

**`config.json` 示例:**

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

**配置选项:**

*   **`elasticsearch`**:
    *   `host` (字符串): Elasticsearch 服务器的主机名或 IP 地址。
    *   `port` (整数): Elasticsearch 的端口号 (例如, 9200)。
    *   `scheme` (字符串): 连接协议 ("http" 或 "https")。
*   **`crawler`**:
    *   `dht_port` (整数): DHT 节点监听的 UDP 端口 (默认: 6881)。
    *   `max_concurrent_downloads` (整数): 并发运行的元信息下载任务的最大数量。
    *   `max_peers_per_infohash` (整数): 在尝试下载元数据时，为单个 infohash 从 DHT 获取的最大节点数。
*   **`logging`**:
    *   `level` (字符串): 日志级别 (例如, "DEBUG", "INFO", "WARNING", "ERROR")。
    *   `file_path` (字符串 | null): 可选的日志文件路径。如果为 `null` 或省略，日志将仅输出到控制台。如果提供了路径，日志将同时输出到控制台和文件。

## 运行爬虫

配置完成后，您可以从项目的根目录运行爬虫：

```bash
python main_crawler.py
```

爬虫将会启动，加入 DHT 网络，发现 infohash，下载元信息，并将其存储到 Elasticsearch 中。日志消息将显示在控制台上，并且如果已配置，还会写入日志文件。

要停止爬虫，请按 `Ctrl+C`。它将尝试优雅地关闭。

## 运行测试

该项目包含各个组件的单元测试。要运行测试，请导航到项目的根目录并执行：

```bash
python -m unittest discover tests
```

此命令将自动发现并运行 `tests` 目录中的所有测试。请确保您的环境中已安装所有依赖项，包括用于测试的依赖项（如标准的 `unittest.mock`）。
