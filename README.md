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
*   **可配置**: 关键操作参数通过分层的 JSON 配置文件进行配置 (详见“配置”部分)。
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
├── config_loader.py       # 处理分层配置加载的模块。
├── config/                  # 包含配置文件的目录。
│   ├── base.json            # 基础配置文件，包含所有默认设置。
│   ├── development.json     # 开发环境的特定配置。
│   └── production.json      # 生产环境的特定配置。
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
*   `config_loader.py`: 加载和合并配置文件的模块。
*   `config/`: 包含 JSON 配置文件的目录。
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
    首次运行爬虫前，您需要在 Elasticsearch 中创建目标索引并配置正确的映射。
    确保您的 Elasticsearch 实例正在运行且可访问。然后，在项目根目录中运行：
    ```bash
    python es_mapping.py
    ```
    请注意：如果您希望启用并获得最佳的中文全文检索功能，请确保在运行 `python es_mapping.py` 之前，您的 Elasticsearch 服务已安装并正确配置了所选的中文分词插件（如 IK Analyzer）。`es_mapping.py` 文件中定义的映射依赖此插件进行中文分词。
    此脚本将尝试连接到 Elasticsearch（默认连接 `http://localhost:9200`，除非在脚本中修改了该地址）并创建索引。索引名称由 `elasticsearch_config.index_name` 在您的配置文件中定义。

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

## 配置

应用程序的配置采用分层系统，由位于 `config/` 目录下的 JSON 文件组成。

*   `config/base.json`: 提供所有配置选项的默认值。
*   `config/development.json`: 包含开发环境特有的设置。这些设置会覆盖 `base.json` 中的相应值。
*   `config/production.json`: 包含生产环境特有的设置。这些设置同样会覆盖 `base.json` 中的相应值。

**环境设置**:
应用程序环境由 `APP_ENV` 环境变量决定。
*   有效值是 `development` 和 `production`。
*   如果 `APP_ENV` 未设置，则默认为 `development`。
*   如果 `APP_ENV` 设置为无法识别的值，它也会回退到 `development`。

**合并逻辑**:
加载配置时，应用程序首先加载 `config/base.json`。然后，根据 `APP_ENV` 的值，加载相应的环境特定配置文件（例如 `config/development.json`）。环境特定的配置会深度合并到基础配置中，环境特定文件中的值将优先于 `base.json` 中的值。

### `config/base.json` - 默认配置选项

以下是 `config/base.json` 中定义的所有可用配置选项及其默认值。您可以在特定环境的配置文件中覆盖这些选项。

#### `environment`
此键用于指示配置文件的来源，通常在基础或特定环境的 JSON 文件中设置，以帮助识别加载的配置。
*   `environment`: (字符串) 当前配置文件的环境标识符。
    *   默认 (在 `base.json` 中): `"base_default"`

#### `development_settings`
与开发模式相关的特定设置。
*   `print_metainfo_details`: (布尔值) 如果为 `true` 且环境为 `development`，则在成功下载元信息后，会将其解码并打印到日志中。
    *   默认: `false`
*   `store_in_es_dev_mode`: (布尔值) 如果为 `true` 且环境为 `development`，则元信息会尝试存储到 Elasticsearch（或根据 `elasticsearch_config.simulate_es_write` 进行模拟）。如果为 `false`，则在开发模式下跳过 ES 存储。
    *   默认: `false`

#### `dht_crawler_config`
DHT 爬虫模块的配置。
*   `bootstrap_nodes`: (列表的列表) 用于 DHT 网络引导的初始节点列表。每个内部列表包含节点 IP 地址和端口号。
    *   默认: `[["router.bittorrent.com", 6881], ["dht.transmissionbt.com", 6881], ["router.utorrent.com", 6881]]`
*   `k_size`: (整数) Kademlia k-bucket 大小。控制路由表中每个 k-bucket 可以存储的节点数。
    *   默认: `20`
*   `alpha`: (整数) Kademlia alpha 并发参数。控制并行发出的查找请求数。
    *   默认: `3`
*   `dht_port`: (整数) DHT 节点将监听传入连接的 UDP 端口。
    *   默认: `6881`
*   `node_id_source`: (字符串) 确定如何生成 DHT 节点 ID。可以是 `"random"`（每次运行时生成新的随机 ID）或 `"file"`（从指定文件加载 ID，如果文件不存在则生成并保存）。
    *   默认: `"random"`
*   `node_id_file_path`: (字符串) 如果 `node_id_source` 设置为 `"file"`，则此路径指定用于存储/加载节点 ID 的文件。
    *   默认: `"node_id.bin"`

#### `metainfo_downloader_config`
元信息下载器模块的配置。
*   `connect_timeout_seconds`: (整数) 尝试连接到对等节点以下载元信息时的超时时间（秒）。
    *   默认: `5`
*   `read_timeout_seconds`: (整数) 从对等节点读取数据（例如握手、消息）时的通用超时时间（秒）。
    *   默认: `10`
*   `piece_request_timeout_seconds`: (整数) 请求并等待元信息片段数据时的特定超时时间（秒）。
    *   默认: `15`
*   `max_metadata_size_mb`: (整数) 愿意下载的元信息的最大大小（以兆字节 MB 为单位）。超过此大小的元信息将被拒绝。
    *   默认: `5`
*   `max_concurrent_downloads`: (整数) 同时处理的 infohash（元信息下载任务）的最大数量。
    *   默认: `10`
*   `max_peers_per_infohash`: (整数) 对于单个 infohash，从 DHT 网络获取并尝试下载元信息的最大对等节点数。
    *   默认: `50`

#### `elasticsearch_config`
Elasticsearch 连接和索引的配置。
*   `host`: (字符串) Elasticsearch 服务器的主机名或 IP 地址。
    *   默认: `"localhost"`
*   `port`: (整数) Elasticsearch 服务器的端口号。
    *   默认: `9200`
*   `scheme`: (字符串) 连接到 Elasticsearch 使用的协议（例如 `"http"` 或 `"https"`）。
    *   默认: `"http"`
*   `index_name`: (字符串) 用于存储种子元信息的 Elasticsearch 索引的名称。
    *   默认 (在 `base.json` 中): `"torrent_metainfo_base"`
*   `connection_timeout_seconds`: (整数) 连接到 Elasticsearch 服务器的超时时间（秒）。
    *   默认: `10`
*   `simulate_es_write`: (布尔值) 如果为 `true`，则元信息不会实际写入 Elasticsearch，而只是记录将要执行的操作。这对于测试很有用，而无需修改 ES 数据。
    *   默认: `false`

#### `logging_config`
应用程序日志记录的配置。
*   `level`: (字符串) 最低日志级别（例如 `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`）。
    *   默认: `"INFO"`
*   `file_path`: (字符串 | null) 日志文件的可选路径。如果为 `null` 或省略，日志将仅输出到控制台。如果提供路径，日志将同时写入控制台和指定文件。
    *   默认 (在 `base.json` 中): `"dht_crawler_base.log"`

#### `main_crawler_config`
主爬虫协调逻辑的配置。
*   `metrics_log_interval_seconds`: (整数) 记录爬虫指标（例如，发现的 infohash 总数、下载成功/失败次数）的频率（秒）。
    *   默认: `60`
*   `client_name`: (字符串) 用于生成 peer_id 的客户端名称前缀（通常为2个字符）。
    *   默认: `"MyCr"`
*   `client_version`: (字符串) 用于生成 peer_id 的客户端版本号（通常为4个数字字符）。
    *   默认: `"0001"`
*   `shutdown_timeout_seconds`: (整数) 在收到关闭信号后，等待活动任务完成的超时时间（秒）。
    *   默认: `30`

### `config/development.json`
此文件用于覆盖开发环境的 `base.json` 中的设置。通常用于启用调试功能、更详细的日志记录或连接到开发服务。

**示例覆盖 (实际值在文件中定义):**
*   设置 `logging_config.level` 为 `"DEBUG"`。
*   更改 `elasticsearch_config.index_name` 以用于开发测试。
*   启用 `development_settings.print_metainfo_details`。
*   设置 `elasticsearch_config.simulate_es_write` 为 `true`。

### `config/production.json`
此文件用于覆盖生产环境的 `base.json` 中的设置。它应包含适用于实时部署的稳定、高性能的配置。

**示例覆盖 (实际值在文件中定义):**
*   确保 `logging_config.level` 设置为 `"INFO"` 或更高级别。
*   为生产 Elasticsearch 实例配置 `elasticsearch_config.host` 和 `elasticsearch_config.port`。
*   设置 `elasticsearch_config.simulate_es_write` 为 `false`。

## 运行爬虫

配置完成后，您可以从项目的根目录运行爬虫。使用 `APP_ENV` 环境变量来指定运行环境。

*   **开发模式** (如果 `APP_ENV` 未设置，则为默认模式):
    ```bash
    python main_crawler.py
    ```
    或者显式设置:
    ```bash
    APP_ENV=development python main_crawler.py
    ```

*   **生产模式**:
    ```bash
    APP_ENV=production python main_crawler.py
    ```

爬虫将会启动，加入 DHT 网络，发现 infohash，下载元信息，并将其存储到 Elasticsearch 中。日志消息将显示在控制台上，并且如果已配置，还会写入日志文件（日志文件的名称和级别取决于所选环境的配置）。

要停止爬虫，请按 `Ctrl+C`。它将尝试优雅地关闭。

## 运行测试

该项目包含各个组件的单元测试。要运行测试，请导航到项目的根目录并执行：

```bash
python -m unittest discover tests
```

此命令将自动发现并运行 `tests` 目录中的所有测试。请确保您的环境中已安装所有依赖项，包括用于测试的依赖项（如标准的 `unittest.mock`）。
