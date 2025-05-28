```python
import json
import logging
import binascii
import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions # 从 elasticsearch 导入 Elasticsearch 和异常 (Import Elasticsearch and exceptions from elasticsearch)
import bencodepy

# 获取此模块的日志记录器 (Get logger for this module)
# 日志级别和处理器由主脚本 (main_crawler.py) 配置
# (Log level and handlers are configured by the main script (main_crawler.py))
logger = logging.getLogger(__name__)

# 默认配置文件路径 (Default configuration file path)
DEFAULT_CONFIG_PATH = 'config.json'
DEFAULT_ES_INDEX_NAME = 'torrent_metainfo'

def load_es_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict | None:
    """
    加载 Elasticsearch 配置。
    (Load Elasticsearch configuration.)
    Args:
        config_path (str): 配置文件的路径。 (Path to the configuration file.)
    Returns:
        dict | None: 包含 Elasticsearch 连接参数的字典，如果失败则返回 None。
                      (Dictionary containing Elasticsearch connection parameters, or None if failed.)
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f) 
            # logger.info(f"成功从 '{config_path}' 加载配置。 (Successfully loaded configuration from '{config_path}'.)") # 由主配置加载器处理 (Handled by main config loader)
            return config.get("elasticsearch") 
    except FileNotFoundError:
        logger.error(f"配置文件 '{config_path}' 未找到。 (Configuration file '{config_path}' not found.)")
        return None
    except json.JSONDecodeError:
        logger.error(f"解析配置文件 '{config_path}' 时出错。请检查 JSON 格式是否正确。 "
                     f"(Error parsing configuration file '{config_path}'. Please check if the JSON format is correct.)")
        return None
    except Exception as e:
        logger.error(f"加载配置文件 '{config_path}' 时发生未知错误: {e} "
                     f"(An unknown error occurred while loading configuration file '{config_path}': {e})")
        return None

class ElasticsearchLoader:
    """
    一个用于将种子元信息存储到 Elasticsearch 中的类。
    (A class for storing torrent metainfo into Elasticsearch.)
    """
    def __init__(self, es_config: dict | None = None):
        """
        初始化 ElasticsearchLoader。
        (Initialize ElasticsearchLoader.)
        Args:
            es_config (dict | None): Elasticsearch 连接配置。如果为 None，则尝试从默认路径加载。
                                     (Elasticsearch connection configuration. If None, tries to load from default path.)
        """
        if es_config is None:
            # 尝试加载 Elasticsearch 特定配置 (Try to load Elasticsearch specific configuration)
            # 注意：主爬虫现在处理组合的配置加载 (Note: main crawler now handles combined config loading)
            # 这个函数可能不会被直接调用，除非独立使用 es_loader
            # (This function might not be called directly unless es_loader is used standalone)
            es_cfg_loaded = load_es_config() 
            if not es_cfg_loaded: # 如果特定加载失败 (If specific loading fails)
                 raise ValueError("Elasticsearch 配置未提供且无法从默认路径加载。 "
                                  "(Elasticsearch configuration not provided and failed to load from default path.)")
            current_es_config = es_cfg_loaded
        else:
            current_es_config = es_config


        if not current_es_config or not all(k in current_es_config for k in ["host", "port"]):
            logger.error("Elasticsearch 配置无效或不完整。请提供主机和端口。 "
                         "(Elasticsearch configuration is invalid or incomplete. Please provide host and port.)")
            raise ValueError("Elasticsearch 配置无效或不完整。 (Elasticsearch configuration is invalid or incomplete.)")

        es_scheme = current_es_config.get("scheme", "http") 
        es_host = current_es_config["host"]
        es_port = current_es_config["port"]
        
        self.connection_string = f"{es_scheme}://{es_host}:{es_port}" # 存储连接字符串以供日志记录 (Store connection string for logging)
        try:
            # 根据 Elasticsearch 客户端版本，ping 方法可能是异步的
            # (Depending on Elasticsearch client version, ping method might be async)
            # 当前实现假设它是同步的，或者在异步上下文中正确处理
            # (Current implementation assumes it's synchronous or handled correctly in async context)
            # 对于 >=8.x, es.ping() 是同步的，但如果与 async loop 一起使用，则应使用 await es.ping()
            # (For >=8.x, es.ping() is synchronous, but if used with an async loop, await es.ping() should be used)
            # 在 main_crawler.py 中，我们 await es.ping()
            # (In main_crawler.py, we await es.ping())
            self.es = Elasticsearch([self.connection_string], timeout=10) 
            logger.info(f"ElasticsearchLoader 初始化，目标: {self.connection_string} "
                        f"(ElasticsearchLoader initialized, targeting: {self.connection_string})")
            # Ping 操作移至主爬虫的初始化逻辑中 (Ping operation moved to main crawler's init logic)
        except es_exceptions.ConnectionError as e:
            logger.error(f"连接到 Elasticsearch ({self.connection_string}) 失败: {e} "
                         f"(Failed to connect to Elasticsearch ({self.connection_string}): {e})")
            raise
        except Exception as e:
            logger.error(f"初始化 Elasticsearch 客户端时发生未知错误: {e} "
                         f"(An unknown error occurred while initializing Elasticsearch client: {e})", exc_info=True) # 添加 exc_info (Add exc_info)
            raise


    def _parse_metainfo(self, metainfo_bytes: bytes, infohash_for_log: str = "N/A") -> dict | None:
        """
        解析 bencoded 元信息。
        (Parse bencoded metainfo.)
        Args:
            metainfo_bytes (bytes): bencoded 元信息字节串。 (Bencoded metainfo byte string.)
            infohash_for_log (str): 用于日志记录的 infohash。 (Infohash for logging.)
        Returns:
            dict | None: 解析后的元信息字典，如果失败则返回 None。
                          (Parsed metainfo dictionary, or None if failed.)
        """
        try:
            parsed_data = bencodepy.decode(metainfo_bytes)
            if not isinstance(parsed_data, dict):
                logger.error(f"[{infohash_for_log}] 元信息顶层不是一个字典。实际类型: {type(parsed_data)} "
                             f"(Metainfo top level is not a dictionary for {infohash_for_log}. Actual type: {type(parsed_data)})")
                return None
            logger.debug(f"[{infohash_for_log}] 元信息成功解析。 (Metainfo parsed successfully for {infohash_for_log}.)")
            return parsed_data
        except bencodepy.BencodeDecodeError as e:
            logger.error(f"[{infohash_for_log}] 解析 bencoded 元信息失败: {e} "
                         f"(Failed to parse bencoded metainfo for {infohash_for_log}: {e})")
            return None
        except Exception as e:
            logger.error(f"[{infohash_for_log}] 解析元信息时发生未知错误: {e} "
                         f"(An unknown error occurred while parsing metainfo for {infohash_for_log}: {e})", exc_info=True)
            return None

    def _prepare_document(self, infohash_hex: str, parsed_metainfo: dict) -> dict | None:
        """
        从解析后的元信息中提取数据并准备 Elasticsearch 文档。
        (Extract data from parsed metainfo and prepare Elasticsearch document.)
        Args:
            infohash_hex (str): Infohash 的十六进制字符串。 (Hex string of the Infohash.)
            parsed_metainfo (dict): 解析后的元信息字典。 (Parsed metainfo dictionary.)
        Returns:
            dict | None: 用于 Elasticsearch 的文档字典，如果失败则返回 None。
                          (Document dictionary for Elasticsearch, or None if failed.)
        """
        try:
            info_dict = parsed_metainfo.get(b'info')
            if not isinstance(info_dict, dict):
                logger.error(f"[{infohash_hex}] 元信息中缺少 'info' 字典或其类型不正确。 "
                             f"('info' dictionary missing or incorrect type in metainfo for {infohash_hex}.)")
                return None

            doc = {"infohash": infohash_hex} 

            doc['name'] = info_dict.get(b'name', b'').decode('utf-8', 'ignore')

            if b'files' in info_dict: 
                doc['files'] = []
                total_length = 0
                for file_item in info_dict.get(b'files', []):
                    if isinstance(file_item, dict):
                        path_parts = [part.decode('utf-8', 'ignore') for part in file_item.get(b'path', [])]
                        file_path = '/'.join(path_parts) if path_parts else ''
                        length = file_item.get(b'length')
                        if isinstance(length, int):
                            total_length += length
                            doc['files'].append({'path': file_path, 'length': length})
                doc['length'] = total_length 
            elif b'length' in info_dict: 
                length = info_dict.get(b'length')
                if isinstance(length, int):
                    doc['length'] = length
            
            if b'creation date' in parsed_metainfo:
                creation_timestamp = parsed_metainfo.get(b'creation date')
                if isinstance(creation_timestamp, int):
                    if creation_timestamp >= 0: # Check for non-negative
                        try:
                            doc['creation_date'] = datetime.datetime.fromtimestamp(creation_timestamp, tz=datetime.timezone.utc).isoformat()
                        except OverflowError:
                             logger.warning(f"[{infohash_hex}] 'creation date' 时间戳 {creation_timestamp} 过大无法转换。")
                        except OSError: # e.g. on Windows, negative timestamps raise OSError
                             logger.warning(f"[{infohash_hex}] 'creation date' 时间戳 {creation_timestamp} 无效 (例如为负数)。")
                    else:
                        logger.warning(f"[{infohash_hex}] 'creation date' 时间戳 {creation_timestamp} 为负数，已忽略。")

            doc['encoding'] = parsed_metainfo.get(b'encoding', b'').decode('utf-8', 'ignore')
            
            announce_list_raw = parsed_metainfo.get(b'announce-list')
            if isinstance(announce_list_raw, list):
                doc['announce_list'] = [] 
                for tier in announce_list_raw:
                    if isinstance(tier, list):
                        for url_bytes in tier:
                            if isinstance(url_bytes, bytes):
                                doc['announce_list'].append({'url': url_bytes.decode('utf-8', 'ignore')})
            
            source_val = info_dict.get(b'source')
            if isinstance(source_val, bytes):
                 doc['source'] = source_val.decode('utf-8', 'ignore')

            logger.debug(f"[{infohash_hex}] 准备好的文档: {doc} (Prepared document for {infohash_hex}: {doc})")
            return doc
        except Exception as e:
            logger.error(f"[{infohash_hex}] 准备文档时出错: {e} "
                         f"(Error preparing document for {infohash_hex}: {e})", exc_info=True)
            return None

    def store_metainfo(self, infohash_input: str | bytes, metainfo_bytes: bytes) -> bool:
        """
        存储单个种子元信息到 Elasticsearch。
        (Store a single torrent metainfo into Elasticsearch.)
        Args:
            infohash_input (str | bytes): Infohash，可以是十六进制字符串或字节串。
                                          (Infohash, can be hex string or byte string.)
            metainfo_bytes (bytes): 原始 bencoded 元信息字节串。
                                    (Raw bencoded metainfo byte string.)
        Returns:
            bool: 如果存储成功则返回 True，否则返回 False。
                  (Returns True if storage was successful, False otherwise.)
        """
        if isinstance(infohash_input, bytes):
            infohash_hex = binascii.hexlify(infohash_input).decode('utf-8')
        elif isinstance(infohash_input, str):
            infohash_hex = infohash_input.lower() 
        else:
            logger.error(f"无效的 infohash 类型: {type(infohash_input)}。需要 str 或 bytes。 "
                         f"(Invalid infohash type: {type(infohash_input)}. Expected str or bytes.)")
            return False

        if len(infohash_hex) != 40:
            logger.error(f"无效的 infohash 长度: {len(infohash_hex)}。需要 40 个字符的十六进制字符串。 "
                         f"(Invalid infohash length: {len(infohash_hex)}. Expected 40-character hex string.)")
            return False

        logger.debug(f"[{infohash_hex}] 开始存储元信息... (Starting to store metainfo for {infohash_hex}...)")
        parsed_metainfo = self._parse_metainfo(metainfo_bytes, infohash_hex)
        if not parsed_metainfo:
            logger.error(f"[{infohash_hex}] 无法解析元信息，存储中止。 (Failed to parse metainfo for {infohash_hex}, storage aborted.)")
            return False

        document_to_store = self._prepare_document(infohash_hex, parsed_metainfo)
        if not document_to_store:
            logger.error(f"[{infohash_hex}] 无法准备文档，存储中止。 (Failed to prepare document for {infohash_hex}, storage aborted.)")
            return False

        try:
            response = self.es.index(
                index=DEFAULT_ES_INDEX_NAME,
                id=infohash_hex,
                document=document_to_store
            )
            # 记录更详细的成功信息 (Log more detailed success information)
            logger.info(f"[{infohash_hex}] 成功为 infohash 建立索引。ES 响应: {{'result': '{response.get('result')}', '_id': '{response.get('_id')}', '_version': {response.get('_version')}}} "
                        f"(Successfully indexed infohash {infohash_hex}. ES Response: {{'result': '{response.get('result')}', '_id': '{response.get('_id')}', '_version': {response.get('_version')}}})")
            return True
        except es_exceptions.ConnectionError as e:
            logger.error(f"[{infohash_hex}] 存储时连接到 Elasticsearch ({self.connection_string}) 失败: {e} "
                         f"(Failed to connect to Elasticsearch ({self.connection_string}) while storing {infohash_hex}: {e})")
        except es_exceptions.TransportError as e:
            logger.error(f"[{infohash_hex}] 存储时发生 Elasticsearch 传输错误: {e} (status: {e.status_code}) "
                         f"(Elasticsearch transport error occurred while storing {infohash_hex}: {e} (status: {e.status_code}))")
            if e.status_code == 404: 
                logger.error(f"[{infohash_hex}] 错误详情: 索引 '{DEFAULT_ES_INDEX_NAME}' 可能不存在。 "
                             f"(Error details for {infohash_hex}: Index '{DEFAULT_ES_INDEX_NAME}' might not exist.)")
            elif e.status_code == 400: 
                 logger.error(f"[{infohash_hex}] 错误详情: 文档可能与索引 '{DEFAULT_ES_INDEX_NAME}' 的映射不匹配。错误: {e.info} "
                              f"(Error details for {infohash_hex}: Document might not match mapping of index '{DEFAULT_ES_INDEX_NAME}'. Error: {e.info})")
        except es_exceptions.ElasticsearchException as e:
            logger.error(f"[{infohash_hex}] 存储时发生 Elasticsearch 错误: {e} "
                         f"(Elasticsearch error occurred while storing {infohash_hex}: {e})", exc_info=True)
        except Exception as e:
            logger.error(f"[{infohash_hex}] 存储时发生未知错误: {e} "
                         f"(An unknown error occurred while storing {infohash_hex}: {e})", exc_info=True)
        
        return False


if __name__ == '__main__':
    # 这是一个示例用法，通常这个模块会被其他下载器或爬虫脚本导入和使用
    # (This is an example usage, typically this module would be imported and used by other downloaders or crawler scripts)
    
    # 为独立运行配置基本日志记录 (Configure basic logging for standalone run)
    # 这不会影响由 main_crawler.py 进行的全局日志配置
    # (This will not affect global logging configuration done by main_crawler.py)
    if not logging.getLogger().hasHandlers(): # 检查是否已配置处理器 (Check if handlers are already configured)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("es_loader.py 模块示例运行开始。 (es_loader.py module example run started.)")

    es_loader_instance = None # 在 try 块外声明 (Declare outside try block)
    try:
        # 尝试使用默认配置文件路径加载 (Try loading with default config path)
        es_loader_instance = ElasticsearchLoader() 
    except Exception as e_init:
        logger.error(f"无法初始化 ElasticsearchLoader: {e_init} (Failed to initialize ElasticsearchLoader: {e_init})")
        exit(1) 

    # 检查 Elasticsearch 连接 (Check Elasticsearch connection)
    # 注意：在 Elasticsearch 8.x+ 中，ping() 是同步的。如果 es_loader 在异步环境中使用，
    # 则 ping() 可能需要 await。但 ElasticsearchLoader 本身是同步类。
    # (Note: In Elasticsearch 8.x+, ping() is synchronous. If es_loader is used in an async context,
    # ping() might need await. However, ElasticsearchLoader itself is a synchronous class.)
    # main_crawler.py 中的 ping 是 await es_loader.es.ping()
    # (The ping in main_crawler.py is await es_loader.es.ping())
    # 这里，我们直接调用，因为它在 __main__ 的同步上下文中
    # (Here, we call it directly as it's in a synchronous context of __main__)
    try:
        if not es_loader_instance.es.ping():
            logger.error("无法 ping通 Elasticsearch。请检查配置和 Elasticsearch 服务器状态。 "
                         "(Failed to ping Elasticsearch. Please check configuration and Elasticsearch server status.)")
            exit(1) 
        logger.info("成功 ping通 Elasticsearch 服务器。 (Successfully pinged Elasticsearch server.)")
    except Exception as e_ping:
         logger.error(f"Ping Elasticsearch 时出错: {e_ping} (Error pinging Elasticsearch: {e_ping})")
         exit(1)


    sample_info_dict = {
        b'name': b'My Sample Torrent',
        b'piece length': 262144, 
        b'pieces': b'01234567890123456789', 
        b'length': 1234567 
    }
    sample_metainfo_bencoded = {
        b'info': sample_info_dict,
        b'announce': b'udp://tracker.example.com:80',
        b'creation date': int(datetime.datetime.now().timestamp()), 
        b'comment': b'This is a sample torrent for testing purposes.',
        b'created by': b'MyTestClient/1.0',
        b'encoding': b'UTF-8',
        b'announce-list': [[b'udp://tracker.example.com:80'], [b'http://tracker.example.org/announce']]
    }

    try:
        metainfo_bytes_content = bencodepy.encode(sample_metainfo_bencoded)
    except Exception as e:
        logger.error(f"编码示例元信息失败: {e} (Failed to encode sample metainfo: {e})")
        exit(1)

    import hashlib
    info_bencoded_for_hash = bencodepy.encode(sample_info_dict)
    example_infohash_bytes = hashlib.sha1(info_bencoded_for_hash).digest()
    example_infohash_hex = binascii.hexlify(example_infohash_bytes).decode('utf-8')
    
    logger.info(f"生成的示例 infohash: {example_infohash_hex} (Generated sample infohash: {example_infohash_hex})")

    success = es_loader_instance.store_metainfo(example_infohash_hex, metainfo_bytes_content)

    if success:
        logger.info(f"示例元信息 (infohash: {example_infohash_hex}) 已成功存储。 "
                    f"(Sample metainfo (infohash: {example_infohash_hex}) stored successfully.)")
    else:
        logger.error(f"存储示例元信息 (infohash: {example_infohash_hex}) 失败。 "
                     f"(Failed to store sample metainfo (infohash: {example_infohash_hex}).)")

    logger.info("es_loader.py 模块示例运行结束。 (es_loader.py module example run finished.)")

```
