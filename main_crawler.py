import asyncio
import signal
import logging
import hashlib
import os # 用于生成 peer_id (For generating peer_id)
import binascii # 用于 infohash 的十六进制转换 (For hex conversion of infohash)
import json # 用于加载配置 (For loading configuration)
import time # 用于定期日志记录 (For periodic logging)

from dht_crawler import DHTCrawler
from metainfo_downloader import MetainfoDownloader
from es_loader import ElasticsearchLoader, load_es_config as load_es_config_from_loader, DEFAULT_CONFIG_PATH

# --- 日志配置开始 (Logging Configuration Start) ---
# 默认日志格式 (Default log format)
DEFAULT_LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
# 获取根日志记录器 (Get the root logger)
# 注意：在这里获取 logger 实例，以便在配置函数之前就可以使用它
# (Note: Get the logger instance here so it can be used even before the config function)
logger = logging.getLogger(__name__) # 获取此模块的日志记录器 (Get logger for this module)

# 指标计数器 (Metric Counters)
metrics = {
    "infohashes_discovered_total": 0,
    "metainfo_downloaded_success_total": 0,
    "metainfo_downloaded_failure_total": 0,
    "metainfo_stored_es_total": 0,
    "active_download_tasks": 0 # 当前活动的下载任务数 (Current number of active download tasks)
}
# 上次记录指标的时间 (Time when metrics were last logged)
last_metrics_log_time = time.monotonic()
# 指标记录间隔（秒）(Metrics logging interval in seconds)
METRICS_LOG_INTERVAL = 60 


def setup_logging(log_level_str: str = "INFO", log_file: str | None = None, log_format: str = DEFAULT_LOG_FORMAT):
    """
    配置根日志记录器。
    (Configure the root logger.)
    Args:
        log_level_str (str): 日志级别字符串 (e.g., "DEBUG", "INFO").
                             (Log level string (e.g., "DEBUG", "INFO").)
        log_file (str | None): 可选的日志文件路径。如果提供，日志将写入此文件。
                               (Optional path to a log file. If provided, logs will be written to this file.)
        log_format (str): 日志消息的格式。
                          (Format for log messages.)
    """
    numeric_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    root_logger = logging.getLogger() 
    root_logger.setLevel(numeric_level) 
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(log_format)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler) 
    # 使用模块级 logger 记录初始配置信息 (Use module-level logger for initial config messages)
    # 避免在 setup_logging 本身中使用 root_logger.info，因为它可能尚未完全配置
    # (Avoid using root_logger.info within setup_logging itself as it might not be fully configured yet)
    initial_config_logger = logging.getLogger(__name__)
    initial_config_logger.info(f"控制台日志级别设置为: {log_level_str} (Console logging level set to: {log_level_str})")

    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8') 
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler) 
            initial_config_logger.info(f"文件日志级别设置为: {log_level_str}, 日志文件: {log_file} "
                        f"(File logging level set to: {log_level_str}, log file: {log_file})")
        except Exception as e:
            initial_config_logger.error(f"设置文件日志处理器 '{log_file}' 失败: {e} "
                         f"(Failed to set up file log handler '{log_file}': {e})")
# --- 日志配置结束 (Logging Configuration End) ---


# 全局变量，用于信号处理 (Global variable for signal handling)
shutdown_event = asyncio.Event() 

# 客户端标识和版本 (Client identifier and version)
CLIENT_NAME = "MyCr" 
CLIENT_VERSION = "0001" 

def generate_peer_id() -> bytes:
    """
    生成一个符合 BitTorrent 规范的 peer_id。
    (Generate a peer_id compliant with BitTorrent specifications.)
    Returns:
        bytes: 20 字节的 peer_id。 (20-byte peer_id.)
    """
    client_prefix = f"-{CLIENT_NAME[:2].upper()}{CLIENT_VERSION}-" 
    random_bytes_len = 20 - len(client_prefix)
    random_bytes = os.urandom(random_bytes_len) 
    peer_id = client_prefix.encode('ascii') + random_bytes 
    logger.info(f"生成的 peer_id (Latin-1): {peer_id.decode('latin-1')} (十六进制: {binascii.hexlify(peer_id).decode()}) "
                f"(Generated peer_id (Latin-1): {peer_id.decode('latin-1')} (hex: {binascii.hexlify(peer_id).decode()}))")
    return peer_id

def load_main_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict: # 重命名以区分 (Renamed for clarity)
    """
    加载主配置文件，包含爬虫和日志配置。
    (Load main configuration file, containing crawler and logging configurations.)
    Args:
        config_path (str): 配置文件的路径。 (Path to the configuration file.)
    Returns:
        dict: 包含所有配置的字典。 (Dictionary containing all configurations.)
    """
    default_cfg = {
        "crawler": {
            "max_concurrent_downloads": 5,
            "max_peers_per_infohash": 30,
            "dht_port": 6881
        },
        "logging": {
            "level": "INFO",
            "file_path": None
        },
        "elasticsearch": {
            "host": "localhost",
            "port": 9200,
            "scheme": "http",
            "simulate_es_write": False
        }
    }
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
            user_crawler_cfg = config.get("crawler", {})
            user_logging_cfg = config.get("logging", {})
            user_es_cfg = config.get("elasticsearch", {})

            if not isinstance(user_crawler_cfg, dict):
                logger.warning("配置中的 'crawler' 部分不是一个字典对象，将仅使用默认爬虫配置。 (Configuration section 'crawler' is not a dictionary, using default crawler configurations only.)")
                user_crawler_cfg = {}
            if not isinstance(user_logging_cfg, dict):
                logger.warning("配置中的 'logging' 部分不是一个字典对象，将仅使用默认日志配置。 (Configuration section 'logging' is not a dictionary, using default logging configurations only.)")
                user_logging_cfg = {}
            if not isinstance(user_es_cfg, dict):
                logger.warning("配置中的 'elasticsearch' 部分不是一个字典对象，将仅使用默认 ES 配置。 (Configuration section 'elasticsearch' is not a dictionary, using default ES configurations only.)")
                user_es_cfg = {}

            crawler_cfg = {**default_cfg["crawler"], **user_crawler_cfg}
            logging_cfg = {**default_cfg["logging"], **user_logging_cfg}
            es_cfg = {**default_cfg["elasticsearch"], **user_es_cfg} # Merge defaults with user config
            
            final_config = {"crawler": crawler_cfg, "logging": logging_cfg, "elasticsearch": es_cfg}
            logger.info(f"成功从 '{config_path}' 加载配置: {final_config} "
                        f"(Successfully loaded configuration from '{config_path}': {final_config})")
            return final_config
    except FileNotFoundError:
        logger.warning(f"配置文件 '{config_path}' 未找到。使用完整的默认配置。 "
                       f"(Configuration file '{config_path}' not found. Using full default configurations.)")
    except json.JSONDecodeError:
        logger.warning(f"解析配置文件 '{config_path}' 时出错。使用完整的默认配置。 "
                       f"(Error parsing configuration file '{config_path}'. Using full default configurations.)")
    except Exception as e:
        logger.error(f"加载配置文件 '{config_path}' 时发生未知错误: {e}。使用完整的默认配置。 "
                     f"(An unknown error occurred while loading configuration file '{config_path}': {e}. Using full default configurations.)")
    # If any error occurs or file not found, return the comprehensive default_cfg
    logger.info(f"返回默认配置: {default_cfg} (Returning default configuration: {default_cfg})")
    return default_cfg


async def log_metrics_periodically():
    """
    定期记录收集的指标。
    (Periodically log collected metrics.)
    """
    global last_metrics_log_time
    while not shutdown_event.is_set(): 
        await asyncio.sleep(METRICS_LOG_INTERVAL) 
        if shutdown_event.is_set():
            break
        
        current_time = time.monotonic()
        time_elapsed = current_time - last_metrics_log_time
        
        current_metrics_snapshot = metrics.copy()
        # 添加路由表大小的特殊处理 (Add special handling for routing table size)
        # 这假设 dht_crawler 实例在全局范围内可用，或者通过参数传递
        # (This assumes dht_crawler instance is available globally or passed as an argument)
        # 为了简单起见，我们暂时不直接从这里访问 dht_crawler
        # (For simplicity, we won't access dht_crawler directly from here for now)
        metrics_log_message = ", ".join(f"{key}={value}" for key, value in current_metrics_snapshot.items())
        
        logger.info(f"爬虫指标 (过去 {time_elapsed:.2f} 秒): {metrics_log_message} "
                    f"(Crawler Metrics (last {time_elapsed:.2f}s): {metrics_log_message})")
        last_metrics_log_time = current_time 

async def process_infohash_task(
    infohash_bytes: bytes,
    dht_crawler_instance: DHTCrawler, # 明确传递 DHTCrawler 实例 (Explicitly pass DHTCrawler instance)
    downloader_peer_id: bytes, 
    es_loader_instance: ElasticsearchLoader, # 明确传递 ESLoader 实例 (Explicitly pass ESLoader instance)
    semaphore: asyncio.Semaphore,
    max_peers: int
):
    """
    处理单个 infohash 的异步任务。
    (Asynchronous task to process a single infohash.)
    """
    global metrics 
    infohash_hex = binascii.hexlify(infohash_bytes).decode('utf-8') 
    metainfo_downloader_session = MetainfoDownloader(infohash=infohash_bytes, peer_id=downloader_peer_id)

    async with semaphore: 
        metrics["active_download_tasks"] += 1 
        try:
            if shutdown_event.is_set(): 
                logger.info(f"关闭事件已设置，跳过处理 infohash: {infohash_hex} "
                            f"(Shutdown event is set, skipping processing for infohash: {infohash_hex})")
                return

            logger.info(f"开始处理 infohash: {infohash_hex} (当前活动任务: {metrics['active_download_tasks']}) "
                        f"(Starting processing for infohash: {infohash_hex} (current active tasks: {metrics['active_download_tasks']}))")
            
            logger.info(f"[{infohash_hex}] 正在发现对等节点... (Discovering peers...)") # 修改日志级别 (Changed log level)
            peers = await dht_crawler_instance.get_peers_for_infohash(infohash_bytes, max_peers)
            if not peers:
                logger.info(f"[{infohash_hex}] 未找到对等节点。 (No peers found.)")
                metrics["metainfo_downloaded_failure_total"] += 1 
                return
            logger.info(f"[{infohash_hex}] 找到 {len(peers)} 个对等节点。 (Found {len(peers)} peers.)")

            logger.info(f"[{infohash_hex}] 正在下载元信息... (Downloading metainfo...)")
            metadata_bytes = await metainfo_downloader_session.download(peers) 

            if not metadata_bytes:
                logger.info(f"[{infohash_hex}] 元信息下载失败。 (Failed to download metainfo.)")
                metrics["metainfo_downloaded_failure_total"] += 1 
                return
            logger.info(f"[{infohash_hex}] 元信息下载成功，大小: {len(metadata_bytes)} 字节。 "
                        f"(Metainfo downloaded successfully, size: {len(metadata_bytes)} bytes.)")
            metrics["metainfo_downloaded_success_total"] += 1 
            
            logger.info(f"[{infohash_hex}] 正在存储元信息到 Elasticsearch... (Storing metainfo to Elasticsearch...)")
            stored_successfully = es_loader_instance.store_metainfo(infohash_bytes, metadata_bytes) 
            if stored_successfully:
                logger.info(f"[{infohash_hex}] 元信息成功存储到 Elasticsearch。 (Metainfo successfully stored in Elasticsearch.)")
                metrics["metainfo_stored_es_total"] += 1 
            else:
                logger.warning(f"[{infohash_hex}] 元信息存储到 Elasticsearch 失败。 (Failed to store metainfo in Elasticsearch.)")

        except Exception as e:
            logger.error(f"处理 infohash {infohash_hex} 时发生错误: {e} "
                         f"(Error processing infohash {infohash_hex}: {e})", exc_info=True)
            metrics["metainfo_downloaded_failure_total"] += 1 
        finally:
            metrics["active_download_tasks"] -= 1 
            logger.info(f"完成处理 infohash: {infohash_hex} (当前活动任务: {metrics['active_download_tasks']}) "
                         f"(Finished processing for infohash: {infohash_hex} (current active tasks: {metrics['active_download_tasks']}))") # 修改日志级别 (Changed log level)


async def main_crawler_loop(
    dht_crawler_instance: DHTCrawler, # 明确传递实例 (Explicitly pass instance)
    downloader_peer_id: bytes, 
    es_loader_instance: ElasticsearchLoader, # 明确传递实例 (Explicitly pass instance)
    semaphore: asyncio.Semaphore,
    max_peers: int
):
    """
    爬虫的主循环。
    (Main loop for the crawler.)
    """
    global metrics 
    logger.info("爬虫主循环已启动。等待来自 DHT 爬虫的 infohashes... "
                "(Crawler main loop started. Waiting for infohashes from DHT crawler...)")
    pending_tasks = set() 

    while not shutdown_event.is_set(): 
        try:
            infohash_bytes = await asyncio.wait_for(dht_crawler_instance.infohash_queue.get(), timeout=1.0)
            metrics["infohashes_discovered_total"] += 1 
            
            task = asyncio.create_task(
                process_infohash_task(
                    infohash_bytes,
                    dht_crawler_instance, 
                    downloader_peer_id, 
                    es_loader_instance, 
                    semaphore,
                    max_peers
                )
            )
            pending_tasks.add(task) 
            task.add_done_callback(pending_tasks.discard) 

            dht_crawler_instance.infohash_queue.task_done() 
            logger.debug(f"当前待处理任务数: {len(pending_tasks)}, 活动下载: {metrics['active_download_tasks']} "
                         f"(Current number of pending tasks: {len(pending_tasks)}, active downloads: {metrics['active_download_tasks']})")

        except asyncio.TimeoutError:
            continue 
        except Exception as e:
            logger.error(f"主循环遇到错误: {e} (Main loop encountered an error: {e})", exc_info=True)
            await asyncio.sleep(1) 
    
    logger.info(f"关闭事件已设置。等待 {len(pending_tasks)} 个待处理任务完成... "
                f"(Shutdown event set. Waiting for {len(pending_tasks)} pending tasks to complete...)")
    if pending_tasks:
        done, still_pending = await asyncio.wait(pending_tasks, timeout=30.0) 
        if still_pending:
            logger.warning(f"{len(still_pending)} 个任务在超时后仍未完成。将尝试取消它们。 "
                           f"({len(still_pending)} tasks still pending after timeout. Attempting to cancel them.)")
            for task_to_cancel in still_pending:
                task_to_cancel.cancel() 
            await asyncio.gather(*still_pending, return_exceptions=True) 
    logger.info("所有待处理任务已处理完毕。 (All pending tasks have been processed.)")


def handle_shutdown_signal(sig, loop):
    """
    处理关闭信号。
    (Handle shutdown signals.)
    """
    logger.info(f"收到信号 {sig}。正在启动优雅关闭... (Received signal {sig}. Initiating graceful shutdown...)")
    shutdown_event.set() 

async def main():
    """
    爬虫应用程序的主入口点。
    (Main entry point for the crawler application.)
    """
    # 加载主配置 (Load main configuration)
    main_config = load_main_config() 
    crawler_cfg = main_config["crawler"]
    logging_cfg = main_config["logging"]
    es_cfg = main_config["elasticsearch"] # 从主配置获取 ES 配置 (Get ES config from main config)

    setup_logging(log_level_str=logging_cfg.get("level", "INFO"), 
                  log_file=logging_cfg.get("file_path"),
                  log_format=DEFAULT_LOG_FORMAT)

    if not es_cfg or not es_cfg.get("host"): # 检查 ES 配置是否有效 (Check if ES config is valid)
        logger.critical("Elasticsearch 配置无效或缺失。爬虫无法启动。 "
                        "(Elasticsearch configuration is invalid or missing. Crawler cannot start.)")
        return

    client_downloader_peer_id = generate_peer_id()
    infohash_discovered_queue = asyncio.Queue() 
    dht_node_id = hashlib.sha1(os.urandom(20)).digest() 

    dht_crawler = DHTCrawler(
        infohash_queue=infohash_discovered_queue,
        node_id=dht_node_id 
    )
    
    es_loader = None # 在 try 块外声明 (Declare outside try block)
    try:
        es_loader = ElasticsearchLoader(es_config=es_cfg) # 使用从主配置加载的 es_cfg (Use es_cfg loaded from main config)
        if not await es_loader.es.ping(): 
             logger.critical("无法 ping通 Elasticsearch 服务器。请检查连接和服务器状态。爬虫将退出。 "
                            "(Failed to ping Elasticsearch server. Please check connection and server status. Crawler will exit.)")
             return
    except Exception as e_init_es:
        logger.critical(f"初始化 ElasticsearchLoader 失败: {e_init_es}。爬虫无法启动。 "
                        f"(Failed to initialize ElasticsearchLoader: {e_init_es}. Crawler cannot start.)")
        return

    semaphore = asyncio.Semaphore(crawler_cfg["max_concurrent_downloads"])
    loop = asyncio.get_running_loop() 
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, handle_shutdown_signal, sig_name, loop)

    metrics_task = asyncio.create_task(log_metrics_periodically())
    # 将 DHTCrawler 实例传递给指标日志记录任务，以便它可以访问路由表大小
    # (Pass DHTCrawler instance to metrics logging task so it can access routing table size)
    # 这需要修改 log_metrics_periodically 以接受 dht_crawler 作为参数
    # (This requires modifying log_metrics_periodically to accept dht_crawler as an argument)
    # 为了简单起见，暂时不这样做，但这是一个可以改进的地方
    # (For simplicity, not doing this for now, but it's an area for improvement)

    try:
        await dht_crawler.start(port=crawler_cfg.get("dht_port", 6881)) 
        
        await main_crawler_loop(
            dht_crawler, # 传递 DHTCrawler 实例 (Pass DHTCrawler instance)
            client_downloader_peer_id, 
            es_loader, # 传递 ESLoader 实例 (Pass ESLoader instance)
            semaphore,
            crawler_cfg["max_peers_per_infohash"]
        )

    except Exception as e:
        logger.critical(f"爬虫主函数发生严重错误: {e} (Critical error in crawler main function: {e})", exc_info=True)
    finally:
        logger.info("正在关闭爬虫... (Shutting down crawler...)")
        
        shutdown_event.set() 
        if metrics_task and not metrics_task.done(): # 检查任务是否存在且未完成 (Check if task exists and is not done)
            metrics_task.cancel() # 取消指标任务 (Cancel metrics task)
            try:
                await metrics_task # 等待取消完成 (Wait for cancellation to complete)
            except asyncio.CancelledError:
                logger.info("指标日志记录任务已取消。 (Metrics logging task cancelled.)")
            except Exception as e_metric_cancel:
                logger.error(f"取消指标任务时出错: {e_metric_cancel} (Error cancelling metrics task: {e_metric_cancel})")


        if dht_crawler.server and dht_crawler.server.protocol: 
            await dht_crawler.stop() 
        
        final_metrics_log_message = ", ".join(f"{key}={value}" for key, value in metrics.items())
        logger.info(f"最终爬虫指标: {final_metrics_log_message} (Final Crawler Metrics: {final_metrics_log_message})")


        if es_loader and hasattr(es_loader.es, 'close') and callable(es_loader.es.close): # 检查 es_loader 是否已初始化 (Check if es_loader was initialized)
            try:
                await es_loader.es.close() 
                logger.info("Elasticsearch 连接已关闭。 (Elasticsearch connection closed.)")
            except Exception as e_es_close:
                logger.error(f"关闭 Elasticsearch 连接时出错: {e_es_close} "
                             f"(Error closing Elasticsearch connection: {e_es_close})")
        
        logger.info("爬虫已关闭。 (Crawler shut down.)")

if __name__ == "__main__":
    try:
        asyncio.run(main()) 
    except KeyboardInterrupt:
        logger.info("用户请求关闭。 (Shutdown requested by user.)")
    except Exception as e:
        logger.critical(f"运行主爬虫时发生未处理的异常: {e} (Unhandled exception while running main crawler: {e})", exc_info=True)
