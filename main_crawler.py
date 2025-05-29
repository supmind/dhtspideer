import asyncio
import signal
import logging
import hashlib
import os # 用于生成 peer_id (For generating peer_id)
import binascii # 用于 infohash 的十六进制转换 (For hex conversion of infohash)
# import json # No longer used for config loading here, but might be used by bencodepy or other parts
import time # 用于定期日志记录 (For periodic logging)
import bencodepy # For decoding metainfo in development mode

from dht_crawler import DHTCrawler
from metainfo_downloader import MetainfoDownloader
# DEFAULT_CONFIG_PATH and load_es_config_from_loader are no longer needed here
from es_loader import ElasticsearchLoader 
# Assuming config_loader.py is in the same directory or accessible in PYTHONPATH
from config_loader import load_config 

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
# METRICS_LOG_INTERVAL = 60 # Will be loaded from config


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

# load_main_config is removed as we are now using config_loader.py
# def load_main_config(...): ...

async def log_metrics_periodically(interval_seconds: int): # Modified to accept interval
    """
    定期记录收集的指标。
    (Periodically log collected metrics.)
    """
    global last_metrics_log_time
    while not shutdown_event.is_set(): 
        await asyncio.sleep(interval_seconds) # Use passed interval
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
    dht_crawler_instance: DHTCrawler,
    downloader_peer_id: bytes,
    es_loader_instance: ElasticsearchLoader,
    semaphore: asyncio.Semaphore,
    max_peers: int,
    # New parameters:
    app_environment: str,
    config: dict 
):
    """
    处理单个 infohash 的异步任务。
    (Asynchronous task to process a single infohash.)
    """
    global metrics
    infohash_hex = binascii.hexlify(infohash_bytes).decode('utf-8')
    
    downloader_cfg_for_instance = config.get("metainfo_downloader_config", {})
    metainfo_downloader_session = MetainfoDownloader(
        infohash=infohash_bytes,
        peer_id=downloader_peer_id,
        connect_timeout_seconds=downloader_cfg_for_instance.get("connect_timeout_seconds", 5),
        read_timeout_seconds=downloader_cfg_for_instance.get("read_timeout_seconds", 10),
        piece_request_timeout_seconds=downloader_cfg_for_instance.get("piece_request_timeout_seconds", 15),
        max_metadata_size_mb=downloader_cfg_for_instance.get("max_metadata_size_mb", 5)
    )

    async with semaphore:
        metrics["active_download_tasks"] += 1
        try:
            if shutdown_event.is_set():
                logger.info(f"关闭事件已设置，跳过处理 infohash: {infohash_hex} "
                            f"(Shutdown event is set, skipping processing for infohash: {infohash_hex})")
                return

            logger.info(f"开始处理 infohash: {infohash_hex} (当前活动任务: {metrics['active_download_tasks']}) "
                        f"(Starting processing for infohash: {infohash_hex} (current active tasks: {metrics['active_download_tasks']}))")
            
            logger.debug(f"[{infohash_hex}] 正在发现对等节点... (Discovering peers...)")
            peers = await dht_crawler_instance.get_peers_for_infohash(infohash_bytes, max_peers)
            if not peers:
                logger.debug(f"[{infohash_hex}] 未找到对等节点。 (No peers found.)")
                metrics["metainfo_downloaded_failure_total"] += 1
                return
            logger.debug(f"[{infohash_hex}] 找到 {len(peers)} 个对等节点。 (Found {len(peers)} peers.)")

            logger.debug(f"[{infohash_hex}] 正在下载元信息... (Downloading metainfo...)")
            metadata_bytes = await metainfo_downloader_session.download(peers)

            if metadata_bytes:
                logger.info(f"[{infohash_hex}] 元信息下载成功，大小: {len(metadata_bytes)} 字节。 "
                            f"(Metainfo downloaded successfully, size: {len(metadata_bytes)} bytes.)")
                metrics["metainfo_downloaded_success_total"] += 1

                dev_settings = config.get("development_settings", {})
                if app_environment == "development":
                    if dev_settings.get("print_metainfo_details", True): # Default to True as per snippet
                        try:
                            decoded_metainfo_for_print = bencodepy.decode(metadata_bytes)
                            logger.info(f"[{infohash_hex}] DEV: Metainfo details:\n{decoded_metainfo_for_print}")
                        except Exception as e_decode:
                            logger.error(f"[{infohash_hex}] DEV: Error decoding metainfo for printing: {e_decode}")
                    else:
                        logger.info(f"[{infohash_hex}] DEV: Metainfo downloaded (details printing disabled).")

                    if dev_settings.get("store_in_es_dev_mode", False):
                        logger.info(f"[{infohash_hex}] DEV: Proceeding to store metainfo in ES (store_in_es_dev_mode is true).")
                        stored_successfully = es_loader_instance.store_metainfo(infohash_bytes, metadata_bytes)
                        if stored_successfully:
                            logger.info(f"[{infohash_hex}] DEV: Metainfo successfully stored/simulated in Elasticsearch.")
                            metrics["metainfo_stored_es_total"] += 1
                        else:
                            logger.warning(f"[{infohash_hex}] DEV: Failed to store/simulate metainfo in Elasticsearch.")
                    else:
                        logger.info(f"[{infohash_hex}] DEV: Skipping ES storage (store_in_es_dev_mode is false).")
                
                else: # Production or other environments
                    # Use app_environment.upper() for more generic logging if more envs are added
                    env_name_for_log = app_environment.upper() if app_environment else "UNKNOWN_ENV"
                    logger.info(f"[{infohash_hex}] {env_name_for_log}: Storing metainfo to Elasticsearch...")
                    stored_successfully = es_loader_instance.store_metainfo(infohash_bytes, metadata_bytes)
                    if stored_successfully:
                        logger.info(f"[{infohash_hex}] {env_name_for_log}: Metainfo successfully stored in Elasticsearch.")
                        metrics["metainfo_stored_es_total"] += 1
                    else:
                        logger.warning(f"[{infohash_hex}] {env_name_for_log}: Failed to store metainfo in Elasticsearch.")
            else: # metadata_bytes is None
                logger.info(f"[{infohash_hex}] 元信息下载失败。 (Failed to download metainfo.)")
                metrics["metainfo_downloaded_failure_total"] += 1

        except Exception as e:
            logger.error(f"处理 infohash {infohash_hex} 时发生错误: {e} "
                         f"(Error processing infohash {infohash_hex}: {e})", exc_info=True)
            metrics["metainfo_downloaded_failure_total"] += 1
        finally:
            metrics["active_download_tasks"] -= 1
            logger.debug(f"完成处理 infohash: {infohash_hex} (当前活动任务: {metrics['active_download_tasks']}) "
                         f"(Finished processing for infohash: {infohash_hex} (current active tasks: {metrics['active_download_tasks']}))")


async def main_crawler_loop(
    dht_crawler_instance: DHTCrawler,
    downloader_peer_id: bytes,
    es_loader_instance: ElasticsearchLoader,
    semaphore: asyncio.Semaphore,
    max_peers: int,
    # New parameters:
    app_environment: str,
    config: dict
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
                    max_peers,
                    app_environment, # Pass current environment
                    config           # Pass full config
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
    # Load configuration using the new config_loader; it now returns config and environment string
    config, app_environment = load_config() 

    # 从新的配置结构中提取各个部分的配置 (Extract configurations for different parts from the new structure)
    dev_settings = config.get("development_settings", {}) # Used in process_infohash_task via config
    dht_cfg = config.get("dht_crawler_config", {})
    downloader_cfg = config.get("metainfo_downloader_config", {}) # Used for semaphore and max_peers
    es_cfg = config.get("elasticsearch_config", {})
    log_cfg = config.get("logging_config", {})
    main_cfg = config.get("main_crawler_config", {})

    # Update global client name and version (used by generate_peer_id)
    global CLIENT_NAME, CLIENT_VERSION
    CLIENT_NAME = main_cfg.get("client_name", "MyCr") 
    CLIENT_VERSION = main_cfg.get("client_version", "0001")

    setup_logging(log_level_str=log_cfg.get("level", "INFO"),
                  log_file=log_cfg.get("file_path"), 
                  log_format=DEFAULT_LOG_FORMAT)

    logger.info(f"实际加载的环境配置: {app_environment} "
                f"(Actual loaded environment config: {app_environment})")
    logger.info(f"配置文件中的环境值: {config.get('environment', '未指定')} "
                f"(Environment value from config file: {config.get('environment', 'Not specified')})")

    if app_environment == "development":
        logger.info(f"开发设置 (从config中获取): {dev_settings} (Development Settings (from config): {dev_settings})")

    if not es_cfg.get("host") or not es_cfg.get("port"):
        logger.critical("Elasticsearch 配置 (host, port) 无效或缺失。爬虫无法启动。 "
                        "(Elasticsearch configuration (host, port) is invalid or missing. Crawler cannot start.)")
        return

    client_downloader_peer_id = generate_peer_id()
    infohash_discovered_queue = asyncio.Queue()
        
    # DHT 节点 ID 处理 (DHT Node ID handling)
    node_id_source = dht_cfg.get("node_id_source", "random").lower()
    dht_node_id = None
    if node_id_source == "file":
        node_id_file = dht_cfg.get("node_id_file_path", "node_id.bin")
        try:
            with open(node_id_file, 'rb') as f_node_id:
                dht_node_id = f_node_id.read()
            if len(dht_node_id) != 20:
                logger.warning(f"从 '{node_id_file}' 加载的节点 ID 长度不为 20 字节。将生成随机 ID。 "
                               f"(Node ID loaded from '{node_id_file}' is not 20 bytes long. Generating random ID instead.)")
                dht_node_id = None 
        except FileNotFoundError:
            logger.warning(f"节点 ID 文件 '{node_id_file}' 未找到。将生成随机 ID 并尝试保存。 "
                           f"(Node ID file '{node_id_file}' not found. Generating random ID and attempting to save.)")
        except Exception as e_node_id:
            logger.error(f"加载节点 ID 文件 '{node_id_file}' 时出错: {e_node_id}。将生成随机 ID。 "
                          f"(Error loading node ID file '{node_id_file}': {e_node_id}. Generating random ID.)")

    if dht_node_id is None: 
        dht_node_id = hashlib.sha1(os.urandom(20)).digest()
        if node_id_source == "file": 
            node_id_file_to_save = dht_cfg.get("node_id_file_path", "node_id.bin")
            try:
                with open(node_id_file_to_save, 'wb') as f_save_node_id:
                    f_save_node_id.write(dht_node_id)
                logger.info(f"新的随机节点 ID 已保存到 '{node_id_file_to_save}'。 (New random node ID saved to '{node_id_file_to_save}'.)")
            except Exception as e_save_node_id:
                logger.error(f"保存新的节点 ID 到 '{node_id_file_to_save}' 失败: {e_save_node_id}。 "
                              f"(Failed to save new node ID to '{node_id_file_to_save}': {e_save_node_id}.)")
    
    logger.info(f"使用的 DHT 节点 ID (十六进制): {binascii.hexlify(dht_node_id).decode()} "
                f"(Using DHT Node ID (hex): {binascii.hexlify(dht_node_id).decode()})")

    dht_crawler = DHTCrawler(
        infohash_queue=infohash_discovered_queue,
        node_id=dht_node_id,
        k_size=dht_cfg.get("k_size", 20),
        alpha=dht_cfg.get("alpha", 3),
        bootstrap_nodes=dht_cfg.get("bootstrap_nodes", [])
    )
    
    es_loader = None
    try:
        es_loader = ElasticsearchLoader(es_config=es_cfg) # es_cfg from loaded config
        if not await es_loader.es.ping():
             logger.critical("无法 ping通 Elasticsearch 服务器。请检查连接和服务器状态。爬虫将退出。 "
                            "(Failed to ping Elasticsearch server. Please check connection and server status. Crawler will exit.)")
             return
    except Exception as e_init_es:
        logger.critical(f"初始化 ElasticsearchLoader 失败: {e_init_es}。爬虫无法启动。 "
                        f"(Failed to initialize ElasticsearchLoader: {e_init_es}. Crawler cannot start.)")
        return

    semaphore = asyncio.Semaphore(downloader_cfg.get("max_concurrent_downloads", 10))
    
    loop = asyncio.get_running_loop()
    shutdown_timeout_seconds = main_cfg.get("shutdown_timeout_seconds", 30) 
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, handle_shutdown_signal, sig_name, loop)

    metrics_log_interval = main_cfg.get("metrics_log_interval_seconds", 60)
    metrics_task = asyncio.create_task(log_metrics_periodically(metrics_log_interval)) # Pass interval

    try:
        await dht_crawler.start(port=dht_cfg.get("dht_port", 6881))
        
        await main_crawler_loop(
            dht_crawler_instance=dht_crawler, 
            downloader_peer_id=client_downloader_peer_id,
            es_loader_instance=es_loader,
            semaphore=semaphore,
            max_peers=downloader_cfg.get("max_peers_per_infohash", 50),
            # Pass new arguments:
            app_environment=app_environment,
            config=config
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
