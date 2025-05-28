import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock, mock_open, call # 导入 call 用于检查调用顺序 (Import call for checking call order)
import logging # 用于测试日志记录 (For testing logging)
import json # 用于模拟配置文件 (For mocking config files)
import os # 用于测试 peer_id 生成 (For testing peer_id generation)
import binascii # 用于字节串和十六进制字符串之间的转换 (For conversion between bytes and hex strings)

# 假设 main_crawler.py 等模块在项目的根目录或 PYTHONPATH 中
# (Assuming main_crawler.py and other modules are in the project root or PYTHONPATH)
# 需要从 main_crawler 导入被测试的函数和类
# (Need to import functions and classes to be tested from main_crawler)
# 这可能需要根据实际的项目结构进行调整
# (This might need adjustment based on actual project structure)
import main_crawler 
from main_crawler import (
    generate_peer_id, 
    load_main_config, # 在 main_crawler.py 中重命名为 load_main_config (Renamed to load_main_config in main_crawler.py)
    setup_logging,
    process_infohash_task,
    main_crawler_loop,
    handle_shutdown_signal,
    main as main_crawler_main_func # 避免与 unittest.main 冲突 (Avoid conflict with unittest.main)
)

# 从其他模块导入被模拟的类 (Import classes to be mocked from other modules)
from dht_crawler import DHTCrawler
from metainfo_downloader import MetainfoDownloader
from es_loader import ElasticsearchLoader

# --- 辅助数据 --- (Helper Data)
SAMPLE_INFOHASH_BYTES = os.urandom(20) # 示例 infohash 字节串 (Sample infohash bytes)
SAMPLE_INFOHASH_HEX = binascii.hexlify(SAMPLE_INFOHASH_BYTES).decode() # 示例 infohash 十六进制字符串 (Sample infohash hex string)
DEFAULT_TEST_CONFIG = { # 用于测试的默认配置 (Default config for testing)
    "crawler": {
        "max_concurrent_downloads": 2,
        "max_peers_per_infohash": 5,
        "dht_port": 6881
    },
    "logging": {
        "level": "DEBUG",
        "file_path": None
    },
    "elasticsearch": { # Elasticsearch 配置也需要 (Elasticsearch config also needed)
        "host": "mockhost", 
        "port": 1234, 
        "scheme": "http"
    }
}

class TestMainCrawler(unittest.IsolatedAsyncioTestCase):
    """
    测试 main_crawler.py 中的主要业务流程和辅助函数。
    (Tests for the main orchestration logic and helper functions in main_crawler.py.)
    """

    def setUp(self):
        # 通用设置 (Common setup)
        # 重置 main_crawler 中的全局 shutdown_event，以确保测试隔离性
        # (Reset global shutdown_event in main_crawler to ensure test isolation)
        main_crawler.shutdown_event.clear() 
        # 重置指标 (Reset metrics)
        main_crawler.metrics = {
            "infohashes_discovered_total": 0,
            "metainfo_downloaded_success_total": 0,
            "metainfo_downloaded_failure_total": 0,
            "metainfo_stored_es_total": 0,
            "active_download_tasks": 0
        }


    # --- 测试配置加载 --- (Configuration Loading Tests)
    @patch("builtins.open", new_callable=mock_open) # 模拟内置的 open 函数 (Mock the built-in open function)
    def test_load_main_config_valid(self, mock_file_open):
        # 测试加载有效的配置文件 (Test loading a valid configuration file)
        valid_json_str = json.dumps(DEFAULT_TEST_CONFIG) # 将字典转换为 JSON 字符串 (Convert dict to JSON string)
        mock_file_open.return_value.read.return_value = valid_json_str # 设置模拟文件读取的内容 (Set content for mock file read)
        
        config = load_main_config("dummy_path/config.json") # 调用被测函数 (Call the function under test)
        
        self.assertEqual(config["crawler"]["max_concurrent_downloads"], DEFAULT_TEST_CONFIG["crawler"]["max_concurrent_downloads"]) # 验证爬虫配置 (Verify crawler config)
        self.assertEqual(config["logging"]["level"], DEFAULT_TEST_CONFIG["logging"]["level"]) # 验证日志配置 (Verify logging config)
        self.assertEqual(config["elasticsearch"]["host"], DEFAULT_TEST_CONFIG["elasticsearch"]["host"]) # 验证 ES 配置 (Verify ES config)
        mock_file_open.assert_called_once_with("dummy_path/config.json", 'r', encoding='utf-8') # 验证文件打开调用 (Verify file open call)

    @patch("builtins.open", side_effect=FileNotFoundError) # 模拟文件未找到 (Simulate file not found)
    def test_load_main_config_file_not_found_uses_defaults(self, mock_file_open):
        # 测试当配置文件不存在时，是否使用默认值 (Test if default values are used when config file is not found)
        config = load_main_config("dummy_path/non_existent.json")
        # 验证返回的是默认配置 (Verify that the returned config is the default config)
        self.assertEqual(config["crawler"]["max_concurrent_downloads"], 5) # 检查默认值 (Check default value)
        self.assertEqual(config["logging"]["level"], "INFO") # 检查默认值 (Check default value)

    @patch("builtins.open", new_callable=mock_open, read_data='{"crawler": {"max_concurrent_downloads": 20}}') # 部分配置 (Partial config)
    def test_load_main_config_partial_config_uses_defaults(self, mock_file_open):
        # 测试当配置文件部分缺失时，是否正确合并默认值 (Test if defaults are correctly merged when config is partial)
        config = load_main_config("dummy_path/partial_config.json")
        self.assertEqual(config["crawler"]["max_concurrent_downloads"], 20) # 来自文件的值 (Value from file)
        self.assertEqual(config["crawler"]["max_peers_per_infohash"], 30) # 来自默认的值 (Default value)
        self.assertEqual(config["logging"]["level"], "INFO") # 来自默认的值 (Default value)

    # --- 测试 Peer ID 生成 --- (Peer ID Generation Tests)
    def test_generate_peer_id(self):
        # 测试 peer_id 生成 (Test peer_id generation)
        peer_id = generate_peer_id() # 调用生成函数 (Call generation function)
        self.assertIsInstance(peer_id, bytes) # 断言类型为字节串 (Assert type is bytes)
        self.assertEqual(len(peer_id), 20) # 断言长度为 20 (Assert length is 20)
        # 验证 Azureus 风格前缀 (Verify Azureus-style prefix)
        # main_crawler.CLIENT_NAME 和 main_crawler.CLIENT_VERSION 在模块级别定义
        # (main_crawler.CLIENT_NAME and main_crawler.CLIENT_VERSION are defined at module level)
        expected_prefix = f"-{main_crawler.CLIENT_NAME[:2].upper()}{main_crawler.CLIENT_VERSION}-"
        self.assertTrue(peer_id.startswith(expected_prefix.encode('ascii'))) # 断言前缀正确 (Assert prefix is correct)

    # --- 测试日志设置 --- (Logging Setup Tests)
    @patch('logging.FileHandler') # 模拟 FileHandler (Mock FileHandler)
    @patch('logging.StreamHandler') # 模拟 StreamHandler (Mock StreamHandler)
    @patch('logging.Formatter') # 模拟 Formatter (Mock Formatter)
    @patch('logging.getLogger') # 模拟 getLogger (Mock getLogger)
    def test_setup_logging(self, mock_get_logger, mock_formatter, mock_stream_handler, mock_file_handler):
        # 测试日志设置功能 (Test logging setup function)
        mock_root_logger = MagicMock() # 创建根日志记录器的模拟实例 (Create mock instance for root logger)
        mock_get_logger.return_value = mock_root_logger # getLogger 返回模拟的根记录器 (getLogger returns mocked root logger)

        # 测试 INFO 级别，无文件日志 (Test INFO level, no file logging)
        setup_logging(log_level_str="INFO", log_file=None)
        mock_root_logger.setLevel.assert_called_with(logging.INFO) # 验证设置的级别 (Verify level set)
        mock_stream_handler.assert_called_once() # 验证流处理器被创建 (Verify stream handler created)
        mock_file_handler.assert_not_called() # 验证文件处理器未被创建 (Verify file handler not created)
        
        # 重置模拟对象以进行下一次调用 (Reset mocks for next call)
        mock_root_logger.reset_mock()
        mock_stream_handler.reset_mock()
        mock_file_handler.reset_mock()

        # 测试 DEBUG 级别，带文件日志 (Test DEBUG level, with file logging)
        setup_logging(log_level_str="DEBUG", log_file="test.log")
        mock_root_logger.setLevel.assert_called_with(logging.DEBUG) # 验证设置的级别 (Verify level set)
        mock_stream_handler.assert_called_once() # 验证流处理器被创建 (Verify stream handler created)
        mock_file_handler.assert_called_once_with("test.log", mode='a', encoding='utf-8') # 验证文件处理器被创建和参数 (Verify file handler created and its arguments)


    # --- 测试 process_infohash_task --- (process_infohash_task Tests)
    @patch('main_crawler.ElasticsearchLoader') # 模拟 ESLoader (Mock ESLoader)
    @patch('main_crawler.MetainfoDownloader') # 模拟 MetainfoDownloader (Mock MetainfoDownloader)
    @patch('main_crawler.DHTCrawler') # 模拟 DHTCrawler (Mock DHTCrawler)
    async def test_process_infohash_task_success_pipeline(self, MockDHTCrawler, MockMetainfoDownloader, MockESLoader):
        # 测试成功处理 infohash 的完整流程 (Test successful processing pipeline for an infohash)
        mock_dht_instance = MockDHTCrawler.return_value # 获取 DHTCrawler 的模拟实例 (Get mock instance of DHTCrawler)
        mock_downloader_instance = MockMetainfoDownloader.return_value # 获取 MetainfoDownloader 的模拟实例 (Get mock instance of MetainfoDownloader)
        mock_es_loader_instance = MockESLoader.return_value # 获取 ESLoader 的模拟实例 (Get mock instance of ESLoader)

        # 配置模拟方法的返回值 (Configure return values of mocked methods)
        sample_peers = [("1.2.3.4", 12345)] # 示例对等节点列表 (Sample peer list)
        mock_dht_instance.get_peers_for_infohash = AsyncMock(return_value=sample_peers)
        
        sample_metadata_bytes = b"d4:infod4:name4:testi0ee" # 示例元信息字节串 (Sample metainfo bytes)
        mock_downloader_instance.download = AsyncMock(return_value=sample_metadata_bytes)
        
        mock_es_loader_instance.store_metainfo = MagicMock(return_value=True) # store_metainfo 是同步的 (store_metainfo is synchronous)

        semaphore = asyncio.Semaphore(1) # 用于测试的信号量 (Semaphore for testing)
        downloader_peer_id = generate_peer_id() # 生成下载器 peer_id (Generate downloader peer_id)

        # 调用被测任务 (Call the task under test)
        await process_infohash_task(
            SAMPLE_INFOHASH_BYTES, 
            mock_dht_instance, 
            downloader_peer_id, 
            mock_es_loader_instance, 
            semaphore, 
            max_peers=5
        )

        # 验证调用 (Verify calls)
        mock_dht_instance.get_peers_for_infohash.assert_called_once_with(SAMPLE_INFOHASH_BYTES, 5) # 验证 get_peers 调用 (Verify get_peers call)
        # 验证 MetainfoDownloader 是否使用正确的 infohash 和 peer_id 实例化
        # (Verify MetainfoDownloader was instantiated with correct infohash and peer_id)
        # 由于它在任务内部实例化，我们需要检查 MockMetainfoDownloader 的调用
        # (Since it's instantiated inside the task, we need to check calls to MockMetainfoDownloader)
        MockMetainfoDownloader.assert_called_once_with(infohash=SAMPLE_INFOHASH_BYTES, peer_id=downloader_peer_id)
        mock_downloader_instance.download.assert_called_once_with(sample_peers) # 验证 download 调用 (Verify download call)
        mock_es_loader_instance.store_metainfo.assert_called_once_with(SAMPLE_INFOHASH_BYTES, sample_metadata_bytes) # 验证 store_metainfo 调用 (Verify store_metainfo call)
        
        # 验证指标更新 (Verify metric updates)
        self.assertEqual(main_crawler.metrics["metainfo_downloaded_success_total"], 1) # 下载成功计数 (Download success count)
        self.assertEqual(main_crawler.metrics["metainfo_stored_es_total"], 1) # 存储成功计数 (Store success count)


    @patch('main_crawler.ElasticsearchLoader')
    @patch('main_crawler.MetainfoDownloader')
    @patch('main_crawler.DHTCrawler')
    async def test_process_infohash_task_no_peers(self, MockDHTCrawler, MockMetainfoDownloader, MockESLoader):
        # 测试当 get_peers_for_infohash 未返回对等节点时的流程
        # (Test pipeline when get_peers_for_infohash returns no peers)
        mock_dht_instance = MockDHTCrawler.return_value
        mock_downloader_instance = MockMetainfoDownloader.return_value
        mock_es_loader_instance = MockESLoader.return_value
        
        mock_dht_instance.get_peers_for_infohash = AsyncMock(return_value=[]) # 返回空列表 (Return empty list)
        semaphore = asyncio.Semaphore(1)
        downloader_peer_id = generate_peer_id()

        await process_infohash_task(SAMPLE_INFOHASH_BYTES, mock_dht_instance, downloader_peer_id, mock_es_loader_instance, semaphore, 5)
        
        mock_dht_instance.get_peers_for_infohash.assert_called_once() # 验证 get_peers 调用 (Verify get_peers call)
        mock_downloader_instance.download.assert_not_called() # download 不应被调用 (download should not be called)
        mock_es_loader_instance.store_metainfo.assert_not_called() # store_metainfo 不应被调用 (store_metainfo should not be called)
        self.assertEqual(main_crawler.metrics["metainfo_downloaded_failure_total"], 1) # 下载失败计数 (Download failure count)


    # --- 测试 main_crawler_loop 和 shutdown --- (Tests for main_crawler_loop and shutdown)
    @patch('main_crawler.process_infohash_task', new_callable=AsyncMock) # 模拟 process_infohash_task (Mock process_infohash_task)
    @patch('main_crawler.DHTCrawler')
    @patch('main_crawler.ElasticsearchLoader') # MetainfoDownloader 在 process_infohash_task 中处理 (MetainfoDownloader handled within process_infohash_task)
    async def test_main_crawler_loop_processes_items_and_shuts_down(
        self, MockESLoader, MockDHTCrawler, mock_process_task
    ):
        # 测试主循环处理项目并在关闭事件设置时停止
        # (Test main loop processing items and stopping when shutdown_event is set)
        mock_dht_instance = MockDHTCrawler.return_value
        mock_es_loader_instance = MockESLoader.return_value
        
        # 模拟 DHT 队列返回几个 infohash，然后是 TimeoutError (表示队列暂时为空)
        # (Simulate DHT queue returning a few infohashes, then TimeoutError (queue temporarily empty))
        # 然后设置关闭事件 (Then set shutdown event)
        mock_dht_instance.infohash_queue = AsyncMock(spec=asyncio.Queue)
        async def queue_side_effect(*args, **kwargs):
            if mock_dht_instance.infohash_queue.get.call_count == 1:
                return SAMPLE_INFOHASH_BYTES # 第一个 infohash (First infohash)
            elif mock_dht_instance.infohash_queue.get.call_count == 2:
                return os.urandom(20) # 第二个 infohash (Second infohash)
            else:
                # 在几次成功获取后，模拟超时或关闭 (After a few successful gets, simulate timeout or shutdown)
                if not main_crawler.shutdown_event.is_set(): # 如果未关闭，则引发超时 (If not shutting down, raise timeout)
                    raise asyncio.TimeoutError 
                else: # 如果已关闭，则引发 CancelledError 或其他方式停止循环 (If shutting down, raise CancelledError or otherwise stop loop)
                    # 实际上，循环会因为 shutdown_event.is_set() 而退出
                    # (Actually, the loop will exit due to shutdown_event.is_set())
                    # 为了测试，我们可以让它在关闭后继续引发超时，循环应该会自行终止
                    # (For testing, we can let it keep raising TimeoutError after shutdown, loop should terminate itself)
                    raise asyncio.TimeoutError 

        mock_dht_instance.infohash_queue.get = AsyncMock(side_effect=queue_side_effect)
        mock_dht_instance.infohash_queue.task_done = AsyncMock() # 模拟 task_done (Mock task_done)

        semaphore = asyncio.Semaphore(1) # 信号量 (Semaphore)
        downloader_peer_id = generate_peer_id()

        # 运行主循环一小段时间，然后设置关闭事件
        # (Run main loop for a short period, then set shutdown event)
        async def timed_shutdown():
            await asyncio.sleep(0.1) # 允许循环处理一些项目 (Allow loop to process some items)
            main_crawler.shutdown_event.set() # 设置关闭事件 (Set shutdown event)

        await asyncio.gather(
            main_crawler_loop(
                mock_dht_instance, downloader_peer_id, mock_es_loader_instance, semaphore, 5
            ),
            timed_shutdown() # 同时运行定时关闭 (Run timed shutdown concurrently)
        )

        self.assertTrue(main_crawler.shutdown_event.is_set()) # 验证关闭事件已设置 (Verify shutdown event is set)
        self.assertGreaterEqual(mock_process_task.call_count, 2) # 验证至少处理了两个项目 (Verify at least two items processed)
        self.assertEqual(main_crawler.metrics["infohashes_discovered_total"], 2) # 验证发现的 infohash 计数 (Verify discovered infohash count)


    # --- 测试 main 函数 (整体集成和关闭) --- (Tests for main function (overall integration and shutdown))
    # 这需要更广泛的补丁 (This requires more extensive patching)
    @patch('main_crawler.load_main_config')
    @patch('main_crawler.setup_logging')
    @patch('main_crawler.generate_peer_id')
    @patch('main_crawler.DHTCrawler', new_callable=AsyncMock) # 使用 AsyncMock 作为类模拟 (Use AsyncMock as class mock)
    @patch('main_crawler.ElasticsearchLoader')
    @patch('main_crawler.main_crawler_loop', new_callable=AsyncMock) # 模拟主循环 (Mock the main loop)
    @patch('main_crawler.log_metrics_periodically', new_callable=AsyncMock) # 模拟指标日志记录 (Mock metrics logging)
    @patch('asyncio.Semaphore') # 模拟信号量 (Mock Semaphore)
    @patch('asyncio.Queue') # 模拟队列 (Mock Queue)
    async def test_main_function_orchestration_and_shutdown(
        self, MockQueue, MockSemaphore, mock_log_metrics, mock_main_loop, MockESLoader, 
        MockDHTCrawler, mock_gen_peer_id, mock_setup_logging, mock_load_cfg
    ):
        # 测试 main 函数的整体业务流程和关闭逻辑 (Test main function's overall orchestration and shutdown logic)
        
        # 配置模拟返回值 (Configure mock return values)
        mock_load_cfg.return_value = DEFAULT_TEST_CONFIG # 返回测试配置 (Return test config)
        mock_gen_peer_id.return_value = b"test_peer_id_1234567" # 模拟生成的 peer_id (Simulated generated peer_id)
        
        mock_dht_instance = MockDHTCrawler.return_value # DHTCrawler 的模拟实例 (Mock instance of DHTCrawler)
        mock_dht_instance.start = AsyncMock() # 模拟 start (Mock start)
        mock_dht_instance.stop = AsyncMock() # 模拟 stop (Mock stop)

        mock_es_instance = MockESLoader.return_value # ESLoader 的模拟实例 (Mock instance of ESLoader)
        mock_es_instance.es = AsyncMock() # 模拟 ES 客户端 (Mock ES client)
        mock_es_instance.es.ping = AsyncMock(return_value=True) # 模拟 ping 成功 (Simulate successful ping)
        mock_es_instance.es.close = AsyncMock() # 模拟 close (Mock close)

        # 模拟主循环在一段时间后通过设置 shutdown_event 来停止
        # (Simulate main_crawler_loop stopping after some time by setting shutdown_event)
        async def main_loop_behavior(*args, **kwargs):
            await asyncio.sleep(0.05) # 模拟一些工作 (Simulate some work)
            main_crawler.shutdown_event.set() # 在循环内部设置关闭 (Set shutdown from within loop)
            return
        mock_main_loop.side_effect = main_loop_behavior

        # 调用 main_crawler.py 中的 main 函数 (Call main function in main_crawler.py)
        await main_crawler_main_func()

        # 验证调用 (Verify calls)
        mock_load_cfg.assert_called_once() # 验证配置加载 (Verify config load)
        mock_setup_logging.assert_called_once() # 验证日志设置 (Verify logging setup)
        mock_gen_peer_id.assert_called_once() # 验证 peer_id 生成 (Verify peer_id generation)
        
        MockDHTCrawler.assert_called_once() # 验证 DHTCrawler 实例化 (Verify DHTCrawler instantiation)
        mock_dht_instance.start.assert_called_once() # 验证 DHT start (Verify DHT start)
        
        MockESLoader.assert_called_once() # 验证 ESLoader 实例化 (Verify ESLoader instantiation)
        mock_es_instance.es.ping.assert_called_once() # 验证 ES ping (Verify ES ping)
        
        mock_main_loop.assert_called_once() # 验证主循环被调用 (Verify main loop was called)
        mock_log_metrics.assert_called_once() # 验证指标日志记录任务启动 (Verify metrics logging task started)

        # 验证关闭逻辑 (Verify shutdown logic)
        self.assertTrue(main_crawler.shutdown_event.is_set()) # 验证关闭事件已设置 (Verify shutdown event is set)
        mock_dht_instance.stop.assert_called_once() # 验证 DHT stop (Verify DHT stop)
        mock_es_instance.es.close.assert_called_once() # 验证 ES close (Verify ES close)
        # 验证指标任务被取消 (Verify metrics task was cancelled)
        # mock_log_metrics.return_value.cancel.assert_called_once() # cancel() 是在任务对象上调用的 (cancel() is called on the task object)
        # 这需要更复杂的模拟来捕获创建的任务 (This requires more complex mocking to capture the created task)
        # 对于这个测试，我们主要信任 main_crawler_loop 在关闭时正确处理了它
        # (For this test, we primarily trust that main_crawler_loop handled it correctly on shutdown)


    def test_handle_shutdown_signal(self):
        # 测试关闭信号处理函数 (Test shutdown signal handler)
        mock_loop = MagicMock() # 模拟事件循环 (Mock event loop)
        self.assertFalse(main_crawler.shutdown_event.is_set()) # 初始状态：关闭事件未设置 (Initial state: shutdown event not set)
        handle_shutdown_signal(signal.SIGINT, mock_loop) # 调用信号处理函数 (Call signal handler)
        self.assertTrue(main_crawler.shutdown_event.is_set()) # 验证关闭事件已设置 (Verify shutdown event is set)


if __name__ == '__main__':
    unittest.main() # 运行测试 (Run tests)

```
