import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock, call # 导入 call 用于检查调用顺序 (Import call for checking call order)
import binascii
import os
import socket # 用于 IP 地址转换 (For IP address conversion)
import struct # 用于解包紧凑的节点/对等方信息 (For unpacking compact node/peer info)

# 假设 dht_crawler.py 在项目的根目录或 PYTHONPATH 中
# (Assuming dht_crawler.py is in the project root or PYTHONPATH)
from dht_crawler import DHTCrawler, InfohashQueueStorage, DEFAULT_BOOTSTRAP_NODES
# 从 kademlia 库导入 Node，因为测试中会用到它
# (Import Node from kademlia library as it will be used in tests)
from kademlia.node import Node 

# --- 辅助数据 --- (Helper Data)
SAMPLE_NODE_ID = os.urandom(20) # 示例节点 ID (Sample Node ID)
SAMPLE_INFOHASH_HEX = "aabbccddeeff00112233445566778899aabbccdd"
SAMPLE_INFOHASH_BYTES = bytes.fromhex(SAMPLE_INFOHASH_HEX)


class TestInfohashQueueStorage(unittest.IsolatedAsyncioTestCase):
    """
    测试 InfohashQueueStorage 类。
    (Tests for the InfohashQueueStorage class.)
    这些测试验证存储后端是否按预期将 infohash 放入队列。
    (These tests verify that the storage backend correctly puts infohashes into the queue.)
    """
    async def test_storage_queues_infohash_on_set(self):
        # 测试当调用 set 方法时，infohash 是否被放入队列
        # (Test if infohash is put into the queue when set method is called)
        queue = asyncio.Queue() # 创建一个真实的 asyncio.Queue (Create a real asyncio.Queue)
        storage = InfohashQueueStorage(infohash_queue=queue) # 使用队列初始化存储 (Initialize storage with the queue)
        
        test_key = os.urandom(20) # 测试用的 infohash (Infohash for testing)
        test_value = b"peer_data" # 测试用的对等节点数据 (Peer data for testing)
        
        # 调用异步的 set 方法 (Call the async set method)
        await storage.set(test_key, test_value, original_publisher=False, original_publish_time=0)
        
        # 从队列中获取 infohash 并验证 (Get infohash from queue and verify)
        queued_infohash = await asyncio.wait_for(queue.get(), timeout=1) # 设置超时以防测试挂起 (Set timeout to prevent test hanging)
        self.assertEqual(queued_infohash, test_key) # 断言队列中的 infohash 正确 (Assert infohash in queue is correct)
        queue.task_done() # 标记任务完成 (Mark task as done)

    async def test_storage_queues_infohash_on_get(self):
        # 测试当调用 get 方法时，infohash 是否被放入队列
        # (Test if infohash is put into the queue when get method is called)
        queue = asyncio.Queue()
        storage = InfohashQueueStorage(infohash_queue=queue)
        test_key = os.urandom(20)
        
        # 调用异步的 get 方法 (Call the async get method)
        # get 方法在 ForgetfulStorage 中可能会查找不存在的键并返回 None
        # (get method in ForgetfulStorage might look for non-existent key and return None)
        # 我们主要关心的是键是否被记录并放入队列
        # (We are primarily concerned if the key is logged and put into the queue)
        await storage.get(test_key) 
        
        queued_infohash = await asyncio.wait_for(queue.get(), timeout=1)
        self.assertEqual(queued_infohash, test_key)
        queue.task_done()

    def test_storage_queues_infohash_on_getitem_sync(self):
        # 测试当调用同步的 __getitem__ 方法时，infohash 是否被放入队列
        # (Test if infohash is put into the queue when synchronous __getitem__ method is called)
        queue = asyncio.Queue() 
        storage = InfohashQueueStorage(infohash_queue=queue)
        test_key = os.urandom(20)
        
        # __getitem__ 可能会因为键不存在而引发 KeyError
        # ( __getitem__ might raise KeyError for non-existent key)
        with self.assertRaises(KeyError): # 断言会引发 KeyError (Assert KeyError is raised)
            storage[test_key] # 调用同步的 __getitem__ (Call synchronous __getitem__)
        
        # 检查队列 (Check queue)
        # 由于 __getitem__ 是同步的，而 put_nowait 也是同步的，我们可以立即检查
        # (Since __getitem__ is sync and put_nowait is also sync, we can check immediately)
        self.assertFalse(queue.empty()) 
        queued_infohash = queue.get_nowait() 
        self.assertEqual(queued_infohash, test_key) 
        queue.task_done() 


@patch('dht_crawler.Server', new_callable=AsyncMock) # 模拟 kademlia.network.Server (Mock kademlia.network.Server)
class TestDHTCrawler(unittest.IsolatedAsyncioTestCase):
    """
    测试 DHTCrawler 类。
    (Tests for the DHTCrawler class.)
    """

    def setUp(self):
        # 通用设置 (Common setup)
        self.mock_queue = MagicMock(spec=asyncio.Queue) 
        self.node_id = SAMPLE_NODE_ID
        # 在每个测试中创建 crawler 实例，以确保 MockServer 被正确地重新初始化/传递
        # (Create crawler instance in each test to ensure MockServer is correctly re-initialized/passed)

    # --- 初始化测试 --- (Initialization Tests)
    def test_init_success(self, MockServerCls): 
        # 测试成功初始化 (Test successful initialization)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        self.assertEqual(crawler.infohash_queue, self.mock_queue) 
        self.assertEqual(crawler.node_id, self.node_id) 
        self.assertIsInstance(crawler.custom_storage, InfohashQueueStorage) 
        self.assertEqual(crawler.custom_storage.infohash_queue, self.mock_queue) 

    def test_init_no_queue_raises_value_error(self, MockServerCls):
        # 测试未提供 infohash_queue 时引发 ValueError (Test ValueError raised if infohash_queue is not provided)
        with self.assertRaisesRegex(ValueError, "infohash_queue .* 必须提供"): 
            DHTCrawler(infohash_queue=None)
    
    def test_init_default_bootstrap_nodes(self, MockServerCls):
        # 测试当未提供引导节点时，是否使用默认节点
        # (Test if default bootstrap nodes are used when none are provided)
        crawler = DHTCrawler(infohash_queue=self.mock_queue) # 不传递 bootstrap_nodes (Do not pass bootstrap_nodes)
        self.assertEqual(crawler.bootstrap_nodes, DEFAULT_BOOTSTRAP_NODES) # 验证使用了默认节点 (Verify default nodes are used)

    def test_init_custom_bootstrap_nodes(self, MockServerCls):
        # 测试是否正确使用了自定义引导节点
        # (Test if custom bootstrap nodes are correctly used)
        custom_nodes = [("node1.com", 123), ("node2.org", 456)] # 自定义节点列表 (Custom node list)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, bootstrap_nodes=custom_nodes)
        self.assertEqual(crawler.bootstrap_nodes, custom_nodes) # 验证使用了自定义节点 (Verify custom nodes are used)


    # --- start 方法测试 --- (start method tests)
    async def test_start_listens_and_bootstraps(self, MockServerCls):
        # 测试 start 方法是否正确调用 listen 和 bootstrap (Test if start method calls listen and bootstrap correctly)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value 
        
        mock_server_instance.listen = AsyncMock() 
        mock_server_instance.bootstrap = AsyncMock(return_value=([Node(os.urandom(20))], [os.urandom(20)])) # bootstrap 返回一个元组 (bootstrap returns a tuple)
        
        await crawler.start(port=12345) 
        
        MockServerCls.assert_called_once() 
        args, kwargs = MockServerCls.call_args
        self.assertIsInstance(kwargs['storage'], InfohashQueueStorage) 
        self.assertEqual(kwargs['id'], self.node_id) 

        mock_server_instance.listen.assert_called_once_with(12345) 
        mock_server_instance.bootstrap.assert_called_once_with(DEFAULT_BOOTSTRAP_NODES) 
        self.assertEqual(crawler.server, mock_server_instance) 

    async def test_start_handles_listen_os_error(self, MockServerCls):
        # 测试 start 方法处理 OSError (例如端口已占用) (Test start method handling OSError (e.g., port already in use))
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        mock_server_instance.listen = AsyncMock(side_effect=OSError("端口已占用 (Port already in use)")) 
        
        with self.assertRaises(OSError): 
            await crawler.start(port=12345)

    async def test_start_handles_bootstrap_exception(self, MockServerCls):
        # 测试 start 方法处理 bootstrap 期间的异常
        # (Test start method handling exception during bootstrap)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        mock_server_instance.listen = AsyncMock() # listen 成功 (listen succeeds)
        mock_server_instance.bootstrap = AsyncMock(side_effect=Exception("引导失败 (Bootstrap failed)")) # bootstrap 失败 (bootstrap fails)

        with self.assertRaisesRegex(Exception, "引导失败"): # 断言特定异常被引发 (Assert specific exception is raised)
            await crawler.start(port=12345)


    # --- stop 方法测试 --- (stop method tests)
    async def test_stop_calls_server_stop(self, MockServerCls):
        # 测试 stop 方法是否调用服务器的 stop (Test if stop method calls server's stop)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        mock_server_instance.stop = MagicMock() 
        crawler.server = mock_server_instance 
        
        await crawler.stop() 
        mock_server_instance.stop.assert_called_once() 
        self.assertIsNone(crawler.server) 

    async def test_stop_when_server_not_running(self, MockServerCls):
        # 测试当服务器未运行时调用 stop (Test calling stop when server is not running)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        crawler.server = None 
        await crawler.stop() 
        MockServerCls.return_value.stop.assert_not_called()


    # --- get_peers_for_infohash 测试 --- (get_peers_for_infohash tests)
    async def test_get_peers_success_from_get_iter(self, MockServerCls):
        # 测试通过 get_iter 成功获取对等节点 (Test successful peer retrieval via get_iter)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        crawler.server = mock_server_instance 

        peer1_ip_bytes = socket.inet_aton("1.2.3.4") 
        peer1_port_bytes = struct.pack(">H", 111) 
        peer2_ip_bytes = socket.inet_aton("5.6.7.8")
        peer2_port_bytes = struct.pack(">H", 222)
        compact_peer_info_chunk = peer1_ip_bytes + peer1_port_bytes + peer2_ip_bytes + peer2_port_bytes
        
        async def mock_async_iterator(items_list): # 模拟的异步迭代器 (Mocked async iterator)
            for item in items_list:
                yield item
        
        mock_server_instance.get_iter = MagicMock(return_value=mock_async_iterator([compact_peer_info_chunk]))
        
        peers = await crawler.get_peers_for_infohash(SAMPLE_INFOHASH_BYTES, 5)
        self.assertEqual(len(peers), 2) 
        self.assertIn(("1.2.3.4", 111), peers) 
        self.assertIn(("5.6.7.8", 222), peers)
        mock_server_instance.get_iter.assert_called_once_with(SAMPLE_INFOHASH_BYTES) 

    async def test_get_peers_fallback_to_find_neighbors(self, MockServerCls):
        # 测试当 get_iter 未返回对等节点时，回退到 find_neighbors
        # (Test fallback to find_neighbors when get_iter returns no peers)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        crawler.server = mock_server_instance

        async def empty_async_iterator(): # 空的异步迭代器 (Empty async iterator)
            if False: yield
        mock_server_instance.get_iter = MagicMock(return_value=empty_async_iterator())

        neighbor1 = Node(os.urandom(20), ip="10.0.0.1", port=1001) 
        neighbor2 = Node(os.urandom(20), ip="10.0.0.2", port=1002)
        
        mock_server_instance.protocol = MagicMock() 
        mock_server_instance.protocol.router = MagicMock()
        mock_server_instance.protocol.router.find_neighbors = AsyncMock(return_value=[neighbor1, neighbor2])
        
        peers = await crawler.get_peers_for_infohash(SAMPLE_INFOHASH_BYTES, 5)
        self.assertEqual(len(peers), 2) 
        self.assertIn(("10.0.0.1", 1001), peers) 
        self.assertIn(("10.0.0.2", 1002), peers)
        mock_server_instance.protocol.router.find_neighbors.assert_called_once() 
        args, kwargs = mock_server_instance.protocol.router.find_neighbors.call_args
        self.assertIsInstance(args[0], Node) 
        self.assertEqual(args[0].id, SAMPLE_INFOHASH_BYTES) 


    async def test_get_peers_max_peers_limit(self, MockServerCls):
        # 测试 max_peers 限制是否生效 (Test if max_peers limit is respected)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        crawler.server = mock_server_instance
        
        peer_data = b''
        for i in range(10): 
            ip = socket.inet_aton(f"1.1.1.{i+1}")
            port = struct.pack(">H", 1000 + i)
            peer_data += ip + port
        
        async def mock_async_iterator_single_item(item_bytes): # 模拟返回单个包含所有对等节点信息的字节串 (Simulate returning a single bytestring with all peer info)
            yield item_bytes
        mock_server_instance.get_iter = MagicMock(return_value=mock_async_iterator_single_item(peer_data))
        
        peers = await crawler.get_peers_for_infohash(SAMPLE_INFOHASH_BYTES, 3) # max_peers = 3
        self.assertEqual(len(peers), 3) 

    async def test_get_peers_no_server(self, MockServerCls):
        # 测试当服务器未运行时 get_peers_for_infohash 的行为
        # (Test behavior of get_peers_for_infohash when server is not running)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        crawler.server = None # 确保服务器未设置 (Ensure server is not set)
        peers = await crawler.get_peers_for_infohash(SAMPLE_INFOHASH_BYTES, 5)
        self.assertEqual(peers, []) # 应该返回空列表 (Should return an empty list)


    # --- get_routing_table_size 测试 --- (get_routing_table_size tests)
    def test_get_routing_table_size_with_get_node_count(self, MockServerCls):
        # 测试使用 get_node_count 成功获取路由表大小
        # (Test successful retrieval of routing table size using get_node_count)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        crawler.server = mock_server_instance
        
        mock_server_instance.protocol = MagicMock()
        mock_server_instance.protocol.router = MagicMock()
        # 模拟 get_node_count 方法存在且返回一个值
        # (Simulate get_node_count method exists and returns a value)
        mock_server_instance.protocol.router.get_node_count = MagicMock(return_value=125) 
        
        size = crawler.get_routing_table_size()
        self.assertEqual(size, 125) 
        mock_server_instance.protocol.router.get_node_count.assert_called_once() 

    def test_get_routing_table_size_fallback_to_buckets(self, MockServerCls):
        # 测试当 get_node_count 不可用时，回退到 KBuckets
        # (Test fallback to KBuckets when get_node_count is not available)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        mock_server_instance = MockServerCls.return_value
        crawler.server = mock_server_instance

        mock_server_instance.protocol = MagicMock()
        mock_server_instance.protocol.router = MagicMock()
        # 模拟 get_node_count 不存在 (Simulate get_node_count not existing by deleting it)
        del mock_server_instance.protocol.router.get_node_count 

        mock_bucket1 = MagicMock()
        mock_bucket1.get_nodes = MagicMock(return_value=[Node(os.urandom(20)), Node(os.urandom(20))]) 
        mock_bucket2 = MagicMock()
        mock_bucket2.get_nodes = MagicMock(return_value=[Node(os.urandom(20))]) 
        mock_server_instance.protocol.router.buckets = [mock_bucket1, mock_bucket2] 
        
        size = crawler.get_routing_table_size()
        self.assertEqual(size, 3) 

    def test_get_routing_table_size_no_server(self, MockServerCls):
        # 测试当服务器未运行时获取路由表大小 (Test getting routing table size when server is not running)
        crawler = DHTCrawler(infohash_queue=self.mock_queue, node_id=self.node_id)
        crawler.server = None 
        size = crawler.get_routing_table_size()
        self.assertEqual(size, 0) 

if __name__ == '__main__':
    unittest.main() # 运行测试 (Run tests)

```
