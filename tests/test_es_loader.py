import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import datetime
import binascii
import hashlib

# 确保 bencodepy 和 elasticsearch 库可用 (Ensure bencodepy and elasticsearch libraries are available)
# 在实际测试环境中，这些应该是已安装的依赖项 (In a real test environment, these should be installed dependencies)
import bencodepy
from elasticsearch import exceptions as es_exceptions

# 从目标模块导入类和函数 (Import classes and functions from the target module)
# 假设 es_loader.py 在项目根目录下或 PYTHONPATH 中 (Assuming es_loader.py is in project root or PYTHONPATH)
# 为了使测试能够找到 es_loader，我们可能需要调整 Python 路径
# (To enable tests to find es_loader, we might need to adjust Python path)
# 对于这个环境，我将假设它可以直接导入 (For this environment, I'll assume it can be imported directly)
from es_loader import ElasticsearchLoader, load_es_config, DEFAULT_ES_INDEX_NAME

class TestLoadEsConfig(unittest.TestCase):
    """
    测试 es_loader.py 中的 load_es_config 函数。
    (Tests for the load_es_config function in es_loader.py.)
    """

    @patch("builtins.open", new_callable=mock_open, read_data='{"elasticsearch": {"host": "localhost", "port": 9200, "scheme": "http"}}')
    def test_load_valid_config(self, mock_file):
        # 测试加载有效的配置文件 (Test loading a valid configuration file)
        config = load_es_config("dummy_path/valid_config.json")
        self.assertIsNotNone(config) # 断言配置不为 None (Assert config is not None)
        self.assertEqual(config["host"], "localhost") # 断言主机名正确 (Assert hostname is correct)
        self.assertEqual(config["port"], 9200) # 断言端口号正确 (Assert port number is correct)
        self.assertEqual(config["scheme"], "http") # 断言协议正确 (Assert scheme is correct)
        mock_file.assert_called_once_with("dummy_path/valid_config.json", 'r', encoding='utf-8') # 验证文件打开调用 (Verify file open call)

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_missing_config_file(self, mock_file):
        # 测试当配置文件不存在时的情况 (Test case when configuration file does not exist)
        config = load_es_config("dummy_path/non_existent_config.json")
        self.assertIsNone(config) # 断言配置为 None (Assert config is None)
        mock_file.assert_called_once_with("dummy_path/non_existent_config.json", 'r', encoding='utf-8') # 验证文件打开调用 (Verify file open call)

    @patch("builtins.open", new_callable=mock_open, read_data='{"elasticsearch": {"host": "localhost"}}')
    def test_load_config_missing_keys(self, mock_file):
        # 测试配置文件缺少必要的键 (如 'port') (Test configuration file missing necessary keys (e.g., 'port'))
        # load_es_config 本身不强制执行键检查，ElasticsearchLoader 的 __init__ 会处理
        # (load_es_config itself doesn't enforce key checks, ElasticsearchLoader's __init__ handles it)
        config = load_es_config("dummy_path/missing_keys_config.json")
        self.assertIsNotNone(config) # 配置对象本身会被加载 (The config object itself will be loaded)
        self.assertNotIn("port", config) # 'port' 键不存在 (The 'port' key is not present)

    @patch("builtins.open", new_callable=mock_open, read_data='this is not json')
    def test_load_config_invalid_format(self, mock_file):
        # 测试配置文件格式无效 (Test configuration file with invalid format)
        config = load_es_config("dummy_path/invalid_format_config.json")
        self.assertIsNone(config) # 断言配置为 None (Assert config is None)

class TestElasticsearchLoader(unittest.TestCase):
    """
    测试 ElasticsearchLoader 类。
    (Tests for the ElasticsearchLoader class.)
    """

    def setUp(self):
        # 通用设置，例如创建 ElasticsearchLoader 实例时使用的有效配置
        # (Common setup, e.g., a valid configuration used for creating ElasticsearchLoader instances)
        self.valid_es_config = {"host": "localhost", "port": 9200, "scheme": "http"}
        # 创建一个假的 infohash 用于测试 (Create a fake infohash for testing)
        self.sample_infohash_hex = "0123456789abcdef0123456789abcdef01234567"
        self.sample_infohash_bytes = binascii.unhexlify(self.sample_infohash_hex)

        # 示例 'info' 字典和元信息 (Sample 'info' dictionary and metainfo)
        self.sample_info_dict = {
            b'name': b'My Test Torrent',
            b'piece length': 262144,
            b'pieces': b'01234567890123456789', # 20 字节的 SHA1 哈希 (20-byte SHA1 hash)
            b'length': 1234567 
        }
        self.sample_metainfo_bencoded = {
            b'info': self.sample_info_dict,
            b'announce': b'udp://tracker.example.com:80',
            b'creation date': int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            b'comment': b'Test comment \xe4\xb8\xad\xe6\x96\x87', # 包含中文的评论 (Comment with Chinese characters)
            b'encoding': b'UTF-8'
        }
        self.sample_metainfo_bytes = bencodepy.encode(self.sample_metainfo_bencoded)

    @patch('es_loader.Elasticsearch') # 模拟 Elasticsearch 客户端库 (Mock Elasticsearch client library)
    def test_init_success(self, MockElasticsearch):
        # 测试 ElasticsearchLoader 成功初始化 (Test successful initialization of ElasticsearchLoader)
        mock_es_instance = MockElasticsearch.return_value # 获取模拟的 Elasticsearch 实例 (Get the mocked Elasticsearch instance)
        mock_es_instance.ping.return_value = True # 模拟 ping 成功 (Simulate successful ping)
        
        loader = ElasticsearchLoader(self.valid_es_config)
        self.assertIsNotNone(loader.es) # 断言 Elasticsearch 客户端已创建 (Assert Elasticsearch client was created)
        MockElasticsearch.assert_called_once_with(
            [f"{self.valid_es_config['scheme']}://{self.valid_es_config['host']}:{self.valid_es_config['port']}"],
            timeout=10
        ) # 验证 Elasticsearch 客户端的调用参数 (Verify Elasticsearch client call arguments)
        # mock_es_instance.ping.assert_called_once() # Ping 操作移至 main_crawler.py (Ping operation moved to main_crawler.py)

    @patch('es_loader.Elasticsearch')
    def test_init_ping_false(self, MockElasticsearch):
        # 测试 ElasticsearchLoader 初始化时 ping 失败 (Test ElasticsearchLoader initialization with ping failure)
        mock_es_instance = MockElasticsearch.return_value
        mock_es_instance.ping.return_value = False # 模拟 ping 失败 (Simulate ping failure)
        
        # 即使 ping 失败，初始化也应该继续，但会记录警告
        # (Even if ping fails, initialization should proceed, but a warning will be logged)
        loader = ElasticsearchLoader(self.valid_es_config)
        self.assertIsNotNone(loader.es)
        # mock_es_instance.ping.assert_called_once()

    def test_init_invalid_config(self):
        # 测试使用无效配置初始化 ElasticsearchLoader (Test ElasticsearchLoader initialization with invalid configuration)
        with self.assertRaises(ValueError): # 断言会引发 ValueError (Assert ValueError is raised)
            ElasticsearchLoader({"host": "localhost"}) # 缺少 'port' (Missing 'port')

    @patch('es_loader.Elasticsearch')
    def test_init_connection_error(self, MockElasticsearch):
        # 测试初始化期间发生 ConnectionError (Test ConnectionError during initialization)
        MockElasticsearch.side_effect = es_exceptions.ConnectionError("Test Connection Error")
        with self.assertRaises(es_exceptions.ConnectionError): # 断言会引发 ConnectionError (Assert ConnectionError is raised)
            ElasticsearchLoader(self.valid_es_config)

    # --- 测试 _parse_metainfo --- (Tests for _parse_metainfo)
    def test_parse_valid_metainfo(self):
        # 测试解析有效的 bencoded 元信息 (Test parsing valid bencoded metainfo)
        loader = ElasticsearchLoader(self.valid_es_config) # 需要一个 loader 实例来调用受保护的方法 (Need a loader instance to call protected method)
        parsed = loader._parse_metainfo(self.sample_metainfo_bytes, self.sample_infohash_hex)
        self.assertIsNotNone(parsed) # 断言解析结果不为 None (Assert parsed result is not None)
        self.assertEqual(parsed[b'info'][b'name'], b'My Test Torrent') # 验证字段内容 (Verify field content)

    def test_parse_invalid_bencode(self):
        # 测试解析无效的 bencoded 数据 (Test parsing invalid bencoded data)
        loader = ElasticsearchLoader(self.valid_es_config)
        invalid_data = b'invalid_bencode_data' # 无效的 bencode 格式 (Invalid bencode format)
        parsed = loader._parse_metainfo(invalid_data, self.sample_infohash_hex)
        self.assertIsNone(parsed) # 断言解析失败，返回 None (Assert parsing fails, returns None)

    def test_parse_non_dict_metainfo(self):
        # 测试解析顶层不是字典的有效 bencoded 数据 (Test parsing valid bencoded data that is not a dict at top level)
        loader = ElasticsearchLoader(self.valid_es_config)
        non_dict_data = bencodepy.encode([b'list_item']) # 编码一个列表而非字典 (Encode a list instead of a dict)
        parsed = loader._parse_metainfo(non_dict_data, self.sample_infohash_hex)
        self.assertIsNone(parsed) # 断言解析失败，返回 None (Assert parsing fails, returns None)

    # --- 测试 _prepare_document --- (Tests for _prepare_document)
    def test_prepare_single_file_torrent(self):
        # 测试准备单文件种子的文档 (Test preparing document for a single-file torrent)
        loader = ElasticsearchLoader(self.valid_es_config)
        doc = loader._prepare_document(self.sample_infohash_hex, self.sample_metainfo_bencoded)
        self.assertIsNotNone(doc) # 断言文档已创建 (Assert document was created)
        self.assertEqual(doc["infohash"], self.sample_infohash_hex) # 验证 infohash (Verify infohash)
        self.assertEqual(doc["name"], "My Test Torrent") # 验证名称 (Verify name)
        self.assertEqual(doc["length"], 1234567) # 验证长度 (Verify length)
        self.assertNotIn("files", doc) # 单文件种子不应有 'files' 字段 (Single-file torrent should not have 'files' field)
        self.assertNotIn("comment", doc)
        self.assertNotIn("announce", doc)
        self.assertNotIn("private", doc)
        self.assertNotIn("created_by", doc)

    def test_prepare_multi_file_torrent(self):
        # 测试准备多文件种子的文档 (Test preparing document for a multi-file torrent)
        loader = ElasticsearchLoader(self.valid_es_config)
        multi_file_info = {
            b'name': b'Multi File Test \xe6\xb5\x8b\xe8\xaf\x95', # 名称包含中文 (Name with Chinese characters)
            b'piece length': 1024,
            b'pieces': b'x'*20,
            b'files': [
                {b'length': 100, b'path': [b'dir1', b'file1.txt']},
                {b'length': 200, b'path': [b'file2.txt']}
            ]
        }
        multi_file_metainfo = {b'info': multi_file_info}
        doc = loader._prepare_document(self.sample_infohash_hex, multi_file_metainfo)
        self.assertIsNotNone(doc)
        self.assertEqual(doc["name"], "Multi File Test 测试") # 验证名称和中文解码 (Verify name and Chinese decoding)
        self.assertEqual(doc["length"], 300) # 验证总长度 (Verify total length)
        self.assertIsInstance(doc["files"], list) # 断言 'files' 是列表 (Assert 'files' is a list)
        self.assertEqual(len(doc["files"]), 2) # 断言文件数量 (Assert number of files)
        self.assertDictEqual(doc["files"][0], {"path": "dir1/file1.txt", "length": 100}) # 验证第一个文件信息 (Verify first file info)
        self.assertDictEqual(doc["files"][1], {"path": "file2.txt", "length": 200}) # 验证第二个文件信息 (Verify second file info)

    def test_prepare_torrent_with_all_fields(self):
        # 测试包含所有可能字段的种子文档准备 (Test document preparation for torrent with all possible fields)
        loader = ElasticsearchLoader(self.valid_es_config)
        now_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        complex_metainfo = {
            b'info': {
                b'name': b'Complex Torrent',
                b'length': 5000,
                b'private': 1, # 私有种子 (Private torrent)
                b'source': b'unit test source' # Trackerless 种子来源 (Source for trackerless torrent)
            },
            b'announce': b'http://tracker.one/announce',
            b'announce-list': [[b'http://tracker.one/announce'], [b'udp://tracker.two/announce']],
            b'creation date': now_ts,
            b'comment': b'Complex torrent comment',
            b'created by': b'TestClient v1.0',
            b'encoding': b'UTF-8'
        }
        doc = loader._prepare_document(self.sample_infohash_hex, complex_metainfo)
        self.assertIsNotNone(doc)
        # Verify fields that should still be present
        self.assertEqual(doc["name"], "Complex Torrent")
        self.assertEqual(doc["length"], 5000)
        self.assertIsInstance(doc["announce_list"], list) # 断言 announce-list 是列表 (Assert announce-list is a list)
        self.assertIn({"url": "http://tracker.one/announce"}, doc["announce_list"]) # 验证 announce-list 内容 (Verify announce-list content)
        self.assertIn({"url": "udp://tracker.two/announce"}, doc["announce_list"])
        self.assertEqual(doc["creation_date"], datetime.datetime.fromtimestamp(now_ts, datetime.timezone.utc).isoformat()) # 验证创建日期 (Verify creation date)
        self.assertEqual(doc["encoding"], "UTF-8")
        self.assertEqual(doc["source"], "unit test source") # 验证来源 (Verify source)
        
        # Verify fields that should NOT be present
        self.assertNotIn("comment", doc)
        self.assertNotIn("announce", doc)
        self.assertNotIn("private", doc)
        self.assertNotIn("created_by", doc)

    def test_prepare_torrent_missing_info_dict(self):
        # 测试当 'info' 字典缺失时的处理 (Test handling when 'info' dictionary is missing)
        loader = ElasticsearchLoader(self.valid_es_config)
        metainfo_no_info = {b'announce': b'http://tracker.one/announce'} # 没有 'info' 字典 (No 'info' dictionary)
        doc = loader._prepare_document(self.sample_infohash_hex, metainfo_no_info)
        self.assertIsNone(doc) # 应该返回 None，因为 'info' 是必需的 (Should return None as 'info' is essential)

    # --- 测试 store_metainfo --- (Tests for store_metainfo)
    @patch.object(ElasticsearchLoader, '_parse_metainfo')
    @patch.object(ElasticsearchLoader, '_prepare_document')
    @patch('es_loader.Elasticsearch') # 模拟 Elasticsearch 客户端类本身 (Mock the Elasticsearch client class itself)
    def test_store_metainfo_success(self, MockESClient, mock_prepare, mock_parse):
        # 测试元信息成功存储 (Test successful storage of metainfo)
        mock_es_instance = MockESClient.return_value # 获取模拟的 Elasticsearch 实例 (Get the mocked Elasticsearch instance)
        mock_es_instance.index.return_value = {"result": "created", "_id": self.sample_infohash_hex, "_version": 1} # 模拟成功的索引响应 (Simulate successful index response)
        
        parsed_dict = {"key": "parsed_value"} # 模拟的解析结果 (Simulated parsed result)
        prepared_doc = {"key": "prepared_value"} # 模拟的准备好的文档 (Simulated prepared document)
        mock_parse.return_value = parsed_dict
        mock_prepare.return_value = prepared_doc

        loader = ElasticsearchLoader(self.valid_es_config)
        loader.es = mock_es_instance # 将模拟实例分配给加载器 (Assign mocked instance to loader)
        
        success = loader.store_metainfo(self.sample_infohash_bytes, self.sample_metainfo_bytes)
        
        self.assertTrue(success) # 断言存储成功 (Assert storage was successful)
        mock_parse.assert_called_once_with(self.sample_metainfo_bytes, self.sample_infohash_hex) # 验证 _parse_metainfo 调用 (Verify _parse_metainfo call)
        mock_prepare.assert_called_once_with(self.sample_infohash_hex, parsed_dict) # 验证 _prepare_document 调用 (Verify _prepare_document call)
        mock_es_instance.index.assert_called_once_with(
            index=DEFAULT_ES_INDEX_NAME,
            id=self.sample_infohash_hex,
            document=prepared_doc
        ) # 验证 es.index 调用参数 (Verify es.index call arguments)

    @patch('es_loader.Elasticsearch')
    def test_store_metainfo_connection_error(self, MockESClient):
        # 测试存储时发生 ConnectionError (Test ConnectionError during storage)
        mock_es_instance = MockESClient.return_value
        mock_es_instance.index.side_effect = es_exceptions.ConnectionError("ES Connection Failed") # 模拟连接错误 (Simulate connection error)

        loader = ElasticsearchLoader(self.valid_es_config)
        loader.es = mock_es_instance
        
        success = loader.store_metainfo(self.sample_infohash_hex, self.sample_metainfo_bytes)
        self.assertFalse(success) # 断言存储失败 (Assert storage failed)

    @patch('es_loader.Elasticsearch')
    def test_store_metainfo_transport_error_400(self, MockESClient):
        # 测试存储时发生 TransportError 400 (通常是映射问题) (Test TransportError 400 (usually mapping issue) during storage)
        mock_es_instance = MockESClient.return_value
        # 模拟 TransportError (Simulate TransportError)
        mock_es_instance.index.side_effect = es_exceptions.TransportError("Simulated Transport Error", status_code=400, info={"error": "mapping_error"})

        loader = ElasticsearchLoader(self.valid_es_config)
        loader.es = mock_es_instance
        
        success = loader.store_metainfo(self.sample_infohash_hex, self.sample_metainfo_bytes)
        self.assertFalse(success) # 断言存储失败 (Assert storage failed)

    def test_store_metainfo_parse_failure(self):
        # 测试当 _parse_metainfo 返回 None 时 store_metainfo 的行为
        # (Test behavior of store_metainfo when _parse_metainfo returns None)
        with patch.object(ElasticsearchLoader, '_parse_metainfo', return_value=None) as mock_parse:
            loader = ElasticsearchLoader(self.valid_es_config) # 初始化加载器 (Initialize loader)
            success = loader.store_metainfo(self.sample_infohash_hex, self.sample_metainfo_bytes)
            self.assertFalse(success) # 断言存储失败 (Assert storage failed)
            mock_parse.assert_called_once() # 验证 _parse_metainfo 被调用 (Verify _parse_metainfo was called)

    def test_store_metainfo_prepare_failure(self):
        # 测试当 _prepare_document 返回 None 时 store_metainfo 的行为
        # (Test behavior of store_metainfo when _prepare_document returns None)
        # _parse_metainfo 需要返回一个字典以进行到 _prepare_document 的流程
        # (_parse_metainfo needs to return a dict to proceed to _prepare_document)
        with patch.object(ElasticsearchLoader, '_parse_metainfo', return_value=self.sample_metainfo_bencoded), \
             patch.object(ElasticsearchLoader, '_prepare_document', return_value=None) as mock_prepare:
            loader = ElasticsearchLoader(self.valid_es_config) # 初始化加载器 (Initialize loader)
            success = loader.store_metainfo(self.sample_infohash_hex, self.sample_metainfo_bytes)
            self.assertFalse(success) # 断言存储失败 (Assert storage failed)
            mock_prepare.assert_called_once() # 验证 _prepare_document 被调用 (Verify _prepare_document was called)

    def test_store_metainfo_invalid_infohash_type(self):
        # 测试使用无效类型的 infohash 调用 store_metainfo
        # (Test calling store_metainfo with invalid type for infohash)
        loader = ElasticsearchLoader(self.valid_es_config)
        success = loader.store_metainfo(12345, self.sample_metainfo_bytes) # 传递整数 infohash (Pass integer infohash)
        self.assertFalse(success) # 断言应失败 (Assert should fail)

    def test_store_metainfo_invalid_infohash_length(self):
        # 测试使用无效长度的 infohash 调用 store_metainfo
        # (Test calling store_metainfo with invalid length for infohash)
        loader = ElasticsearchLoader(self.valid_es_config)
        success = loader.store_metainfo("shorthex", self.sample_metainfo_bytes) # 传递短的十六进制 infohash (Pass short hex infohash)
        self.assertFalse(success) # 断言应失败 (Assert should fail)


if __name__ == '__main__':
    unittest.main() # 运行测试 (Run tests)

```
