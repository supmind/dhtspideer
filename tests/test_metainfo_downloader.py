import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock, call # 导入 call 用于检查调用顺序 (Import call for checking call order)
import hashlib
import struct
import bencodepy # 用于创建测试数据 (For creating test data)
import os # 用于生成随机 peer_id (For generating random peer_id)

# 假设 metainfo_downloader.py 在项目的根目录或 PYTHONPATH 中
# (Assuming metainfo_downloader.py is in the project root or PYTHONPATH)
from metainfo_downloader import MetainfoDownloader, PSTR, PSTRLEN, RESERVED_BYTES, EXTENDED_MESSAGE_ID, BLOCK_SIZE

# --- 辅助数据和函数 --- (Helper Data and Functions)

# 示例 Infohash 和 Peer ID (Sample Infohash and Peer ID)
SAMPLE_INFOHASH_HEX = "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33" # Ubuntu 20.04 ISO
SAMPLE_INFOHASH = bytes.fromhex(SAMPLE_INFOHASH_HEX)
CLIENT_PEER_ID = b"-MC0001-" + os.urandom(12) # 测试客户端的 Peer ID (Peer ID for the test client)
OTHER_PEER_ID = b"-OT0001-" + os.urandom(12)  # 模拟对等节点的 Peer ID (Peer ID for the simulated peer)

# 示例元信息 'info' 字典 (Sample metainfo 'info' dictionary)
SAMPLE_INFO_DICT = {
    b'name': b'test_torrent.txt',
    b'piece length': BLOCK_SIZE, # 使用与下载器相同的块大小 (Use same block size as downloader)
    b'pieces': hashlib.sha1(b'piece1data').digest() + hashlib.sha1(b'piece2data').digest(), # 模拟的块哈希 (Simulated piece hashes)
    b'length': BLOCK_SIZE + 5 # 总长度，例如一个完整块 + 5 字节 (Total length, e.g., one full block + 5 bytes)
}
# 完整的元信息字典 (Complete metainfo dictionary)
SAMPLE_METADATA_DICT = {
    b'info': SAMPLE_INFO_DICT,
    b'announce': b'http://example.com/announce'
}
# bencoded 元信息 (Bencoded metainfo)
SAMPLE_METADATA_BYTES = bencodepy.encode(SAMPLE_METADATA_DICT)
METADATA_SIZE = len(SAMPLE_METADATA_BYTES)

# 元信息块 (Metainfo pieces)
# 假设 SAMPLE_METADATA_BYTES 分为两块 (Assume SAMPLE_METADATA_BYTES is split into two pieces)
PIECE_0_DATA = SAMPLE_METADATA_BYTES[:BLOCK_SIZE]
PIECE_1_DATA = SAMPLE_METADATA_BYTES[BLOCK_SIZE:] # 剩余部分 (Remaining part)


async def mock_reader_writer_factory(responses: list[bytes | Exception]):
    """
    创建一个模拟的 StreamReader 和 StreamWriter 对。
    (Creates a mock StreamReader and StreamWriter pair.)
    `responses` 是一个字节串或异常的列表，StreamReader 将按顺序返回它们。
    (`responses` is a list of byte strings or Exceptions that the StreamReader will return in order.)
    """
    mock_reader = AsyncMock(spec=asyncio.StreamReader)
    # 使用 AsyncMock 配置 readexactly (Configure readexactly using AsyncMock)
    
    # readexactly 的 side_effect 列表 (side_effect list for readexactly)
    read_effects = []
    for resp in responses:
        if isinstance(resp, Exception):
            read_effects.append(resp)
        else:
            # 模拟 readexactly(n) 的行为 (Simulate behavior of readexactly(n))
            # readexactly 需要一个协程函数作为 side_effect 的一部分
            # (readexactly needs a coroutine function as part of its side_effect)
            async def side_effect_func(data_to_return): # 捕获 data_to_return (Capture data_to_return)
                # 这个内部函数将实际被调用 (This inner function will actually be called)
                # readexactly 的第一个参数是 n (字节数) (First argument to readexactly is n (number of bytes))
                # 我们忽略它，因为我们是基于预设响应来模拟的
                # (We ignore it as we are simulating based on preset responses)
                return data_to_return
            
            # functools.partial 不能直接用于异步函数作为 AsyncMock 的 side_effect
            # (functools.partial cannot be directly used with async functions as side_effect for AsyncMock)
            # 因此，我们创建一个 lambda 来包装它 (So, we create a lambda to wrap it)
            read_effects.append(lambda n_ignored, data=resp: side_effect_func(data))


    mock_reader.readexactly = AsyncMock(side_effect=read_effects)
    
    mock_writer = AsyncMock(spec=asyncio.StreamWriter)
    mock_writer.drain = AsyncMock() # drain 是一个协程 (drain is a coroutine)
    mock_writer.close = MagicMock() # close 不是协程 (close is not a coroutine)
    mock_writer.wait_closed = AsyncMock() # wait_closed 是一个协程 (wait_closed is a coroutine)
    mock_writer.is_closing = MagicMock(return_value=False) # is_closing 不是协程 (is_closing is not a coroutine)

    return mock_reader, mock_writer

class TestMetainfoDownloader(unittest.IsolatedAsyncioTestCase):
    """
    测试 MetainfoDownloader 类。
    (Tests for the MetainfoDownloader class.)
    """

    def setUp(self):
        # 通用设置 (Common setup)
        self.infohash = SAMPLE_INFOHASH
        self.peer_id = CLIENT_PEER_ID
        # 创建一个下载器实例用于测试，infohash 和 peer_id 将在每个测试中根据需要进行设置
        # (Create a downloader instance for testing, infohash and peer_id will be set as needed in each test)
        # 注意：MetainfoDownloader 的 __init__ 是同步的
        # (Note: MetainfoDownloader's __init__ is synchronous)
        self.downloader = MetainfoDownloader(infohash=self.infohash, peer_id=self.peer_id)
        
        # 用于模拟 asyncio.open_connection 的补丁 (Patch for mocking asyncio.open_connection)
        self.open_connection_patch = patch('asyncio.open_connection')
        self.mock_open_connection = self.open_connection_patch.start() # 启动补丁 (Start the patch)
        
        # 用于模拟 asyncio.wait_for 的补丁 (Patch for mocking asyncio.wait_for)
        # 这允许我们模拟超时而不实际等待 (This allows us to simulate timeouts without actual waiting)
        async def fake_wait_for(fut, timeout):
            # 如果 fut 是一个异常，则引发它 (If fut is an exception, raise it)
            if isinstance(fut, Exception):
                raise fut
            try:
                # 尝试在短时间内等待 future (Try to await the future for a short duration)
                # 这允许已完成的 future 通过，并使 side_effect 更容易管理
                # (This allows already completed futures to pass through and makes side_effect management easier)
                return await asyncio.wait_for(fut, timeout=0.01) 
            except asyncio.TimeoutError as e:
                # 如果原始 future 应该超时，则引发我们模拟的 TimeoutError
                # (If the original future was supposed to timeout, raise our simulated TimeoutError)
                # 这需要 fut 是一个 TimeoutError 实例
                # (This requires fut to be a TimeoutError instance)
                # 对于这个简单的 fake_wait_for，我们直接重新引发原始超时
                # (For this simple fake_wait_for, we just re-raise the original timeout)
                raise e # 重新引发原始 asyncio.TimeoutError (Re-raise original asyncio.TimeoutError)

        self.wait_for_patch = patch('asyncio.wait_for', side_effect=fake_wait_for)
        # self.mock_wait_for = self.wait_for_patch.start() # 暂时不启动 (Not starting for now)


    def tearDown(self):
        self.open_connection_patch.stop() # 停止补丁 (Stop the patch)
        # self.wait_for_patch.stop() # 停止补丁 (Stop the patch)
        patch.stopall() # 确保所有补丁都已停止 (Ensure all patches are stopped)

    # --- 初始化测试 --- (Initialization Tests)
    def test_downloader_initialization_valid(self):
        # 测试有效参数的初始化 (Test initialization with valid parameters)
        downloader = MetainfoDownloader(infohash=self.infohash, peer_id=self.peer_id)
        self.assertEqual(downloader.infohash, self.infohash) # 验证 infohash (Verify infohash)
        self.assertEqual(downloader.peer_id, self.peer_id) # 验证 peer_id (Verify peer_id)
        self.assertIsNotNone(downloader.loop) # 验证事件循环已设置 (Verify event loop is set)

    def test_downloader_initialization_invalid_infohash(self):
        # 测试 infohash 无效时的初始化 (Test initialization with invalid infohash)
        with self.assertRaises(ValueError): # 断言引发 ValueError (Assert ValueError is raised)
            MetainfoDownloader(infohash=b'short', peer_id=self.peer_id)

    def test_downloader_initialization_invalid_peer_id(self):
        # 测试 peer_id 无效时的初始化 (Test initialization with invalid peer_id)
        with self.assertRaises(ValueError): # 断言引发 ValueError (Assert ValueError is raised)
            MetainfoDownloader(infohash=self.infohash, peer_id=b'short_peer')

    # --- _connect_to_peer 测试 --- (_connect_to_peer Tests)
    async def test_connect_to_peer_success(self):
        # 测试成功连接到对等节点 (Test successful connection to peer)
        mock_reader, mock_writer = await mock_reader_writer_factory([]) # 创建模拟的 reader/writer (Create mock reader/writer)
        self.mock_open_connection.return_value = (mock_reader, mock_writer) # 设置 open_connection 返回值 (Set open_connection return value)
        
        result = await self.downloader._connect_to_peer("1.2.3.4", 12345)
        self.assertIsNotNone(result) # 断言结果不为 None (Assert result is not None)
        self.assertEqual(result, (mock_reader, mock_writer)) # 验证返回的 reader/writer (Verify returned reader/writer)
        self.mock_open_connection.assert_called_once_with("1.2.3.4", 12345) # 验证 open_connection 调用 (Verify open_connection call)

    async def test_connect_to_peer_timeout(self):
        # 测试连接超时 (Test connection timeout)
        self.mock_open_connection.side_effect = asyncio.TimeoutError("连接超时 (Connection timeout)") # 模拟超时 (Simulate timeout)
        
        with patch('asyncio.wait_for', side_effect=asyncio.TimeoutError): # 确保 wait_for 引发超时 (Ensure wait_for raises timeout)
             result = await self.downloader._connect_to_peer("1.2.3.4", 12345)
        self.assertIsNone(result) # 断言结果为 None (Assert result is None)

    async def test_connect_to_peer_refused(self):
        # 测试连接被拒绝 (Test connection refused)
        self.mock_open_connection.side_effect = ConnectionRefusedError("连接被拒绝 (Connection refused)") # 模拟连接被拒绝 (Simulate connection refused)
        result = await self.downloader._connect_to_peer("1.2.3.4", 12345)
        self.assertIsNone(result) # 断言结果为 None (Assert result is None)

    # --- _perform_bittorrent_handshake 测试 --- (_perform_bittorrent_handshake Tests)
    async def test_perform_bittorrent_handshake_success(self):
        # 测试成功的 BitTorrent 握手 (Test successful BitTorrent handshake)
        # 准备对等节点的响应：pstrlen, pstr, reserved (支持扩展), infohash, peer_id
        # (Prepare peer's response: pstrlen, pstr, reserved (supports extended), infohash, peer_id)
        peer_response = [
            bytes([PSTRLEN]), # pstrlen
            PSTR, # pstr
            RESERVED_BYTES, # reserved bytes (bit 5, 0x10 表示扩展支持) (reserved bytes (bit 5, 0x10 for extended support))
            self.infohash, # infohash (与我们发送的匹配) (infohash (matches ours))
            OTHER_PEER_ID  # 对等节点的 peer_id (peer's peer_id)
        ]
        mock_reader, mock_writer = await mock_reader_writer_factory(peer_response)
        
        success = await self.downloader._perform_bittorrent_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertTrue(success) # 断言握手成功 (Assert handshake successful)
        # 验证写入的数据 (Verify data written)
        # 构造期望的握手消息 (Construct expected handshake message)
        expected_handshake_msg = struct.pack('>B', PSTRLEN) + PSTR + RESERVED_BYTES + self.infohash + self.peer_id
        mock_writer.write.assert_called_once_with(expected_handshake_msg) # 验证写入的消息 (Verify written message)
        mock_writer.drain.assert_called_once() # 验证 drain 被调用 (Verify drain was called)

    async def test_perform_bittorrent_handshake_no_extended_support(self):
        # 测试对等节点不支持扩展消息的情况 (Test peer does not support extended messages)
        no_ext_reserved = b'\x00\x00\x00\x00\x00\x00\x00\x00' # 第 6 字节的第 4 位未设置 (Bit 4 of 6th byte not set)
        peer_response = [bytes([PSTRLEN]), PSTR, no_ext_reserved, self.infohash, OTHER_PEER_ID]
        mock_reader, mock_writer = await mock_reader_writer_factory(peer_response)
        
        success = await self.downloader._perform_bittorrent_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertFalse(success) # 断言握手失败 (Assert handshake failed)

    async def test_perform_bittorrent_handshake_wrong_infohash(self):
        # 测试对等节点返回错误的 infohash (Test peer returns wrong infohash)
        wrong_infohash = os.urandom(20) # 随机错误的 infohash (Random wrong infohash)
        peer_response = [bytes([PSTRLEN]), PSTR, RESERVED_BYTES, wrong_infohash, OTHER_PEER_ID]
        mock_reader, mock_writer = await mock_reader_writer_factory(peer_response)
        
        # downloader 应该记录一个警告但仍然认为握手成功，因为 infohash 验证是在元信息层面
        # (downloader should log a warning but still consider handshake successful, as infohash verification is at metainfo level)
        success = await self.downloader._perform_bittorrent_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertTrue(success) # 断言握手（协议层面）成功 (Assert handshake (protocol level) successful)

    async def test_perform_bittorrent_handshake_timeout(self):
        # 测试握手期间超时 (Test timeout during handshake)
        # 模拟 readexactly 在读取 pstrlen 时超时 (Simulate readexactly timing out on pstrlen)
        mock_reader, mock_writer = await mock_reader_writer_factory([asyncio.TimeoutError("握手超时 (Handshake timeout)")])
        
        with patch('asyncio.wait_for', side_effect=asyncio.TimeoutError):
            success = await self.downloader._perform_bittorrent_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertFalse(success) # 断言握手失败 (Assert handshake failed)

    # --- _send_extended_handshake 测试 --- (_send_extended_handshake Tests)
    async def test_send_extended_handshake_success(self):
        # 测试成功的扩展握手 (Test successful extended handshake)
        peer_ut_metadata_id = 3 # 对等节点选择的 ut_metadata ID (Peer's chosen ut_metadata ID)
        peer_ext_handshake_response_dict = {
            b'm': {b'ut_metadata': peer_ut_metadata_id},
            b'metadata_size': METADATA_SIZE,
            b'v': b'PeerClient v1.0' # 对等节点的客户端版本 (Peer's client version)
        }
        encoded_peer_response_payload = bencodepy.encode(peer_ext_handshake_response_dict)
        # 构造对等节点的完整扩展握手消息 (Construct peer's full extended handshake message)
        # <length_prefix> <EXTENDED_MESSAGE_ID> <extended_msg_type=0> <payload>
        peer_msg_payload = bytes([0]) + encoded_peer_response_payload # 0 表示握手 (0 for handshake)
        peer_full_msg = struct.pack('>I', len(peer_msg_payload) + 1) + bytes([EXTENDED_MESSAGE_ID]) + peer_msg_payload
        
        mock_reader, mock_writer = await mock_reader_writer_factory([peer_full_msg])
        
        result = await self.downloader._send_extended_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertIsNotNone(result) # 断言结果不为 None (Assert result is not None)
        self.assertEqual(result, (peer_ut_metadata_id, METADATA_SIZE)) # 验证返回的 ID 和大小 (Verify returned ID and size)

        # 验证我们发送的扩展握手 (Verify our sent extended handshake)
        # 期望的载荷: {'m': {'ut_metadata': 1}} (Expected payload: {'m': {'ut_metadata': 1}})
        expected_sent_dict = {b'm': {b'ut_metadata': 1}}
        expected_encoded_payload = bencodepy.encode(expected_sent_dict)
        expected_sent_msg_content = bytes([0]) + expected_encoded_payload # 0 是握手ID (0 is handshake ID)
        expected_full_sent_msg = struct.pack('>I', len(expected_sent_msg_content) + 1) + bytes([EXTENDED_MESSAGE_ID]) + expected_sent_msg_content
        mock_writer.write.assert_called_once_with(expected_full_sent_msg) # 验证写入的消息 (Verify written message)

    async def test_send_extended_handshake_no_ut_metadata_support(self):
        # 测试对等节点不支持 ut_metadata (Test peer does not support ut_metadata)
        peer_ext_handshake_response_dict = {b'm': {b'ut_other_ext': 1}, b'metadata_size': 100} # 没有 ut_metadata (No ut_metadata)
        encoded_payload = bencodepy.encode(peer_ext_handshake_response_dict)
        peer_msg = struct.pack('>I', len(bytes([0]) + encoded_payload) + 1) + bytes([EXTENDED_MESSAGE_ID, 0]) + encoded_payload
        mock_reader, mock_writer = await mock_reader_writer_factory([peer_msg])
        
        result = await self.downloader._send_extended_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertIsNone(result) # 断言结果为 None (Assert result is None)

    async def test_send_extended_handshake_missing_metadata_size(self):
        # 测试对等节点未发送 metadata_size (Test peer does not send metadata_size)
        peer_ext_handshake_response_dict = {b'm': {b'ut_metadata': 2}} # 缺少 metadata_size (Missing metadata_size)
        encoded_payload = bencodepy.encode(peer_ext_handshake_response_dict)
        peer_msg = struct.pack('>I', len(bytes([0]) + encoded_payload) + 1) + bytes([EXTENDED_MESSAGE_ID, 0]) + encoded_payload
        mock_reader, mock_writer = await mock_reader_writer_factory([peer_msg])

        result = await self.downloader._send_extended_handshake(mock_reader, mock_writer, "1.2.3.4:12345")
        self.assertIsNone(result) # 断言结果为 None (Assert result is None)

    # --- _request_metadata_pieces 测试 --- (_request_metadata_pieces Tests)
    async def test_request_metadata_pieces_success(self):
        # 测试成功下载所有元信息块 (Test successful download of all metadata pieces)
        peer_ut_metadata_id = 2 # 对等节点使用的 ut_metadata ID (ut_metadata ID used by peer)
        
        # 准备对等节点的响应 (Prepare peer responses)
        # 响应块0 (Response for piece 0)
        piece0_resp_dict = {b'msg_type': 1, b'piece': 0, b'total_size': METADATA_SIZE}
        encoded_piece0_info = bencodepy.encode(piece0_resp_dict)
        msg0_payload = bytes([peer_ut_metadata_id]) + encoded_piece0_info + PIECE_0_DATA
        msg0_full = struct.pack('>I', len(msg0_payload) + 1) + bytes([EXTENDED_MESSAGE_ID]) + msg0_payload
        
        # 响应块1 (Response for piece 1)
        piece1_resp_dict = {b'msg_type': 1, b'piece': 1, b'total_size': METADATA_SIZE}
        encoded_piece1_info = bencodepy.encode(piece1_resp_dict)
        msg1_payload = bytes([peer_ut_metadata_id]) + encoded_piece1_info + PIECE_1_DATA
        msg1_full = struct.pack('>I', len(msg1_payload) + 1) + bytes([EXTENDED_MESSAGE_ID]) + msg1_payload

        mock_reader, mock_writer = await mock_reader_writer_factory([msg0_full, msg1_full])
        
        result_metadata = await self.downloader._request_metadata_pieces(
            mock_reader, mock_writer, peer_ut_metadata_id, METADATA_SIZE, "1.2.3.4:12345"
        )
        self.assertEqual(result_metadata, SAMPLE_METADATA_BYTES) # 验证下载的元信息 (Verify downloaded metadata)

        # 验证发送的请求 (Verify sent requests)
        # 应该有两个请求 (There should be two requests)
        self.assertEqual(mock_writer.write.call_count, 2) # 验证调用次数 (Verify call count)
        # 验证第一个请求 (Verify first request)
        expected_req0_dict = {b'msg_type': 0, b'piece': 0}
        encoded_req0_payload = bencodepy.encode(expected_req0_dict)
        expected_req0_msg_content = bytes([peer_ut_metadata_id]) + encoded_req0_payload
        expected_req0_full_msg = struct.pack('>I', len(expected_req0_msg_content) + 1) + bytes([EXTENDED_MESSAGE_ID]) + expected_req0_msg_content
        # 验证第二个请求 (Verify second request)
        expected_req1_dict = {b'msg_type': 0, b'piece': 1}
        encoded_req1_payload = bencodepy.encode(expected_req1_dict)
        expected_req1_msg_content = bytes([peer_ut_metadata_id]) + encoded_req1_payload
        expected_req1_full_msg = struct.pack('>I', len(expected_req1_msg_content) + 1) + bytes([EXTENDED_MESSAGE_ID]) + expected_req1_msg_content
        
        # 检查调用参数 (Check call arguments)
        # mock_writer.write.assert_any_call(expected_req0_full_msg)
        # mock_writer.write.assert_any_call(expected_req1_full_msg)
        # 或者更严格地按顺序检查 (Or check in order more strictly)
        calls = [call(expected_req0_full_msg), call(expected_req1_full_msg)]
        mock_writer.write.assert_has_calls(calls, any_order=False) # 验证调用顺序和参数 (Verify call order and arguments)


    async def test_request_metadata_pieces_peer_rejects(self):
        # 测试对等节点拒绝一个块请求 (Test peer rejects a piece request)
        peer_ut_metadata_id = 2
        # 响应块0 (成功) (Response for piece 0 (success))
        piece0_resp_dict = {b'msg_type': 1, b'piece': 0, b'total_size': METADATA_SIZE}
        encoded_piece0_info = bencodepy.encode(piece0_resp_dict)
        msg0_payload = bytes([peer_ut_metadata_id]) + encoded_piece0_info + PIECE_0_DATA
        msg0_full = struct.pack('>I', len(msg0_payload) + 1) + bytes([EXTENDED_MESSAGE_ID]) + msg0_payload
        
        # 响应块1 (拒绝) (Response for piece 1 (reject))
        reject1_resp_dict = {b'msg_type': 2, b'piece': 1} # msg_type 2 表示拒绝 (msg_type 2 for reject)
        encoded_reject1_info = bencodepy.encode(reject1_resp_dict)
        msg1_payload_reject = bytes([peer_ut_metadata_id]) + encoded_reject1_info # 拒绝消息没有原始数据 (Reject message has no raw data)
        msg1_full_reject = struct.pack('>I', len(msg1_payload_reject) + 1) + bytes([EXTENDED_MESSAGE_ID]) + msg1_payload_reject

        mock_reader, mock_writer = await mock_reader_writer_factory([msg0_full, msg1_full_reject])
        
        result_metadata = await self.downloader._request_metadata_pieces(
            mock_reader, mock_writer, peer_ut_metadata_id, METADATA_SIZE, "1.2.3.4:12345"
        )
        self.assertIsNone(result_metadata) # 断言下载失败 (Assert download failed)

    # --- _verify_metadata 测试 --- (_verify_metadata Tests)
    def test_verify_metadata_success(self):
        # 测试元信息验证成功 (Test successful metadata verification)
        # 需要一个 infohash 与 SAMPLE_METADATA_BYTES 匹配的下载器实例
        # (Need a downloader instance whose infohash matches SAMPLE_METADATA_BYTES)
        # 计算 SAMPLE_METADATA_BYTES 的 infohash
        # (Calculate infohash for SAMPLE_METADATA_BYTES)
        true_info_bencoded = bencodepy.encode(SAMPLE_INFO_DICT) # 从原始字典重新编码 'info' 部分 (Re-encode 'info' part from original dict)
        true_infohash = hashlib.sha1(true_info_bencoded).digest()
        
        downloader_for_verify = MetainfoDownloader(infohash=true_infohash, peer_id=self.peer_id)
        is_valid = downloader_for_verify._verify_metadata(SAMPLE_METADATA_BYTES)
        self.assertTrue(is_valid) # 断言验证成功 (Assert verification successful)

    def test_verify_metadata_failure(self):
        # 测试元信息验证失败 (哈希不匹配) (Test metadata verification failure (hash mismatch))
        wrong_infohash = os.urandom(20) # 与 SAMPLE_METADATA_BYTES 不匹配的 infohash (Infohash that won't match SAMPLE_METADATA_BYTES)
        downloader_for_verify = MetainfoDownloader(infohash=wrong_infohash, peer_id=self.peer_id)
        is_valid = downloader_for_verify._verify_metadata(SAMPLE_METADATA_BYTES)
        self.assertFalse(is_valid) # 断言验证失败 (Assert verification failed)

    # --- download (集成) 测试 --- (download (integration) Tests)
    @patch.object(MetainfoDownloader, '_connect_to_peer', new_callable=AsyncMock)
    @patch.object(MetainfoDownloader, '_perform_bittorrent_handshake', new_callable=AsyncMock)
    @patch.object(MetainfoDownloader, '_send_extended_handshake', new_callable=AsyncMock)
    @patch.object(MetainfoDownloader, '_request_metadata_pieces', new_callable=AsyncMock)
    @patch.object(MetainfoDownloader, '_verify_metadata') # 同步方法 (Synchronous method)
    async def test_download_overall_success(self, mock_verify, mock_request_pieces, 
                                           mock_send_ext_hs, mock_bt_hs, mock_connect):
        # 测试完整的成功下载流程 (Test overall successful download flow)
        # 配置模拟方法的返回值 (Configure return values of mocked methods)
        mock_reader, mock_writer = AsyncMock(), AsyncMock() # 简单的模拟 reader/writer (Simple mock reader/writer)
        mock_connect.return_value = (mock_reader, mock_writer) # 模拟连接成功 (Simulate successful connection)
        mock_bt_hs.return_value = True # 模拟 BitTorrent 握手成功 (Simulate successful BitTorrent handshake)
        mock_send_ext_hs.return_value = (1, METADATA_SIZE) # ut_metadata_id=1, metadata_size (ut_metadata_id=1, metadata_size)
        mock_request_pieces.return_value = SAMPLE_METADATA_BYTES # 模拟成功下载块 (Simulate successful piece download)
        mock_verify.return_value = True # 模拟验证成功 (Simulate successful verification)

        peers = [("1.2.3.4", 12345)] # 提供一个对等节点 (Provide one peer)
        result = await self.downloader.download(peers)
        
        self.assertEqual(result, SAMPLE_METADATA_BYTES) # 断言返回下载的元信息 (Assert downloaded metadata is returned)
        mock_connect.assert_called_once_with("1.2.3.4", 12345) # 验证连接尝试 (Verify connection attempt)
        mock_bt_hs.assert_called_once() # 验证 BitTorrent 握手调用 (Verify BitTorrent handshake call)
        mock_send_ext_hs.assert_called_once() # 验证扩展握手调用 (Verify extended handshake call)
        mock_request_pieces.assert_called_once() # 验证块请求调用 (Verify piece request call)
        mock_verify.assert_called_once_with(SAMPLE_METADATA_BYTES) # 验证验证调用 (Verify verification call)

    @patch.object(MetainfoDownloader, '_connect_to_peer', new_callable=AsyncMock)
    async def test_download_all_peers_fail_connect(self, mock_connect):
        # 测试所有对等节点连接失败的情况 (Test scenario where all peers fail to connect)
        mock_connect.return_value = None # 模拟连接失败 (Simulate connection failure)
        peers = [("1.1.1.1", 111), ("2.2.2.2", 222)] # 两个对等节点 (Two peers)
        
        result = await self.downloader.download(peers)
        self.assertIsNone(result) # 断言下载失败 (Assert download failed)
        self.assertEqual(mock_connect.call_count, 2) # 验证对两个对等节点都进行了尝试 (Verify attempts for both peers)
        mock_connect.assert_has_calls([call("1.1.1.1", 111), call("2.2.2.2", 222)], any_order=True) # 验证调用参数 (Verify call arguments)

    async def test_download_empty_peer_list(self):
        # 测试对等节点列表为空的情况 (Test scenario with empty peer list)
        result = await self.downloader.download([]) # 空列表 (Empty list)
        self.assertIsNone(result) # 断言下载失败 (Assert download failed)


if __name__ == '__main__':
    unittest.main() # 运行测试 (Run tests)

```
