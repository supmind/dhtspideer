```python
import asyncio
import hashlib
import logging
import bencodepy # 使用已安装的库 (Using the installed library)
import struct # 用于打包/解包消息长度和 ID (For packing/unpacking message lengths and IDs)

# 获取此模块的日志记录器 (Get logger for this module)
# 日志级别和处理器由主脚本 (main_crawler.py) 配置
# (Log level and handlers are configured by the main script (main_crawler.py))
logger = logging.getLogger(__name__)

# BitTorrent 协议常量 (BitTorrent Protocol Constants)
PSTR = b"BitTorrent protocol"
PSTRLEN = len(PSTR)
RESERVED_BYTES = b'\x00\x00\x00\x00\x00\x10\x00\x00' 
EXTENDED_MESSAGE_ID = 20 
HANDSHAKE_LEN = 1 + PSTRLEN + 8 + 20 + 20 
BLOCK_SIZE = 16 * 1024  # 16 KiB
CONNECT_TIMEOUT = 5  
READ_TIMEOUT = 10 
PIECE_REQUEST_TIMEOUT = 15 

class MetainfoDownloadError(Exception):
    """元信息下载失败的自定义异常。 (Custom exception for metainfo download failures.)"""
    pass

class MetainfoDownloader:
    def __init__(self, infohash: bytes, peer_id: bytes, loop: asyncio.AbstractEventLoop = None):
        if len(infohash) != 20:
            raise ValueError("Infohash 必须为 20 字节长。 (Infohash must be 20 bytes long.)")
        if len(peer_id) != 20:
            raise ValueError("Peer ID 必须为 20 字节长。 (Peer ID must be 20 bytes long.)")

        self.infohash = infohash
        self.peer_id = peer_id
        self.loop = loop or asyncio.get_event_loop() # 获取事件循环 (Get event loop)
        # 初始化日志记录，包括 infohash (Log initialization including infohash)
        logger.info(f"MetainfoDownloader 已为 infohash 初始化: {self.infohash.hex()} "
                    f"(MetainfoDownloader initialized for infohash: {self.infohash.hex()})")

    async def _connect_to_peer(self, peer_ip: str, peer_port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
        # 记录连接尝试，包括目标对等节点 (Log connection attempt including target peer)
        logger.debug(f"[{self.infohash.hex()}] 正在尝试连接到对等节点 {peer_ip}:{peer_port} "
                     f"(Attempting to connect to peer {peer_ip}:{peer_port})")
        try:
            future = asyncio.open_connection(peer_ip, peer_port)
            reader, writer = await asyncio.wait_for(future, timeout=CONNECT_TIMEOUT)
            logger.info(f"[{self.infohash.hex()}] 已成功连接到 {peer_ip}:{peer_port} "
                        f"(Successfully connected to {peer_ip}:{peer_port})")
            return reader, writer
        except asyncio.TimeoutError:
            logger.warning(f"[{self.infohash.hex()}] 连接到 {peer_ip}:{peer_port} 超时。 "
                           f"(Timeout connecting to {peer_ip}:{peer_port}.)")
        except ConnectionRefusedError:
            logger.warning(f"[{self.infohash.hex()}] {peer_ip}:{peer_port} 拒绝连接。 "
                           f"(Connection refused by {peer_ip}:{peer_port}.)")
        except OSError as e: 
            logger.warning(f"[{self.infohash.hex()}] 连接到 {peer_ip}:{peer_port} 时发生操作系统错误: {e} "
                           f"(OS error connecting to {peer_ip}:{peer_port}: {e})")
        except Exception as e:
            logger.error(f"[{self.infohash.hex()}] 连接到 {peer_ip}:{peer_port} 时发生意外错误: {e} "
                          f"(Unexpected error connecting to {peer_ip}:{peer_port}: {e})", exc_info=True) # 添加 exc_info (Add exc_info)
        return None

    async def _perform_bittorrent_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_address: str # 用于日志记录 (For logging)
    ) -> bool:
        logger.debug(f"[{self.infohash.hex()}] 正在向 {peer_address} 发送 BitTorrent 握手... "
                     f"(Sending BitTorrent handshake to {peer_address}...)")
        handshake_msg = struct.pack('>B', PSTRLEN) + PSTR + RESERVED_BYTES + self.infohash + self.peer_id
        writer.write(handshake_msg)
        await writer.drain()

        logger.debug(f"[{self.infohash.hex()}] 正在从 {peer_address} 接收 BitTorrent 握手... "
                     f"(Receiving BitTorrent handshake from {peer_address}...)")
        try:
            resp_pstrlen_byte = await asyncio.wait_for(reader.readexactly(1), timeout=READ_TIMEOUT)
            resp_pstrlen = resp_pstrlen_byte[0]
            if resp_pstrlen != PSTRLEN:
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 返回了不正确的 pstrlen: {resp_pstrlen} "
                               f"(Peer {peer_address} returned incorrect pstrlen: {resp_pstrlen})")
                return False
            
            resp_pstr = await asyncio.wait_for(reader.readexactly(PSTRLEN), timeout=READ_TIMEOUT)
            if resp_pstr != PSTR:
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 返回了不正确的 pstr: {resp_pstr!r} "
                               f"(Peer {peer_address} returned incorrect pstr: {resp_pstr!r})")
                return False

            resp_reserved = await asyncio.wait_for(reader.readexactly(8), timeout=READ_TIMEOUT)
            if not (resp_reserved[5] & 0x10): 
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 不支持扩展消息 (BEP10)。保留字节: {resp_reserved.hex()} "
                               f"(Peer {peer_address} does not support extended messaging (BEP10). Reserved bytes: {resp_reserved.hex()})")
                return False
            
            resp_infohash = await asyncio.wait_for(reader.readexactly(20), timeout=READ_TIMEOUT)
            if resp_infohash != self.infohash:
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 在握手中返回了不同的 infohash: {resp_infohash.hex()}。谨慎处理。 "
                               f"(Peer {peer_address} returned different infohash in handshake: {resp_infohash.hex()}. Proceeding cautiously.)")

            resp_peer_id = await asyncio.wait_for(reader.readexactly(20), timeout=READ_TIMEOUT) 
            logger.debug(f"[{self.infohash.hex()}] 从 {peer_address} 收到的对等节点 ID: {resp_peer_id.hex()} "
                         f"(Received peer ID from {peer_address}: {resp_peer_id.hex()})")

            logger.info(f"[{self.infohash.hex()}] 与 {peer_address} 的 BitTorrent 握手成功。 "
                        f"(BitTorrent handshake with {peer_address} successful.)")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"[{self.infohash.hex()}] 与 {peer_address} 进行 BitTorrent 握手超时。 "
                           f"(Timeout during BitTorrent handshake with {peer_address}.)")
        except asyncio.IncompleteReadError:
            logger.warning(f"[{self.infohash.hex()}] 与 {peer_address} 进行 BitTorrent 握手时读取不完整 (连接已关闭)。 "
                           f"(Incomplete read during BitTorrent handshake with {peer_address} (connection closed).)")
        except Exception as e:
            logger.error(f"[{self.infohash.hex()}] 与 {peer_address} 进行 BitTorrent 握手时出错: {e} "
                          f"(Error during BitTorrent handshake with {peer_address}: {e})", exc_info=True)
        return False

    async def _send_extended_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_address: str # 用于日志记录 (For logging)
    ) -> tuple[int, int] | None: 
        logger.debug(f"[{self.infohash.hex()}] 正在向 {peer_address} 发送扩展握手 (BEP10)... "
                     f"(Sending extended handshake (BEP10) to {peer_address}...)")
        ext_handshake_dict = {
            b'm': {b'ut_metadata': 1}, 
        }
        encoded_ext_handshake_payload = bencodepy.encode(ext_handshake_dict)
        extended_msg_content = bytes([0]) + encoded_ext_handshake_payload 
        msg = struct.pack('>I', len(extended_msg_content) + 1) + bytes([EXTENDED_MESSAGE_ID]) + extended_msg_content
        
        writer.write(msg)
        await writer.drain()
        logger.debug(f"[{self.infohash.hex()}] 已向 {peer_address} 发送扩展握手: {ext_handshake_dict} "
                     f"(Extended handshake sent to {peer_address}: {ext_handshake_dict})")

        try:
            logger.debug(f"[{self.infohash.hex()}] 等待来自 {peer_address} 的扩展握手响应... "
                         f"(Waiting for peer's extended handshake response from {peer_address}...)")
            resp_len_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=READ_TIMEOUT)
            resp_len = struct.unpack('>I', resp_len_bytes)[0]
            if resp_len == 0: 
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到长度为 0 的消息，可能为 keep-alive。 "
                               f"(Received message with length 0 from {peer_address} after extended handshake, might be keep-alive.)")
                return None

            resp_msg_id_byte = await asyncio.wait_for(reader.readexactly(1), timeout=READ_TIMEOUT)
            resp_msg_id = resp_msg_id_byte[0]

            if resp_msg_id != EXTENDED_MESSAGE_ID:
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 期望扩展消息 ID ({EXTENDED_MESSAGE_ID})，但收到 {resp_msg_id}。 "
                               f"(Expected extended message ID ({EXTENDED_MESSAGE_ID}) from {peer_address}, got {resp_msg_id}.)")
                return None 
            
            extended_payload_len = resp_len - 1
            if extended_payload_len < 1: 
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的扩展握手响应中无效的扩展有效载荷长度 {extended_payload_len}。 "
                               f"(Invalid extended payload length {extended_payload_len} in extended handshake response from {peer_address}.)")
                return None

            resp_extended_msg_type_byte = await asyncio.wait_for(reader.readexactly(1), timeout=READ_TIMEOUT)
            if resp_extended_msg_type_byte[0] != 0: 
                 logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 期望扩展握手类型 0，但收到 {resp_extended_msg_type_byte[0]}。 "
                                f"(Expected extended handshake type 0 in response from {peer_address}, got {resp_extended_msg_type_byte[0]}.)")
                 return None

            bencoded_data_len = extended_payload_len - 1 
            if bencoded_data_len < 0:
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的扩展握手响应中无效的 bencoded 数据长度 {bencoded_data_len}。 "
                               f"(Invalid bencoded data length {bencoded_data_len} for extended handshake response from {peer_address}.)")
                return None
            
            if bencoded_data_len == 0: 
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到空的扩展握手有效载荷，对方可能不完全支持 ut_metadata。 "
                               f"(Received empty extended handshake payload from {peer_address}, peer likely doesn't support ut_metadata properly.)")
                return None

            bencoded_payload_data = await asyncio.wait_for(reader.readexactly(bencoded_data_len), timeout=READ_TIMEOUT)
            decoded_payload = bencodepy.decode(bencoded_payload_data)
            logger.debug(f"[{self.infohash.hex()}] 从 {peer_address} 收到的扩展握手解码后有效载荷: {decoded_payload} "
                         f"(Received extended handshake decoded payload from {peer_address}: {decoded_payload})")

            m_dict = decoded_payload.get(b'm')
            if not isinstance(m_dict, dict) or b'ut_metadata' not in m_dict:
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 不支持 ut_metadata (缺少 'm' 字典或 'ut_metadata' 键)。 "
                               f"(Peer {peer_address} does not support ut_metadata (missing 'm' dict or 'ut_metadata' key).)")
                return None
            
            ut_metadata_id = m_dict[b'ut_metadata']
            if not isinstance(ut_metadata_id, int):
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的 ut_metadata_id 类型无效: {type(ut_metadata_id)}。期望 int。 "
                               f"(Invalid ut_metadata_id type from {peer_address}: {type(ut_metadata_id)}. Expected int.)")
                return None

            metadata_size = decoded_payload.get(b'metadata_size')
            if not isinstance(metadata_size, int) or metadata_size <= 0:
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的 metadata_size 无效或缺失: {metadata_size} "
                               f"(Invalid or missing metadata_size from peer {peer_address}: {metadata_size})")
                return None
            
            logger.info(f"[{self.infohash.hex()}] 与 {peer_address} 的扩展握手成功。对方的 ut_metadata ID: {ut_metadata_id}, metadata_size: {metadata_size} "
                        f"(Extended handshake with {peer_address} successful. Peer's ut_metadata ID: {ut_metadata_id}, metadata_size: {metadata_size})")
            return ut_metadata_id, metadata_size

        except asyncio.TimeoutError:
            logger.warning(f"[{self.infohash.hex()}] 与 {peer_address} 进行扩展握手超时。 "
                           f"(Timeout during extended handshake with {peer_address}.)")
        except asyncio.IncompleteReadError:
            logger.warning(f"[{self.infohash.hex()}] 与 {peer_address} 进行扩展握手时读取不完整。 "
                           f"(Incomplete read during extended handshake with {peer_address}.)")
        except bencodepy.BencodeDecodeError as e:
            bencoded_data_hex = binascii.hexlify(bencoded_payload_data[:100] if 'bencoded_payload_data' in locals() else b'').decode()
            logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 解码扩展握手响应时出错: {e}。数据 (前100字节的十六进制): {bencoded_data_hex} "
                           f"(Error bdecoding extended handshake response from {peer_address}: {e}. Data (hex of first 100 bytes): {bencoded_data_hex})")
        except Exception as e:
            logger.error(f"[{self.infohash.hex()}] 与 {peer_address} 进行扩展握手时出错: {e} "
                          f"(Error during extended handshake with {peer_address}: {e})", exc_info=True)
        return None

    async def _request_metadata_pieces(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_ut_metadata_id: int, 
        metadata_size: int,
        peer_address: str # 用于日志记录 (For logging)
    ) -> bytes | None:
        num_pieces = (metadata_size + BLOCK_SIZE - 1) // BLOCK_SIZE
        logger.info(f"[{self.infohash.hex()}] 正在从 {peer_address} 请求 {num_pieces} 个元数据块，总大小 {metadata_size} 字节，使用对方的 ut_metadata_id {peer_ut_metadata_id}。 "
                    f"(Requesting {num_pieces} metadata pieces of total size {metadata_size} bytes from {peer_address} using peer's ut_metadata_id {peer_ut_metadata_id}.)")
        metadata_parts = [b''] * num_pieces 
        downloaded_pieces_count = 0

        for i in range(num_pieces):
            logger.debug(f"[{self.infohash.hex()}] 正在从 {peer_address} 请求元数据块 {i}... "
                         f"(Requesting metadata piece {i} from {peer_address}...)")
            request_msg_dict = {b'msg_type': 0, b'piece': i} 
            encoded_request_payload = bencodepy.encode(request_msg_dict)
            
            extended_msg_content = bytes([peer_ut_metadata_id]) + encoded_request_payload
            msg = struct.pack('>I', len(extended_msg_content) + 1) + bytes([EXTENDED_MESSAGE_ID]) + extended_msg_content
            
            writer.write(msg)
            await writer.drain()

            try:
                resp_len_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=PIECE_REQUEST_TIMEOUT)
                resp_len = struct.unpack('>I', resp_len_bytes)[0]
                if resp_len == 0: # Keep-alive
                    logger.debug(f"[{self.infohash.hex()}] 从 {peer_address} 收到 keep-alive 消息，正在请求块 {i}。 "
                                 f"(Received keep-alive from {peer_address} while requesting piece {i}.)")
                    # 可能需要重新请求或增加超时 (May need to re-request or increase timeout)
                    # 为简单起见，我们继续，但如果这导致超时，则会失败
                    # (For simplicity, we continue, but this will fail if it leads to timeout)
                    continue 

                resp_msg_id_byte = await asyncio.wait_for(reader.readexactly(1), timeout=READ_TIMEOUT)
                if resp_msg_id_byte[0] != EXTENDED_MESSAGE_ID:
                    logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 期望 EXTENDED_MESSAGE_ID (块 {i})，但收到 {resp_msg_id_byte[0]}。 "
                                   f"(Expected EXTENDED_MESSAGE_ID from {peer_address} for piece {i}, got {resp_msg_id_byte[0]}.)")
                    return None

                resp_ut_metadata_id_byte = await asyncio.wait_for(reader.readexactly(1), timeout=READ_TIMEOUT)
                if resp_ut_metadata_id_byte[0] != peer_ut_metadata_id:
                    logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 期望对方的 ut_metadata_id {peer_ut_metadata_id} (块 {i})，但收到 {resp_ut_metadata_id_byte[0]}。 "
                                   f"(Expected peer's ut_metadata_id {peer_ut_metadata_id} from {peer_address} for piece {i}, got {resp_ut_metadata_id_byte[0]}.)")
                    return None

                bencoded_plus_data_len = resp_len - 2 
                if bencoded_plus_data_len < 0:
                    logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的块 {i} 的 bencoded_plus_data_len 无效: {bencoded_plus_data_len}。 "
                                   f"(Invalid bencoded_plus_data_len {bencoded_plus_data_len} for piece {i} from {peer_address}.)")
                    return None
                
                piece_payload_buffer = await asyncio.wait_for(reader.readexactly(bencoded_plus_data_len), timeout=READ_TIMEOUT)
                
                try:
                    decoded_piece_info, bencoded_part_len = bencodepy.decode_from_buffer(piece_payload_buffer)
                except bencodepy.BencodeDecodeError as bde:
                    logger.error(f"[{self.infohash.hex()}] 从 {peer_address} 解码块 {i} 的信息失败: {bde}。缓冲区 (前100字节): {piece_payload_buffer[:100]!r} "
                                 f"(Bdecoding piece info failed for piece {i} from {peer_address}: {bde}. Buffer (first 100 bytes): {piece_payload_buffer[:100]!r})")
                    return None

                msg_type = decoded_piece_info.get(b'msg_type')
                piece_num = decoded_piece_info.get(b'piece')

                if msg_type == 1 and piece_num == i: 
                    piece_data = piece_payload_buffer[bencoded_part_len:]
                    expected_size_this_piece = min(BLOCK_SIZE, metadata_size - (i * BLOCK_SIZE))
                    
                    if len(piece_data) != expected_size_this_piece:
                        logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的块 {i} 大小不正确。期望 {expected_size_this_piece}，收到 {len(piece_data)}。 "
                                       f"(Piece {i} from {peer_address} has incorrect size. Expected {expected_size_this_piece}, got {len(piece_data)}.)")
                        return None 
                    
                    metadata_parts[i] = piece_data
                    downloaded_pieces_count += 1
                    logger.debug(f"[{self.infohash.hex()}] 从 {peer_address} 收到块 {i}，大小 {len(piece_data)}。已下载 {downloaded_pieces_count}/{num_pieces}。 "
                                 f"(Received piece {i} of size {len(piece_data)} from {peer_address}. Downloaded {downloaded_pieces_count}/{num_pieces}.)")
                elif msg_type == 2: 
                    logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address} 拒绝了对块 {i} 的请求。 "
                                   f"(Peer {peer_address} rejected request for piece {i}.)")
                    return None 
                else:
                    logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 收到的块 {i} 的消息类型 {msg_type} 或块编号 {piece_num} (期望 {i}) 不正确。解码后: {decoded_piece_info} "
                                   f"(Unexpected msg_type {msg_type} or piece_num {piece_num} (expected {i}) for piece {i} from {peer_address}. Decoded: {decoded_piece_info})")
                    return None

            except asyncio.TimeoutError:
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 等待块 {i} 超时。 "
                               f"(Timeout waiting for piece {i} from {peer_address}.)")
                return None
            except asyncio.IncompleteReadError:
                logger.warning(f"[{self.infohash.hex()}] 从 {peer_address} 读取块 {i} 时不完整 (连接已关闭)。 "
                               f"(Incomplete read for piece {i} from {peer_address} (connection closed).)")
                return None
            except Exception as e:
                logger.error(f"[{self.infohash.hex()}] 从 {peer_address} 处理块 {i} 时出错: {e} "
                              f"(Error processing piece {i} from {peer_address}: {e})", exc_info=True)
                return None
        
        if downloaded_pieces_count == num_pieces:
            full_metadata = b"".join(metadata_parts)
            if len(full_metadata) != metadata_size:
                logger.error(f"[{self.infohash.hex()}] 从 {peer_address} 下载的最终元信息大小不匹配。期望 {metadata_size}，收到 {len(full_metadata)}。 "
                             f"(Final metadata size mismatch from {peer_address}. Expected {metadata_size}, got {len(full_metadata)}.)")
                return None
            logger.info(f"[{self.infohash.hex()}] 已成功从 {peer_address} 下载所有元信息块。 "
                        f"(All metadata pieces successfully downloaded from {peer_address}.)")
            return full_metadata
        else:
            logger.error(f"[{self.infohash.hex()}] 未能从 {peer_address} 下载所有元信息块。收到 {downloaded_pieces_count}/{num_pieces}。 "
                         f"(Failed to download all metadata pieces from {peer_address}. Got {downloaded_pieces_count}/{num_pieces}.)")
            return None

    def _verify_metadata(self, metadata_bytes: bytes) -> bool:
        try:
            decoded_metadata = bencodepy.decode(metadata_bytes)
            if not isinstance(decoded_metadata, dict):
                logger.error(f"[{self.infohash.hex()}] 元信息不是有效的 bencoded 字典。 "
                             f"(Metadata is not a valid bencoded dictionary.)")
                return False
            
            info_dict = decoded_metadata.get(b'info')
            if not isinstance(info_dict, dict):
                logger.error(f"[{self.infohash.hex()}] 元信息中缺少 'info' 字典或其类型不正确。 "
                             f"('info' dictionary not found or invalid in metadata.)")
                return False
            
            info_bencoded = bencodepy.encode(info_dict)
            calculated_infohash = hashlib.sha1(info_bencoded).digest()

            if calculated_infohash == self.infohash:
                logger.info(f"[{self.infohash.hex()}] 元信息验证成功: SHA1 哈希与 infohash 匹配。 "
                            f"(Metadata verification successful: SHA1 hash matches infohash.)")
                return True
            else:
                logger.error(f"[{self.infohash.hex()}] 元信息验证失败。期望 infohash: {self.infohash.hex()}, 计算得到: {calculated_infohash.hex()} "
                             f"(Metadata verification failed. Expected infohash: {self.infohash.hex()}, calculated: {calculated_infohash.hex()})")
                return False
        except bencodepy.BencodeDecodeError:
            logger.error(f"[{self.infohash.hex()}] 验证时解码元信息失败。 "
                         f"(Failed to bdecode metadata for verification.)")
            return False
        except Exception as e:
            logger.error(f"[{self.infohash.hex()}] 元信息验证期间出错: {e} "
                          f"(Error during metadata verification: {e})", exc_info=True)
            return False

    async def download(self, peers: list[tuple[str, int]]) -> bytes | None:
        if not peers:
            logger.warning(f"[{self.infohash.hex()}] 没有提供用于下载元信息的对等节点。 "
                           f"(No peers provided to download metadata for infohash {self.infohash.hex()}.)")
            return None

        # 尝试从每个对等节点下载 (Attempt to download from each peer)
        for peer_ip, peer_port in peers:
            peer_address_str = f"{peer_ip}:{peer_port}" # 用于日志记录的对等节点地址字符串 (Peer address string for logging)
            reader, writer = None, None 
            try:
                connection_result = await self._connect_to_peer(peer_ip, peer_port)
                if not connection_result:
                    logger.debug(f"[{self.infohash.hex()}] 无法连接到对等节点 {peer_address_str}。尝试下一个。 "
                                 f"(Failed to connect to peer {peer_address_str}. Trying next.)")
                    continue 
                reader, writer = connection_result

                if not await self._perform_bittorrent_handshake(reader, writer, peer_address_str):
                    logger.warning(f"[{self.infohash.hex()}] 与 {peer_address_str} 的 BitTorrent 握手失败。 "
                                   f"(BitTorrent handshake failed with {peer_address_str}.)")
                    if writer and not writer.is_closing(): writer.close(); await writer.wait_closed()
                    continue

                ext_handshake_result = await self._send_extended_handshake(reader, writer, peer_address_str)
                if not ext_handshake_result:
                    logger.warning(f"[{self.infohash.hex()}] 与 {peer_address_str} 的扩展握手失败。 "
                                   f"(Extended handshake failed with {peer_address_str}.)")
                    if writer and not writer.is_closing(): writer.close(); await writer.wait_closed()
                    continue

                peer_ut_metadata_id, metadata_size = ext_handshake_result
                
                if metadata_size > 5 * 1024 * 1024: # 5MB 大小限制 (5MB size limit)
                    logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address_str} 报告的元信息大小 {metadata_size} 过大。跳过。 "
                                   f"(Metadata size {metadata_size} reported by {peer_address_str} is too large. Skipping.)")
                    if writer and not writer.is_closing(): writer.close(); await writer.wait_closed()
                    continue

                full_metadata = await self._request_metadata_pieces(reader, writer, peer_ut_metadata_id, metadata_size, peer_address_str)

                if full_metadata:
                    if self._verify_metadata(full_metadata):
                        logger.info(f"[{self.infohash.hex()}] 已成功从 {peer_address_str} 下载并验证元信息。 "
                                    f"(Successfully downloaded and verified metadata from {peer_address_str} for infohash {self.infohash.hex()}.)")
                        return full_metadata # 成功，返回元信息 (Success, return metadata)
                    else:
                        logger.warning(f"[{self.infohash.hex()}] 从 {peer_address_str} 下载的元信息验证失败。 "
                                       f"(Metadata from {peer_address_str} failed verification for infohash {self.infohash.hex()}.)")
                        # 如果验证失败，我们可能会尝试下一个对等节点 (If verification fails, we might try the next peer)
                else:
                    logger.warning(f"[{self.infohash.hex()}] 未能从 {peer_address_str} 下载元信息块。 "
                                   f"(Failed to download metadata pieces from {peer_address_str} for infohash {self.infohash.hex()}.)")

            except MetainfoDownloadError as e: 
                logger.error(f"[{self.infohash.hex()}] 与对等节点 {peer_address_str} 发生元信息下载错误: {e} "
                             f"(MetainfoDownloadError with peer {peer_address_str} for infohash {self.infohash.hex()}: {e})")
            except ConnectionResetError:
                logger.warning(f"[{self.infohash.hex()}] 对等节点 {peer_address_str} 重置了连接。 "
                               f"(Connection reset by peer {peer_address_str} for infohash {self.infohash.hex()}.)")
            except Exception as e: 
                logger.error(f"[{self.infohash.hex()}] 与对等节点 {peer_address_str} 发生意外错误: {e} "
                              f"(An unexpected error occurred with peer {peer_address_str} for infohash {self.infohash.hex()}: {e})", exc_info=True)
            finally:
                if writer and not writer.is_closing():
                    try:
                        writer.close()
                        await writer.wait_closed() # 等待连接完全关闭 (Wait for connection to close completely)
                        logger.debug(f"[{self.infohash.hex()}] 与 {peer_address_str} 的连接已关闭。 "
                                     f"(Connection closed with {peer_address_str} for infohash {self.infohash.hex()}.)")
                    except Exception as e_close:
                        logger.error(f"[{self.infohash.hex()}] 关闭与 {peer_address_str} 的连接时出错: {e_close} "
                                      f"(Error closing connection with {peer_address_str} for infohash {self.infohash.hex()}: {e_close})")
            
            logger.info(f"[{self.infohash.hex()}] 正在尝试下一个对等节点 (如果有) 以获取 infohash {self.infohash.hex()}... "
                        f"(Trying next peer (if any) for infohash {self.infohash.hex()}...)")

        logger.warning(f"[{self.infohash.hex()}] 在尝试所有提供的对等节点后，未能下载 infohash {self.infohash.hex()} 的元信息。 "
                       f"(Failed to download metadata for infohash {self.infohash.hex()} after trying all provided peers.)")
        return None # 未能从任何对等节点下载 (Failed to download from any peer)

async def example_run(): # 示例运行函数 (Example run function)
    # 为此示例配置日志记录 (Configure logging for this example)
    example_log_format = '%(asctime)s - %(levelname)s - %(name)s - [%(infohash)s] - %(message)s'
    # 创建一个自定义的日志适配器，以便轻松地将 infohash 添加到日志记录中
    # (Create a custom log adapter to easily add infohash to log records)
    class InfohashAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            # 如果上下文中没有 infohash，则使用占位符
            # (Use a placeholder if no infohash in context)
            return '[%s] %s' % (self.extra['infohash'], msg), kwargs

    # 获取根日志记录器并设置级别 (Get root logger and set level)
    logging.basicConfig(level=logging.DEBUG, format=example_log_format)
    # 获取 MetainfoDownloader 的日志记录器 (Get logger for MetainfoDownloader)
    downloader_logger = logging.getLogger(__name__) 
    # 创建适配器实例 (Create adapter instance)
    adapter = InfohashAdapter(downloader_logger, {'infohash': 'N/A'})


    try:
        test_infohash_hex = "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33" # Ubuntu 20.04 ISO
        test_infohash_bytes = bytes.fromhex(test_infohash_hex)
        
        my_peer_id = hashlib.sha1(b"my_client_peer_id_example_003").digest()

        test_peers = [
            # ("1.2.3.4", 12345), # 用实际对等节点替换 (Replace with actual peers)
        ]
        
        if not test_peers:
            adapter.warning("未配置用于 example_run 的对等节点。下载器将无法连接。 "
                            "(No peers configured for example_run. Downloader will not be able to connect.)")
            adapter.warning(f"测试用示例 infohash (Ubuntu 20.04): {test_infohash_hex} "
                            f"(Example infohash for testing (Ubuntu 20.04): {test_infohash_hex})")
            return

        # 更新适配器中的 infohash (Update infohash in adapter)
        adapter.extra['infohash'] = test_infohash_hex 
        downloader = MetainfoDownloader(infohash=test_infohash_bytes, peer_id=my_peer_id)
        metadata = await downloader.download(test_peers)

        if metadata:
            adapter.info(f"元信息下载成功！长度: {len(metadata)} 字节。 "
                         f"(Metadata downloaded successfully! Length: {len(metadata)} bytes.)")
            try:
                decoded = bencodepy.decode(metadata)
                info = decoded.get(b'info', {})
                name = info.get(b'name', b'Unknown').decode('utf-8', 'ignore')
                adapter.info(f"种子名称: {name} (Torrent name: {name})")
            except Exception as e:
                adapter.error(f"无法解码或解析下载的元信息: {e} "
                              f"(Could not decode or parse downloaded metadata: {e})")
        else:
            adapter.info("未能下载元信息。 (Failed to download metadata.)")

    except ValueError as ve: 
        adapter.error(f"初始化错误: {ve} (Initialization error: {ve})")
    except Exception as e:
        adapter.error(f"example_run 中发生错误: {e} (An error occurred in example_run: {e})", exc_info=True)

if __name__ == "__main__":
    # 注意：要进行真实测试，请在 `example_run` 中用实际数据替换 `test_infohash_hex` 和 `test_peers`。
    # (Note: For a real test, replace `test_infohash_hex` and `test_peers` in `example_run` with actual data.)
    # `bencodepy` 必须已安装: `pip install bencodepy`
    # (`bencodepy` must be installed: `pip install bencodepy`)
    asyncio.run(example_run())
    logger.info("示例运行完成。 (Example run finished.)") # 使用模块级日志记录器 (Use module-level logger)

```
