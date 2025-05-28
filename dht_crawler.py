import asyncio
import logging
import binascii
import socket # 用于 IP 地址转换 (For IP address conversion)
import struct # 用于解包紧凑的节点/对等方信息 (For unpacking compact node/peer info)


from kademlia.network import Server
from kademlia.storage import ForgetfulStorage
from kademlia.node import Node # 导入 Node 类以进行类型提示和使用 (Import Node class for type hinting and usage)

# 获取此模块的日志记录器 (Get logger for this module)
# 日志级别和处理器由主脚本 (main_crawler.py) 配置
# (Log level and handlers are configured by the main script (main_crawler.py))
logger = logging.getLogger(__name__)

# 默认引导节点 (Default bootstrap nodes)
DEFAULT_BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

class InfohashQueueStorage(ForgetfulStorage):
    """
    一个 Kademlia 存储后端，它拦截来自 set 和 get 操作的键 (infohashes)，
    并将它们放入一个 asyncio.Queue 中。
    (A Kademlia storage backend that intercepts keys (infohashes) from
    set and get operations and puts them onto an asyncio.Queue.)
    """
    def __init__(self, ttl=3600, infohash_queue=None): # 默认 TTL: 1 小时 (Default TTL: 1 hour)
        super().__init__(ttl)
        if infohash_queue is None:
            # infohash_queue 必须提供错误处理 (infohash_queue must be provided error handling)
            raise ValueError("infohash_queue 必须提供 (infohash_queue must be provided)")
        self.infohash_queue = infohash_queue
        # 初始化日志记录已移至 DHTCrawler 或主脚本 (Initialization logging moved to DHTCrawler or main script)
        # logger.info("InfohashQueueStorage 已初始化。 (InfohashQueueStorage initialized.)")

    def _log_and_queue_key(self, key, operation_name):
        # Kademlia 键是字节串。转换为十六进制以便记录/可读性。
        # 这些键是 Kademlia ID，对于 BitTorrent DHT 来说是 infohashes。
        # (Kademlia keys are bytes. Convert to hex for logging/readability.
        # These keys are the Kademlia IDs, which for BitTorrent DHT are the infohashes.)
        try:
            hex_key = binascii.hexlify(key).decode('utf-8')
            # 记录拦截到的 infohash，级别为 DEBUG (Log intercepted infohash at DEBUG level)
            logger.debug(f"存储操作 '{operation_name}' 拦截到 infohash: {hex_key} "
                         f"(Storage operation '{operation_name}' intercepted infohash: {hex_key})")
            # 确保我们将字节串放入队列中，因为 infohashes 通常是字节串
            # (Ensure we are putting bytes into the queue as infohashes are typically bytes)
            self.infohash_queue.put_nowait(key)
        except Exception as e:
            # 键处理错误 (Error processing key)
            logger.error(f"处理键 {key!r} 时在 {operation_name} 中出错: {e} (Error processing key {key!r} in {operation_name}: {e})")


    async def set(self, key, value, original_publisher=False, original_publish_time=0):
        """
        当收到 STORE RPC 时调用。
        键是 infohash。
        (Called when a STORE RPC is received.
        The key is the infohash.)
        """
        self._log_and_queue_key(key, "set (announce_peer)")
        return await super().set(key, value, original_publisher, original_publish_time)

    async def get(self, key):
        """
        当收到 FIND_VALUE RPC 时调用 (在 BitTorrent DHT 中是 get_peers)。
        键是 infohash。
        (Called when a FIND_VALUE RPC is received (get_peers in BitTorrent DHT).
        The key is the infohash.)
        """
        self._log_and_queue_key(key, "get (get_peers)")
        return await super().get(key)

    def __getitem__(self, key):
        self._log_and_queue_key(key, "getitem (local lookup)")
        return super().__getitem__(key)


class DHTCrawler:
    """
    BitTorrent DHT 网络的爬虫。
    (A crawler for the BitTorrent DHT network.)
    """
    def __init__(self, bootstrap_nodes=None, infohash_queue=None, node_id=None):
        if bootstrap_nodes is None:
            bootstrap_nodes = DEFAULT_BOOTSTRAP_NODES
        if infohash_queue is None:
            raise ValueError("infohash_queue (asyncio.Queue) 必须提供。 (infohash_queue (asyncio.Queue) must be provided.)")

        self.bootstrap_nodes = bootstrap_nodes
        self.infohash_queue = infohash_queue
        self.server = None
        self.node_id = node_id 
        self.custom_storage = InfohashQueueStorage(infohash_queue=self.infohash_queue)
        logger.info(f"DHTCrawler 已初始化。节点 ID: {node_id.hex() if node_id else '自动生成'} "
                    f"(DHTCrawler initialized. Node ID: {node_id.hex() if node_id else 'Auto-generated'})")

    async def start(self, port=6881):
        """
        启动 DHT 爬虫。
        (Starts the DHT crawler.)
        """
        logger.info(f"正在启动 DHT 爬虫，端口为 {port}... (Starting DHT crawler on port {port}...)")
        try:
            self.server = Server(
                ksize=20, 
                alpha=3,  
                id=self.node_id, 
                storage=self.custom_storage 
            )
            await self.server.listen(port)
            logger.info(f"DHT 服务器已成功在端口 {port} 上监听。 (DHT server successfully listening on port {port}.)")

            if self.bootstrap_nodes:
                logger.info(f"正在使用节点进行引导: {self.bootstrap_nodes} (Bootstrapping with nodes: {self.bootstrap_nodes})")
                # bootstrap 方法现在返回一个 Future，其中包含引导结果 (bootstrap method now returns a Future with bootstrap results)
                # 我们 await 它来确保引导完成或引发异常 (We await it to ensure bootstrapping completes or raises an exception)
                bootstrap_result = await self.server.bootstrap(self.bootstrap_nodes)
                logger.info(f"引导过程已完成。结果: {bootstrap_result} (Bootstrap process completed. Results: {bootstrap_result})")
            else:
                logger.warning("未提供引导节点。节点可能无法有效连接到 DHT 网络。 "
                               "(No bootstrap nodes provided. The node may not connect to the DHT network effectively.)")

            logger.info("DHT 爬虫已成功启动。 (DHT Crawler started successfully.)")
        except OSError as e:
            logger.error(f"在端口 {port} 上启动 DHT 爬虫失败: {e}。端口可能已被占用或您可能缺少权限。 "
                         f"(Failed to start DHT crawler on port {port}: {e}. "
                         "The port might be in use or you might lack permissions.)")
            raise
        except Exception as e:
            logger.error(f"爬虫启动期间发生意外错误: {e} (An unexpected error occurred during crawler startup: {e})", exc_info=True)
            raise

    async def stop(self):
        """
        停止 DHT 爬虫。
        (Stops the DHT crawler.)
        """
        if self.server:
            logger.info("正在停止 DHT 爬虫... (Stopping DHT crawler...)")
            if hasattr(self.server, 'stop') and callable(self.server.stop): # 检查 stop 方法是否存在 (Check if stop method exists)
                 self.server.stop() 
            # 如果有 UDP 传输，也可能需要关闭它 (If there's a UDP transport, it might also need closing)
            # bmuller/kademlia 的 Server.stop() 应该处理其传输的关闭
            # (bmuller/kademlia's Server.stop() should handle closing its transport)
            self.server = None # 清除服务器实例 (Clear server instance)
            logger.info("DHT 爬虫已停止。 (DHT crawler stopped.)")
        else:
            logger.info("DHT 爬虫未在运行或已停止。 (DHT crawler is not running or already stopped.)")

    def get_routing_table_size(self) -> int:
        """
        获取路由表中活动节点的数量。
        (Get the number of active nodes in the routing table.)
        Returns:
            int: 路由表中的节点数。 (Number of nodes in the routing table.)
        """
        if self.server and hasattr(self.server, 'protocol') and hasattr(self.server.protocol, 'router'):
            # router.buckets 是 KBucket 对象的列表 (router.buckets is a list of KBucket objects)
            # 每个 KBucket 都有一个 get_nodes() 方法返回该桶中的节点 (Each KBucket has a get_nodes() method returning nodes in that bucket)
            try:
                return sum(len(bucket.get_nodes()) for bucket in self.server.protocol.router.buckets)
            except Exception as e:
                logger.error(f"获取路由表大小时出错: {e} (Error getting routing table size: {e})")
                return 0 # 出错时返回 0 (Return 0 on error)
        return 0 # 服务器未初始化或路由表不可用时返回 0 (Return 0 if server not initialized or routing table not available)


    async def get_peers_for_infohash(self, infohash_bytes: bytes, max_peers: int) -> list[tuple[str, int]]:
        """
        为给定的 infohash 查找对等节点。
        (Finds peers for a given infohash.)
        Args:
            infohash_bytes (bytes): 要查找对等节点的 infohash (字节串)。
                                    (The infohash (bytes) to find peers for.)
            max_peers (int): 要返回的最大对等节点数。
                             (The maximum number of peers to return.)
        Returns:
            list[tuple[str, int]]: (IP 地址, 端口) 元组的列表。
                                   (A list of (IP address, port) tuples.)
        """
        if not self.server:
            logger.warning("DHT 服务器未运行，无法获取对等节点。 (DHT server is not running, cannot get peers.)")
            return []

        # 使用 DEBUG 级别记录此消息，因为它可能非常频繁 (Log this message at DEBUG level as it can be very frequent)
        logger.debug(f"正在为 infohash {infohash_bytes.hex()} 查找对等节点... (Finding peers for infohash {infohash_bytes.hex()}...)")
        
        peers_found = []
        try:
            count = 0
            # server.get_iter() 返回与键关联的值的异步迭代器
            # (server.get_iter() returns an async iterator of values associated with the key)
            # 对于 BitTorrent DHT，这些值应该是对等节点的联系信息（紧凑格式）
            # (For BitTorrent DHT, these values should be peer contact info (compact format))
            async for value in self.server.get_iter(infohash_bytes): 
                if isinstance(value, bytes) and len(value) % 6 == 0: # 检查是否为有效的紧凑对等节点信息 (Check for valid compact peer info)
                    for i in range(0, len(value), 6):
                        try:
                            ip_bytes, port_bytes = value[i:i+4], value[i+4:i+6]
                            ip_addr = socket.inet_ntoa(ip_bytes) 
                            port = struct.unpack(">H", port_bytes)[0] 
                            if ip_addr != "0.0.0.0" and 0 < port <= 65535: 
                                peer_tuple = (ip_addr, port)
                                if peer_tuple not in peers_found: 
                                     peers_found.append(peer_tuple)
                                     logger.debug(f"通过 get_iter 为 infohash {infohash_bytes.hex()} 找到对等节点: {ip_addr}:{port} "
                                                  f"(Found peer via get_iter for infohash {infohash_bytes.hex()}: {ip_addr}:{port})")
                                     count += 1
                                     if count >= max_peers:
                                         break
                        except (socket.error, struct.error) as e: # 合并异常处理 (Consolidate exception handling)
                             logger.warning(f"解析对等节点信息时出错: {e}。数据: {value[i:i+6]!r} "
                                            f"(Error parsing peer info: {e}. Data: {value[i:i+6]!r})")
                # 如果值是 Node 对象的列表 (If values are lists of Node objects) - get_iter 通常不返回这个
                # (get_iter usually doesn't return this)
                # elif isinstance(value, list): # ... (处理列表的代码)
                if count >= max_peers:
                    break
            
            # 如果 get_iter 未找到足够的对等节点，可以考虑使用 find_neighbors 作为备用方案
            # (If get_iter doesn't find enough peers, consider find_neighbors as a fallback)
            if len(peers_found) < max_peers:
                logger.debug(f"get_iter 为 infohash {infohash_bytes.hex()} 找到的对等节点不足 ({len(peers_found)}/{max_peers})，尝试 find_neighbors... "
                             f"(get_iter found insufficient peers ({len(peers_found)}/{max_peers}) for infohash {infohash_bytes.hex()}, trying find_neighbors...)")
                target_node = Node(infohash_bytes) 
                # find_neighbors 返回 Kademlia Node 对象 (find_neighbors returns Kademlia Node objects)
                # k 参数控制返回的邻居数量 (k parameter controls number of neighbors returned)
                # 我们可能需要获取比 max_peers 更多的邻居，因为并非所有邻居都是有效的对等节点
                # (We might want to fetch more neighbors than max_peers as not all are valid peers)
                num_neighbors_to_find = max_peers - len(peers_found) # 需要找到的额外对等节点数 (Number of additional peers needed)
                if self.server.protocol and hasattr(self.server.protocol, 'router'): # 确保 router 可用 (Ensure router is available)
                    neighbor_nodes = await self.server.protocol.router.find_neighbors(target_node, k=num_neighbors_to_find * 2) # 获取更多潜在节点 (Fetch more potential nodes)
                    for node in neighbor_nodes:
                        if node.ip and node.port: # 确保节点有 IP 和端口 (Ensure node has IP and port)
                            peer_tuple = (node.ip, node.port)
                            if peer_tuple not in peers_found:
                                peers_found.append(peer_tuple)
                                logger.debug(f"通过 find_neighbors 为 infohash {infohash_bytes.hex()} 找到潜在节点: {node.ip}:{node.port} "
                                             f"(Found potential node via find_neighbors for infohash {infohash_bytes.hex()}: {node.ip}:{node.port})")
                                if len(peers_found) >= max_peers:
                                    break
                        else:
                            logger.debug(f"通过 find_neighbors 找到的节点缺少 IP/端口: {node.id.hex()} "
                                         f"(Node found via find_neighbors missing IP/port: {node.id.hex()})")
                else:
                    logger.warning("DHT 服务器协议或路由表不可用，无法使用 find_neighbors。 "
                                   "(DHT server protocol or router not available, cannot use find_neighbors.)")

            logger.info(f"为 infohash {infohash_bytes.hex()} 找到 {len(peers_found)} 个对等节点/潜在节点。 "
                        f"(Found {len(peers_found)} peers/potential nodes for infohash {infohash_bytes.hex()}.)")

        except Exception as e:
            logger.error(f"为 infohash {infohash_bytes.hex()} 获取对等节点时出错: {e} "
                         f"(Error getting peers for infohash {infohash_bytes.hex()}: {e})", exc_info=True)
        
        return peers_found[:max_peers] 


async def main_loop_example(): 
    """
    运行爬虫并处理 infohashes 的示例主循环。
    (Example main loop to run the crawler and process infohashes.)
    """
    # 配置基本日志记录以进行独立测试 (Configure basic logging for standalone testing)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    infohash_q = asyncio.Queue()
    client_peer_id = hashlib.sha1(str(asyncio.runners._main_thread_id).encode() + b'-DHTCRAWLER').digest()
    crawler = DHTCrawler(infohash_queue=infohash_q, node_id=client_peer_id[:20]) 

    try:
        await crawler.start(port=6889) 
        logger.info(f"DHT 爬虫正在运行。路由表大小: {crawler.get_routing_table_size()} "
                    f"(DHT Crawler is running. Routing table size: {crawler.get_routing_table_size()})")
        
        async def process_queue():
            while True:
                try:
                    infohash_bytes = await asyncio.wait_for(infohash_q.get(), timeout=5.0)
                    infohash_hex = binascii.hexlify(infohash_bytes).decode('utf-8')
                    logger.info(f"从队列中发现 Infohash: {infohash_hex} (Discovered Infohash (from queue): {infohash_hex})")
                    
                    peers = await crawler.get_peers_for_infohash(infohash_bytes, 10) 
                    if peers:
                        logger.info(f"为 {infohash_hex} 找到的对等节点: {peers} (Peers found for {infohash_hex}: {peers})")
                    else:
                        logger.info(f"未为 {infohash_hex} 找到对等节点。 (No peers found for {infohash_hex}.)")
                        
                    infohash_q.task_done() 
                except asyncio.TimeoutError:
                    # 独立测试时记录路由表大小 (Log routing table size during standalone test)
                    logger.debug(f"当前路由表大小: {crawler.get_routing_table_size()} (Current routing table size: {crawler.get_routing_table_size()})")
                    pass 
                except Exception as e:
                    logger.error(f"处理来自队列的 infohash 时出错: {e} (Error processing infohash from queue: {e})")

        processing_task = asyncio.create_task(process_queue())
        await asyncio.sleep(300) 
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            logger.info("处理任务已取消。 (Processing task cancelled.)")


    except KeyboardInterrupt:
        logger.info("收到键盘中断。正在关闭... (Keyboard interrupt received. Shutting down...)")
    except OSError as e:
        logger.error(f"无法启动爬虫: {e} (Could not start crawler: {e})")
    except Exception as e:
        logger.error(f"主循环遇到错误: {e} (Main loop encountered an error: {e})")
    finally:
        await crawler.stop()
        logger.info("应用程序已关闭。 (Application shut down.)")

if __name__ == "__main__":
    import hashlib 
    try:
        asyncio.run(main_loop_example())
    except KeyboardInterrupt:
        logger.info("应用程序被用户终止。 (Application terminated by user.)")
    except Exception as e:
        logger.critical(f"asyncio.run 中发生未处理的异常: {e} (Unhandled exception in asyncio.run: {e})")

```python
import asyncio
import logging
import binascii
import socket # 用于 IP 地址转换 (For IP address conversion)
import struct # 用于解包紧凑的节点/对等方信息 (For unpacking compact node/peer info)


from kademlia.network import Server
from kademlia.storage import ForgetfulStorage
from kademlia.node import Node # 导入 Node 类以进行类型提示和使用 (Import Node class for type hinting and usage)

# 获取此模块的日志记录器 (Get logger for this module)
# 日志级别和处理器由主脚本 (main_crawler.py) 配置
# (Log level and handlers are configured by the main script (main_crawler.py))
logger = logging.getLogger(__name__)

# 默认引导节点 (Default bootstrap nodes)
DEFAULT_BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

class InfohashQueueStorage(ForgetfulStorage):
    """
    一个 Kademlia 存储后端，它拦截来自 set 和 get 操作的键 (infohashes)，
    并将它们放入一个 asyncio.Queue 中。
    (A Kademlia storage backend that intercepts keys (infohashes) from
    set and get operations and puts them onto an asyncio.Queue.)
    """
    def __init__(self, ttl=3600, infohash_queue=None): # 默认 TTL: 1 小时 (Default TTL: 1 hour)
        super().__init__(ttl)
        if infohash_queue is None:
            # infohash_queue 必须提供错误处理 (infohash_queue must be provided error handling)
            raise ValueError("infohash_queue 必须提供 (infohash_queue must be provided)")
        self.infohash_queue = infohash_queue
        # 初始化日志记录已移至 DHTCrawler 或主脚本 (Initialization logging moved to DHTCrawler or main script)
        # logger.info("InfohashQueueStorage 已初始化。 (InfohashQueueStorage initialized.)")

    def _log_and_queue_key(self, key, operation_name):
        # Kademlia 键是字节串。转换为十六进制以便记录/可读性。
        # 这些键是 Kademlia ID，对于 BitTorrent DHT 来说是 infohashes。
        # (Kademlia keys are bytes. Convert to hex for logging/readability.
        # These keys are the Kademlia IDs, which for BitTorrent DHT are the infohashes.)
        try:
            hex_key = binascii.hexlify(key).decode('utf-8')
            # 记录拦截到的 infohash，级别为 DEBUG (Log intercepted infohash at DEBUG level)
            logger.debug(f"存储操作 '{operation_name}' 拦截到 infohash: {hex_key} "
                         f"(Storage operation '{operation_name}' intercepted infohash: {hex_key})")
            # 确保我们将字节串放入队列中，因为 infohashes 通常是字节串
            # (Ensure we are putting bytes into the queue as infohashes are typically bytes)
            self.infohash_queue.put_nowait(key)
        except Exception as e:
            # 键处理错误 (Error processing key)
            logger.error(f"处理键 {key!r} 时在 {operation_name} 中出错: {e} (Error processing key {key!r} in {operation_name}: {e})")


    async def set(self, key, value, original_publisher=False, original_publish_time=0):
        """
        当收到 STORE RPC 时调用。
        键是 infohash。
        (Called when a STORE RPC is received.
        The key is the infohash.)
        """
        self._log_and_queue_key(key, "set (announce_peer)")
        return await super().set(key, value, original_publisher, original_publish_time)

    async def get(self, key):
        """
        当收到 FIND_VALUE RPC 时调用 (在 BitTorrent DHT 中是 get_peers)。
        键是 infohash。
        (Called when a FIND_VALUE RPC is received (get_peers in BitTorrent DHT).
        The key is the infohash.)
        """
        self._log_and_queue_key(key, "get (get_peers)")
        return await super().get(key)

    def __getitem__(self, key):
        self._log_and_queue_key(key, "getitem (local lookup)")
        return super().__getitem__(key)


class DHTCrawler:
    """
    BitTorrent DHT 网络的爬虫。
    (A crawler for the BitTorrent DHT network.)
    """
    def __init__(self, bootstrap_nodes=None, infohash_queue=None, node_id=None):
        if bootstrap_nodes is None:
            bootstrap_nodes = DEFAULT_BOOTSTRAP_NODES
        if infohash_queue is None:
            raise ValueError("infohash_queue (asyncio.Queue) 必须提供。 (infohash_queue (asyncio.Queue) must be provided.)")

        self.bootstrap_nodes = bootstrap_nodes
        self.infohash_queue = infohash_queue
        self.server = None
        self.node_id = node_id 
        self.custom_storage = InfohashQueueStorage(infohash_queue=self.infohash_queue)
        logger.info(f"DHTCrawler 已初始化。节点 ID: {node_id.hex() if node_id else '自动生成'} "
                    f"(DHTCrawler initialized. Node ID: {node_id.hex() if node_id else 'Auto-generated'})")

    async def start(self, port=6881):
        """
        启动 DHT 爬虫。
        (Starts the DHT crawler.)
        """
        logger.info(f"正在启动 DHT 爬虫，端口为 {port}... (Starting DHT crawler on port {port}...)")
        try:
            self.server = Server(
                ksize=20, 
                alpha=3,  
                id=self.node_id, 
                storage=self.custom_storage 
            )
            await self.server.listen(port)
            logger.info(f"DHT 服务器已成功在端口 {port} 上监听。 (DHT server successfully listening on port {port}.)")

            if self.bootstrap_nodes:
                logger.info(f"正在使用节点进行引导: {self.bootstrap_nodes} (Bootstrapping with nodes: {self.bootstrap_nodes})")
                bootstrap_result = await self.server.bootstrap(self.bootstrap_nodes)
                # 记录引导结果的摘要 (Log summary of bootstrap results)
                # bootstrap_result 是一个 (found_nodes, looked_up_ids) 的元组
                # (bootstrap_result is a tuple of (found_nodes, looked_up_ids))
                if isinstance(bootstrap_result, tuple) and len(bootstrap_result) == 2:
                    logger.info(f"引导过程已完成。找到 {len(bootstrap_result[0])} 个节点，查询了 {len(bootstrap_result[1])} 个 ID。 "
                                f"(Bootstrap process completed. Found {len(bootstrap_result[0])} nodes, looked up {len(bootstrap_result[1])} IDs.)")
                else: # 如果格式未知，则记录原始结果 (Log raw result if format is unknown)
                    logger.info(f"引导过程已完成。结果: {bootstrap_result} (Bootstrap process completed. Results: {bootstrap_result})")

            else:
                logger.warning("未提供引导节点。节点可能无法有效连接到 DHT 网络。 "
                               "(No bootstrap nodes provided. The node may not connect to the DHT network effectively.)")

            logger.info("DHT 爬虫已成功启动。 (DHT Crawler started successfully.)")
        except OSError as e:
            logger.error(f"在端口 {port} 上启动 DHT 爬虫失败: {e}。端口可能已被占用或您可能缺少权限。 "
                         f"(Failed to start DHT crawler on port {port}: {e}. "
                         "The port might be in use or you might lack permissions.)")
            raise
        except Exception as e:
            logger.error(f"爬虫启动期间发生意外错误: {e} (An unexpected error occurred during crawler startup: {e})", exc_info=True)
            raise

    async def stop(self):
        """
        停止 DHT 爬虫。
        (Stops the DHT crawler.)
        """
        if self.server:
            logger.info("正在停止 DHT 爬虫... (Stopping DHT crawler...)")
            if hasattr(self.server, 'stop') and callable(self.server.stop): 
                 self.server.stop() 
            self.server = None 
            logger.info("DHT 爬虫已停止。 (DHT crawler stopped.)")
        else:
            logger.info("DHT 爬虫未在运行或已停止。 (DHT crawler is not running or already stopped.)")

    def get_routing_table_size(self) -> int:
        """
        获取路由表中活动节点的数量。
        (Get the number of active nodes in the routing table.)
        Returns:
            int: 路由表中的节点数。 (Number of nodes in the routing table.)
        """
        if self.server and hasattr(self.server, 'protocol') and hasattr(self.server.protocol, 'router'):
            try:
                # router.get_node_count() 是一个更直接的方法 (router.get_node_count() is a more direct method)
                if hasattr(self.server.protocol.router, 'get_node_count'):
                    return self.server.protocol.router.get_node_count()
                # 备用方法：遍历 KBuckets (Fallback: iterate KBuckets)
                return sum(len(bucket.get_nodes()) for bucket in self.server.protocol.router.buckets)
            except Exception as e:
                logger.error(f"获取路由表大小时出错: {e} (Error getting routing table size: {e})")
                return 0 
        return 0 


    async def get_peers_for_infohash(self, infohash_bytes: bytes, max_peers: int) -> list[tuple[str, int]]:
        """
        为给定的 infohash 查找对等节点。
        (Finds peers for a given infohash.)
        """
        if not self.server:
            logger.warning("DHT 服务器未运行，无法获取对等节点。 (DHT server is not running, cannot get peers.)")
            return []

        infohash_hex = infohash_bytes.hex() # 仅用于日志记录 (For logging only)
        logger.debug(f"正在为 infohash {infohash_hex} 查找对等节点... (Finding peers for infohash {infohash_hex}...)")
        
        peers_found = []
        try:
            count = 0
            async for value in self.server.get_iter(infohash_bytes): 
                if isinstance(value, bytes) and len(value) % 6 == 0: 
                    for i in range(0, len(value), 6):
                        try:
                            ip_bytes, port_bytes = value[i:i+4], value[i+4:i+6]
                            ip_addr = socket.inet_ntoa(ip_bytes) 
                            port = struct.unpack(">H", port_bytes)[0] 
                            if ip_addr != "0.0.0.0" and 0 < port <= 65535: 
                                peer_tuple = (ip_addr, port)
                                if peer_tuple not in peers_found: 
                                     peers_found.append(peer_tuple)
                                     logger.debug(f"通过 get_iter 为 infohash {infohash_hex} 找到对等节点: {ip_addr}:{port} "
                                                  f"(Found peer via get_iter for infohash {infohash_hex}: {ip_addr}:{port})")
                                     count += 1
                                     if count >= max_peers:
                                         break
                        except (socket.error, struct.error) as e: 
                             logger.warning(f"解析对等节点信息时出错: {e}。数据: {value[i:i+6]!r} "
                                            f"(Error parsing peer info: {e}. Data: {value[i:i+6]!r})")
                if count >= max_peers:
                    break
            
            if len(peers_found) < max_peers:
                logger.debug(f"get_iter 为 infohash {infohash_hex} 找到的对等节点不足 ({len(peers_found)}/{max_peers})，尝试 find_neighbors... "
                             f"(get_iter found insufficient peers ({len(peers_found)}/{max_peers}) for infohash {infohash_hex}, trying find_neighbors...)")
                target_node = Node(infohash_bytes) 
                num_neighbors_to_find = max_peers - len(peers_found) 
                if self.server.protocol and hasattr(self.server.protocol, 'router'): 
                    # 确保 k 值至少为 1，如果 num_neighbors_to_find 为 0 或负数 (Ensure k is at least 1 if num_neighbors_to_find is 0 or negative)
                    k_value_for_neighbors = max(1, num_neighbors_to_find * 2) 
                    neighbor_nodes = await self.server.protocol.router.find_neighbors(target_node, k=k_value_for_neighbors) 
                    for node in neighbor_nodes:
                        if node.ip and node.port: 
                            peer_tuple = (node.ip, node.port)
                            if peer_tuple not in peers_found:
                                peers_found.append(peer_tuple)
                                logger.debug(f"通过 find_neighbors 为 infohash {infohash_hex} 找到潜在节点: {node.ip}:{node.port} "
                                             f"(Found potential node via find_neighbors for infohash {infohash_hex}: {node.ip}:{node.port})")
                                if len(peers_found) >= max_peers:
                                    break
                        else:
                            logger.debug(f"通过 find_neighbors 找到的节点缺少 IP/端口: {node.id.hex()} "
                                         f"(Node found via find_neighbors missing IP/port: {node.id.hex()})")
                else:
                    logger.warning("DHT 服务器协议或路由表不可用，无法使用 find_neighbors。 "
                                   "(DHT server protocol or router not available, cannot use find_neighbors.)")

            logger.info(f"为 infohash {infohash_hex} 找到 {len(peers_found)} 个对等节点/潜在节点。 "
                        f"(Found {len(peers_found)} peers/potential nodes for infohash {infohash_hex}.)")

        except Exception as e:
            logger.error(f"为 infohash {infohash_hex} 获取对等节点时出错: {e} "
                         f"(Error getting peers for infohash {infohash_hex}: {e})", exc_info=True)
        
        return peers_found[:max_peers] 


async def main_loop_example(): 
    """
    运行爬虫并处理 infohashes 的示例主循环。
    (Example main loop to run the crawler and process infohashes.)
    """
    # 此示例的日志配置 (Logging configuration for this example)
    example_log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=example_log_format)

    infohash_q = asyncio.Queue()
    client_peer_id = hashlib.sha1(str(asyncio.runners._main_thread_id).encode() + b'-DHTCRAWLER').digest()
    crawler = DHTCrawler(infohash_queue=infohash_q, node_id=client_peer_id[:20]) 

    try:
        await crawler.start(port=6889) 
        logger.info(f"DHT 爬虫正在运行。路由表大小: {crawler.get_routing_table_size()} "
                    f"(DHT Crawler is running. Routing table size: {crawler.get_routing_table_size()})")
        
        async def process_queue():
            while True:
                try:
                    infohash_bytes = await asyncio.wait_for(infohash_q.get(), timeout=5.0)
                    infohash_hex = binascii.hexlify(infohash_bytes).decode('utf-8')
                    logger.info(f"从队列中发现 Infohash: {infohash_hex} (Discovered Infohash (from queue): {infohash_hex})")
                    
                    peers = await crawler.get_peers_for_infohash(infohash_bytes, 10) 
                    if peers:
                        logger.info(f"为 {infohash_hex} 找到的对等节点: {peers} (Peers found for {infohash_hex}: {peers})")
                    else:
                        logger.info(f"未为 {infohash_hex} 找到对等节点。 (No peers found for {infohash_hex}.)")
                        
                    infohash_q.task_done() 
                except asyncio.TimeoutError:
                    logger.debug(f"当前路由表大小: {crawler.get_routing_table_size()} (Current routing table size: {crawler.get_routing_table_size()})")
                    pass 
                except Exception as e:
                    logger.error(f"处理来自队列的 infohash 时出错: {e} (Error processing infohash from queue: {e})")

        processing_task = asyncio.create_task(process_queue())
        # 允许运行一段时间以进行测试 (Allow to run for some time for testing)
        # 例如，运行 60 秒 (e.g., run for 60 seconds)
        await asyncio.sleep(60) 
        logger.info("测试时间结束，正在取消处理任务... (Test duration ended, cancelling processing task...)")
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            logger.info("处理任务已成功取消。 (Processing task successfully cancelled.)")


    except KeyboardInterrupt:
        logger.info("收到键盘中断。正在关闭... (Keyboard interrupt received. Shutting down...)")
    except OSError as e:
        logger.error(f"无法启动爬虫: {e} (Could not start crawler: {e})")
    except Exception as e:
        logger.error(f"主循环遇到错误: {e} (Main loop encountered an error: {e})", exc_info=True) # 添加 exc_info=True (Add exc_info=True)
    finally:
        logger.info("正在停止 DHT 爬虫 (main_loop_example)... (Stopping DHT Crawler (main_loop_example)...)")
        await crawler.stop()
        logger.info("应用程序 (main_loop_example) 已关闭。 (Application (main_loop_example) shut down.)")

if __name__ == "__main__":
    import hashlib 
    try:
        asyncio.run(main_loop_example())
    except KeyboardInterrupt:
        logger.info("应用程序被用户终止。 (Application terminated by user.)")
    except Exception as e:
        # 确保在顶层捕获并记录任何未处理的异常 (Ensure any unhandled exceptions at the top level are caught and logged)
        logger.critical(f"asyncio.run 中发生未处理的异常: {e} (Unhandled exception in asyncio.run: {e})", exc_info=True)

```
