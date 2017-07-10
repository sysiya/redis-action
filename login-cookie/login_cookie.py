import threading
import time
import unittest
import uuid
from urllib.parse import urlparse, parse_qs

from redis import Redis


# region 登录 Cookie
def check_token(conn: Redis, token: str) -> str:
    """
    通过令牌获取用户。

    :param conn:    Redis 连接
    :param token:   用户令牌
    :return:
    """
    assert isinstance(conn, Redis)
    return conn.hget('login:', token)


def update_token(conn: Redis, token: str, user: str, item: str = None) -> None:
    """
    记录用户最后一次浏览记录以及浏览时间。

    :param conn:    Redis 连接
    :param token:   用户令牌
    :param user:    用户
    :param item:    浏览条目
    :return:
    """
    # 获取当前时间戳
    timestamp = time.time()
    # 维持令牌与登录用户的映射
    conn.hset('login:', token, user)
    # 更新令牌最后一次的设置时间
    conn.zadd('recent:', token, timestamp)
    # 记录用户最近浏览记录，只保存 25 条记录
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        # 为缓存页面，记录所有用户的浏览记录
        conn.zincrby('viewed:', item, -1)

QUIT = False
LIMIT = 10000000  #令牌上限为 1000W，开发与测试时可调小


def clean_sessions(conn: Redis) -> None:
    """
    定期清理超过上限的会话相关记录。

    定期清理会话，可以减轻内存压力。
    清除会话时，一并清理用户登录信息，浏览记录和最近更新的令牌信息。

    :param conn:    Redis 连接
    :return:
    """
    while not QUIT:
        # 找出目前已有令牌的数量
        size = conn.zcard('recent:')

        # 当目前已有令牌的数量没有超过上限时，休眠 1 秒
        if size <= LIMIT:
            time.sleep(1)
            continue

        # 获取需要移除的令牌 ID，最多移除 100 个旧令牌
        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index - 1)

        # 暂存要删除的令牌对应的浏览记录
        session_keys = []
        for token in tokens:
            session_keys.append('viewed:' + token)

        # 删除浏览记录
        conn.delete(*session_keys)
        # 删除登录令牌
        conn.hdel('login:', *tokens)
        # 删除令牌最近更新记录
        conn.zrem('recent:', *tokens)


# endregion

# region 购物车 Cookie
def add_to_cart(conn: Redis, session: str, item: str, count: int) -> None:
    """
    添加商品到购物车中，若商品数量小于等于 0，从购物车中删除该商品。

    :param conn:    Redis 连接
    :param session: 登录用户会话
    :param item:    商品名
    :param count:   商品数量，设为 0 或 负数，将从购物车中移除该商品
    :return:
    """
    if count <= 0:
        # 商品数量不大于 0，从购物车中删除该商品
        conn.hdel('cart:' + session, item)
    else:
        # 更新购物车中商品数量
        conn.hset('cart:' + session, item, count)


def clean_full_session(conn: Redis) -> None:
    """
    清除会话内容，在 clean_session 函数基础上再清除购物车信息。

    :param conn:    Redis 连接
    :return:
    """
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size - LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index - 1)

        session_keys = []
        for session in sessions:
            session_keys.append('viewed:' + session)
            session_keys.append('cart:' + session)

        conn.delete(*session_keys)
        conn.hdel('login:', *sessions)
        conn.zrem('recent:', *sessions)


# endregion

# region 缓存页面
def extract_item_id(request: str) -> str:
    """
    解析请求 URL，提取查询字符串中查询参数 item 的值。

    若查询参数 item 不包含在请求 URL 中，返回 None。

    :param request: 请求 URL
    :return:    返回查询参数 item 的值，
                若 item 不存在，返回 None
                若 item 有多个值，返回第一个值
    """
    # 解析 URL，分解成多个部分
    parsed = urlparse(request)
    # 提取查询字符串部分
    query = parse_qs(parsed.query)
    # 获取 item 查询参数的值
    return (query.get('item') or [None])[0]


def is_dynamic(request: str) -> bool:
    """
    如果请求 URL 的查询字符串中包括查询参数 '_'，表明该请求为动态请求，不可缓存。

    :param request: 请求 URL
    :return:    True - 动态资源请求
                False - 静态资源请求
    """
    parsed = urlparse(request)
    query = parse_qs(parsed.query)
    return '_' in query


def can_cache(conn: Redis, request: str) -> bool:
    """
    判断请求是否可以被缓存。

    缓存条件：
        1. 静态请求
        2. 商品页面的请求
        3. 商品排名比较高

    :param conn:    Redis 连接
    :param request: 请求 URL
    :return:    True：可以缓存
                False：不满足缓存条件
    """
    # 提取请求中的商品ID
    item_id = extract_item_id(request)
    # 检查是否为商品页面，是否可以静态缓存
    if not item_id or is_dynamic(request):
        return False

    # 从已访问的商品列表中获取商品ID的排名
    rank = conn.zrank('viewed:', item_id)
    # 检查商品是否达到缓存标准
    return rank is not None and rank < 10000


def hash_request(request: str) -> str:
    """
    将请求 URL 字符串转换为散列。

    :param request: 请求 URL
    :return:    返回散列形式的字符串
    """
    return str(hash(request))


def cache_request(conn: Redis, request: str, callback: any) -> str:
    # 如果不能缓存请求，直接调用回调函数
    if not can_cache(conn, request):
        return callback(request)

    # 将请求转换成散列键
    page_key = 'cache:' + hash_request(request)

    # 尝试查找缓存页面
    content = conn.get(page_key)

    # 若没有缓存，生成页面并缓存
    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)  #缓存 5 分钟

    return content


# endregion

# region 测试
class Test(unittest.TestCase):
    def setUp(self):
        # 设置 decode_responses 为 True，
        # 让 redis 返回 str 类型，而不是默认的 byte 类型，
        # 这样设置，可以减少从 byte 类型转换为 str 类型的工作量
        self.conn = Redis(db=15, decode_responses=True)

    def tearDown(self):
        conn = self.conn

        # 删除测试数据
        to_del = (
            conn.keys('login:*') + conn.keys('viewed:') + conn.keys('recent:'))
        if to_del:
            conn.delete(*to_del)

        # 释放 Redis 连接
        del self.conn

        # 恢复初始的全局变量
        global QUIT, LIMIT
        QUIT = False
        LIMIT = 10000000

    def test_login_cookie(self):
        """
        测试登录 Cookie
        :return:
        """
        # 引用全局变量
        global QUIT, LIMIT
        conn = self.conn

        # 生成一个令牌
        token = str(uuid.uuid4())

        # 绑定用户和令牌
        update_token(conn, token, 'username', 'Mac Book Pro')
        print('我们刚登录／更新了令牌：', token)
        print('登录用户：', 'username')
        print()

        # 获取当前令牌绑定的用户
        print('使用该令牌获得登录用户：')
        r = check_token(conn, token)
        print(r)
        print()
        self.assertTrue(r)

        print('为测试清除会话功能，将 Cookie 上限设为 0')
        print('我们将会启动一个线程来清理会话，之后再关闭该线程')

        # 将上限暂设为 0，便于测试清除会话功能
        LIMIT = 0
        # 启动线程运行 clean_sessions
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1)  # 设置为「守护线程」
        t.start()

        # 休眠 1 秒，结束线程
        time.sleep(1)
        QUIT = True

        # 休眠 2 秒，确认线程退出
        time.sleep(2)
        if t.isAlive():
            raise Exception('清除的线程还在?!')

        s = conn.hlen('login:')
        print('当前会话中的用户数量：', s)
        self.assertFalse(s)

    def test_shopping_cart_cookie(self):
        """
        测试购物车 Cookie
        :return:
        """
        conn = self.conn

        # 生成令牌
        token = str(uuid.uuid4())

        print('我们将刷新令牌...')
        update_token(conn, token, '用户', 'AKG Y45')

        print('添加商品到购物车...')
        add_to_cart(conn, token, 'MacBook Pro', 1)
        add_to_cart(conn, token, 'MacBook Air', 1)
        add_to_cart(conn, token, 'MacBook', 1)

        # 获取购物车的商品列表
        r = conn.hgetall('cart:' + token)
        print('当前购物车中的商品：', r)
        print()

        self.assertTrue(len(r) == 3)

        print('删除购物车中一件商品...')
        add_to_cart(conn, token, 'MacBook Air', 0)

        # 获取购物车的商品列表
        r = conn.hgetall('cart:' + token)
        print('当前购物车中的商品：', r)
        print()

        self.assertTrue(len(r) == 2)

        print('清空用户会话（包括购物车）')

        # 设置 LIMIT 为 0，让守护线程清除会话
        global LIMIT
        LIMIT = 0

        # 启动一个清除会话的守护线程
        t = threading.Thread(target=clean_full_session, args=(conn,))
        t.setDaemon(1)
        t.start()

        # 终止守护线程
        time.sleep(1)
        global QUIT
        QUIT = True

        # 确保守护线程已终止
        time.sleep(2)
        if t.isAlive():
            raise Exception('线程还没结束?!')

        # 测试购物车是否已清空
        r = conn.hgetall('cart:' + token)
        print('现在购物车中有：', r)

        self.assertFalse(r)

    def test_cache_request(self):
        """
        测试页面请求缓存。
        :return:
        """
        conn = self.conn
        # 生成令牌
        token = str(uuid.uuid4())

        # 模拟一个回调函数
        def callback(request):
            return request + ' 的请求内容'

        update_token(conn, token, 'Lu SiYu', 'ThinkPad')
        url = 'https://www.jd.com/?item=ThinkPad'
        print()
        print('我们将缓存请求：', url)
        result = cache_request(conn, url, callback)
        print('请求内容：', result)
        print()

        self.assertTrue(result)

        # 不返回请求内容的回调函数
        def nul(request):
            return request + ' 没有请求内容'

        print('为了测试是否成功缓存请求内容，我们将再次发送同一个请求，但提供另一个回调函数')
        result2 = cache_request(conn, url, nul)

        print('最终我们获得的请求内容: ', result2)

        self.assertEqual(result, result2)

        # 测试不包含 item 参数的请求
        self.assertFalse(can_cache(conn, 'https://www.jd.com/'))
        # 测试包含 '_' 的动态请求
        self.assertFalse(can_cache(conn, 'https://www.jd.com/?item=ThinkPad&_=123456'))

# endregion


if __name__ == '__main__':
    unittest.main()
