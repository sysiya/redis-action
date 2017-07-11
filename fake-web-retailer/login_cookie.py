import json
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
        # 为缓存页面，记录所有用户的浏览记录。
        # 每一件商品浏览一次，对应的分数就减少 1 分，
        # 这样浏览次数越多的商品就越靠前。
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


def rescale_viewed(conn: Redis) -> None:
    """守护进程函数
    优化页面缓存，低内存占用率。

    当浏览记录超过 2W 条时，清空多余的不流行的商品页面缓存，
    保留前 2W 条最流行的商品的缓存页面。

    每次删除操作都将浏览次数减半，即降低分数的数值，但排名不变。

    :param conn: Redis 连接
    :return:
    """
    while not QUIT:
        # 删除排名再 20000 名之后的浏览记录
        conn.zremrangebyrank('viewed:', 20000, -1)
        # 将浏览次数降低为原来的一半
        conn.zinterstore('viewed:', {'viewed:': .5})
        # 5 分钟后重试
        time.sleep(300)


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
        conn.setex(page_key, content, 300)  # 缓存 5 分钟

    return content


# endregion

# region 缓存数据行
class Inventory(object):
    """
    商品库存类，这里主要模拟返回数据库的实际数据
    """

    def __init__(self, inv_id):
        self.id = inv_id

    @classmethod
    def get(cls, inv_id):
        return Inventory(inv_id)

    def to_dict(self):
        return {'id': self.id, 'data': '要缓存的数据', 'cached': time.time()}


def schedule_row_cache(conn: Redis, row_id: str, delay: float) -> None:
    """
    对要缓存的数据行进行调度，为每行缓存数据设置缓存延迟时间（即该数据行下一次被缓存的时间间隔）。

    这里的数据行指用户需要频繁读取关系数据库存储在硬盘里的数据。

    :param conn:    Redis 连接
    :param row_id:  数据行 ID
    :param delay:   数据行缓存延迟时间
    :return:
    """
    # 设置要缓存的数据行的延迟值
    conn.zadd('delay:', row_id, delay)
    # 立即对要缓存的数据行进行调度
    conn.zadd('schedule:', row_id, time.time())


def cache_rows(conn):
    """
    守护进程函数，用于定时缓存数据行。

    :param conn:
    :return:
    """
    while not QUIT:
        # 尝试获取下一条需要缓存的数据行及该行的调度时间戳
        # 返回 0 | 1 个元组的列表
        next_row = conn.zrange('schedule:', 0, 0, withscores=True)

        # 暂时没有行需要被缓存或
        # 需要缓存的数据行的调度时间（已缓存过的要加上延迟）大于当前时间，
        # 休眠 50 毫秒后重试
        now = time.time()
        if not next_row or next_row[0][1] > now:
            time.sleep(.05)
            continue

        # 获取下一条需要缓存的数据行的缓存延迟时间
        row_id = next_row[0][0]
        delay = conn.zscore('delay:', row_id)
        # 如果数据行的缓存延迟时间 <= 0，不再缓存该数据行
        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inventory:' + row_id)
            continue

        # 从库存中读取数据行
        row = Inventory.get(row_id)
        # 更新调度时间（当度过缓存延迟时间之后再缓存）
        conn.zadd('schedule:', row_id, now + delay)
        # 缓存数据行的 JSON 格式
        conn.set('inventory:' + row_id, json.dumps(row.to_dict()))


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
        to_del = (conn.keys('login:*') + conn.keys('viewed:*') + conn.keys('recent:*') +
                  conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
                  conn.keys('schedule:*') + conn.keys('inventory:*'))
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

    def test_cache_rows(self):
        import pprint
        conn = self.conn

        print('首先，让我们每 5 秒缓存 MacBook Pro 的页面')
        schedule_row_cache(conn, 'MacBook Pro', 5)

        print('我们已成功定义调度任务：')
        # 获取所有的调度任务
        # 返回 n (n >= 0) 个元组的列表
        s = conn.zrange('schedule:', 0, -1, withscores=True)
        pprint.pprint(s)

        self.assertTrue(s)

        print('接着，我们启动一个缓存线程，来缓存数据')
        # 启动守护线程，缓存数据行
        t = threading.Thread(target=cache_rows, args=(conn,))
        t.setDaemon(1)
        t.start()

        time.sleep(1)
        print('我们缓存的数据：')
        # 获取缓存的数据
        r = conn.get('inventory:MacBook Pro')
        print(repr(r))
        print()
        self.assertTrue(r)

        print('过 5 秒之后，我们再检查缓存的数据...')
        time.sleep(5)
        print('注意现在数据已经产生了变化')
        # 此时数据的延时时间已重设
        r2 = conn.get('inventory:MacBook Pro')
        print(repr(r2))
        print()
        self.assertTrue(r2)
        self.assertTrue(r != r2)

        print('让我们强制清空缓存')
        # 删除缓存数据
        schedule_row_cache(conn, 'MacBook Pro', -1)
        time.sleep(1)
        # 尝试获取缓存数据
        r = conn.get('inventory:MacBook Pro')
        print('是否已清空缓存？', not r)
        print()
        self.assertFalse(r)

        # 终止缓存线程
        global QUIT
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception('数据库缓存线程还存活?!')


# endregion


if __name__ == '__main__':
    unittest.main()
