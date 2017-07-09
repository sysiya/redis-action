import time
import unittest

from redis import Redis


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

        # 暂存要删除的令牌
        session_keys = []
        for token in tokens:
            session_keys.append('viewed:' + token)

        # 删除浏览记录
        conn.delete(*session_keys)
        # 删除登录令牌
        conn.hdel('login:', *tokens)
        # 删除令牌最近更新记录
        conn.zrem('recent:', *tokens)


class Test(unittest.TestCase):
    def setUp(self):
        import redis

        # 设置 decode_responses 为 True，
        # 让 redis 返回 str 类型，而不是默认的 byte 类型，
        # 这样设置，可以减少从 byte 类型转换为 str 类型的工作量
        self.conn = redis.Redis(db=15, decode_responses=True)

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
        import uuid
        import threading

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


if __name__ == '__main__':
    unittest.main()
