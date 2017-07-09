import time
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
        # 删除
        conn.zrem('recent:', *tokens)
