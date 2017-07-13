import logging
import time
import unittest
from typing import Union

from redis import Redis
from redis.client import Pipeline

# 设置字典，将大部分日志的安全级别映射为字符串
SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARN: 'warn',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical',
}
# 添加字符串键名
SEVERITY.update([(name, name) for name in SEVERITY.values()])


def log_recent(conn: Redis, name: str, message: str,
               severity: Union[int, str] = logging.INFO,
               pipe: Union[None, Pipeline] = None) -> None:
    """
    记录最近日志消息，存储到 Redis 服务器。

    :param conn:        Redis 连接
    :param name:        日志记录者
    :param message:     日志消息内容
    :param severity:    日志安全级别
    :param pipe:        Redis 流水线对象
    :return:
    """
    # 将日志的安全级别转换为简单的字符串
    severity = str(SEVERITY.get(severity, severity)).lower()
    # 存储日志消息的键名
    destination = 'recent:%s:%s' % (name, severity)
    # 在日志中记录发送时间
    message = time.asctime() + ' ' + message

    # 使用流水线只与 Redis 服务器通信一次
    pipe = pipe or conn.pipeline()
    # 将新日志消息添加到日志列表的最前面
    pipe.lpush(destination, message)
    # 只保留最新的 100 条消息
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


class Test(unittest.TestCase):
    def setUp(self):
        self.conn = Redis(db=15, decode_responses=True)
        # 清空数据库
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()
        del self.conn

    def test_log_recent(self):
        import pprint
        conn = self.conn

        for i in range(5):
            log_recent(conn, 'test', '这是第 %s 条消息' % (i + 1))

        recent = conn.lrange('recent:test:info', 0, -1)
        print('\n当前已记录 %s 条消息\n' % len(recent))
        pprint.pprint(recent[:10])

        self.assertTrue(len(recent) >= 5)


if __name__ == '__main__':
    unittest.main()
