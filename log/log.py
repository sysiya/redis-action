import logging
import time
import unittest
from datetime import datetime
from typing import Union

import redis
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


def log_common(conn: Redis, name: str, message: str,
               severity: Union[int, str] = logging.INFO,
               timeout: int = 5) -> None:
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'common:%s:%s' % (name, severity)
    # 使用有序集合存储成功记录当前日志消息时的小时数
    start_key = destination + ':start'  # 此处定义键名

    pipe = conn.pipeline()
    # 从现在（执行记录操作之前）开始计时，计算超时时刻
    end = time.time() + timeout
    # 每次执行循环时都判断当前时刻是否已到达超时时刻
    # 未达到超时时刻之前，会一直重试直到操作完成
    # 如果达到了超时时刻，操作还没有完成，就放弃本次操作
    while time.time() < end:
        try:
            # 如果其他客户端对与当前消息内容相同的日志消息执行了归档操作，
            # 就会重置消息内容与当前日志消息相同的小时数，为避免竞争条件，
            # 所以要监视 start_key 是否发生变更，若发生变更，就重试操作
            pipe.watch(start_key)

            # 获取当前时刻的小时数
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()

            # 获取日志记录时刻的小时数
            existing = pipe.get(start_key)

            # 标志事务开始
            pipe.multi()
            if not existing:
                # 如果当前记录的日志之前未记录过，就当前时刻的小时数
                pipe.set(start_key, hour_start)
            elif existing < hour_start:
                # 如果当前记录的日志已在「上个小时数」被记录了，
                # 将上个小时数记录的日志进行归档
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')

                # 因为小时数已发生变更
                # 重新记录「当前小时数」中当前日志出现的次数
                pipe.set(start_key, hour_start)

            # 增加当前日志消息在「当前小时数」内出现的次数
            pipe.zincrby(destination, message)

            # 记录日志到最近日志中
            # 注意：在 log_recent() 中将调用 pipe.execute()
            #      这样可以将两次记录的命令批量发送给 Redis 服务器
            log_recent(pipe, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue


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

    def test_log_common(self):
        import pprint
        conn = self.conn

        print('为了测试常见日志的记录，我们可以插入多条相同的日志')
        for count in range(1, 6):
            for i in range(count):
                log_common(conn, 'test', "这是第 %s 条消息" % count)

        common_message = conn.zrevrange('common:test:info',
                                        0, -1, withscores=True)
        print('\n当前已记录 %s 条常见消息\n' % len(common_message))
        print('存储的有序集合如下：')
        pprint.pprint(common_message)

        self.assertTrue(len(common_message) >= 5)


if __name__ == '__main__':
    unittest.main()
