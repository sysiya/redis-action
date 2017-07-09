import time
import unittest

# 一周换算的秒数
from redis import Redis

ONE_WEEK_IN_SCORES = 7 * 86400

# 每一票对应的评分数
VOTE_SCORE = 432


def article_vote(conn: Redis, user: str, article: str):
    """
    用户给文章投票。

    :param conn:    Redis 连接。
    :type conn: Redis
    :param user:    投票用户。
    :param article: 投票文章的键名，形如：article:83234
    :return:
    """
    # 计算文章截止投票时间
    cutoff = time.time() - ONE_WEEK_IN_SCORES
    # 当文章发布时间在一周之前，不允许投票
    if conn.zscore('time:', article) < cutoff:
        return

    # 取出文章ID
    article_id = article.partition(':')[1]
    # 如果用户第一次给该文章投票，
    # 增加文章的投票次数与评分
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)


def post_article(conn: Redis, user: str, title: str, link: str) -> str:
    """
    用户发表新文章，需要文章标题与文章链接。

    :param conn: Redis 连接
    :type conn: Redis
    :param user: 发表文章的用户
    :param title: 文章标题
    :param link: 文章链接
    :return:    新文章ID
    """
    # 生成一个新的文章ID
    article_id = str(conn.incr('article:'))

    # 将发布文章的用户添加到该文章的已投票用户的名单中
    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SCORES)  # 该文章的已投票用户名单将会在一周后过期（删除）

    # 存储文章信息
    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1
    })

    # 添加文章到根据评分排序的有序集合里
    conn.zadd('score:', article, now + VOTE_SCORE)
    # 添加文章到根据时间排序的有序集合里
    conn.zadd('time:', article, now)

    return article_id


ARTICLE_PRE_PAGE = 25


def get_articles(conn: Redis, page: int, order: str = 'score:') -> list:
    """
    获取多个文章，按分页和排序，默认按评分排序

    :param conn:    Redis 连接
    :param page:    页面索引
    :param order:   排序方式，默认按评分排序
    :return:        返回指定页面的多篇有序文章
    """
    # 设置获取一页文章的起始索引和结束索引
    start = (page - 1) * ARTICLE_PRE_PAGE
    end = start + ARTICLE_PRE_PAGE - 1

    # 获取多个文章ID
    article_ids = conn.zrevrange(order, start, end)  # redis 默认从小到大排序，故使用 ZREVRANGE 方法反序获取
    articles = []
    # 获取每篇文章的详细信息
    for article_id in article_ids:
        article_data = conn.hgetall(article_id)
        article_data['id'] = article_id
        articles.append(article_data)

    return articles


def add_remove_groups(conn: Redis, article_id: str, to_add: list = [], to_remove: list = []) -> None:
    """
    添加一篇文章到群组，或者从群组中移除某一篇文章。

    使用集合来储存群组列表，是为了能和有序集合进行并集操作，
    用于筛选出单个分组的所有有序文章

    :param article_id:  添加或移除的文章ID
    :param conn:        Redis 连接
    :param to_add:      要添加该文章的群组列表
    :param to_remove:   要移除该文章的群组列表
    :return:
    """
    article = 'article:' + article_id
    # 添加文章到群组
    for group in to_add:
        conn.sadd('group:' + group, article)
    # 从群组删除文章
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn: Redis, group: str, page: int, order: str = 'score:') -> list:
    """
    获取某一分组的文章，支持分页和排序，默认按评分进行排序。

    :param conn:    Redis 连接
    :param group:   文章分组
    :param page:    页面索引
    :param order:   排序方式
    :return:        返回指定分页下的某一分组的文章，按评分进行排序
    """
    # 每个群组的每种排序方式都创建一个 key
    key = order + group

    # 如果没有缓存排序结果，创建一个新的排序结果
    if not conn.exists(key):
        conn.zinterstore(key,
                         ['group:' + group, order],
                         aggregate='max')
        conn.expire(key, 60)  # 60 秒后清除缓存

    return get_articles(conn, page, key)


class Test(unittest.TestCase):
    # 开始测试前
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    # 测试结束后
    def tearDown(self):
        del self.conn
        print()
        print()

    # 测试文章相关功能
    def test_article_functionality(self):
        conn = self.conn
        from pprint import pprint

        # 测试-发表文章
        article_id = str(post_article(conn, 'username', 'A title', 'https://www.google.com'))
        print('我们发表了一篇新文章，文章 ID：', article_id)
        print()
        self.assertTrue(article_id)

        print('新文章的散列如下所示：')
        r = conn.hgetall('article:' + article_id)
        print(r)
        print()
        self.assertTrue(r)

        # 测试-投票
        article_vote(conn, 'other_user', 'article:' + article_id)
        print('我们现在为该文章投了一票，它现在的票数：')
        v = int(conn.hget('article:' + article_id, 'votes'))
        print(v)
        print()
        self.assertTrue(v > 1)

        print('当前评分最高的文章：')
        articles = get_articles(conn, 1)
        pprint(articles)
        print()
        self.assertTrue(len(articles) >= 1)

        # 测试-添加群组
        add_remove_groups(conn, article_id, ['new-group'])
        print('我们刚添加了该文章到 new-group 分组中，该分组下文章：')
        articles = get_group_articles(conn, 'new-group', 1)
        pprint(articles)
        print()
        self.assertTrue(len(articles) >= 1)

        # 删除所有数据
        to_del = (
            conn.keys('time:*') + conn.keys('voted:*') + conn.keys('score:*') +
            conn.keys('article:*') + conn.keys('group:*')
        )
        if to_del:
            conn.delete(*to_del)


if __name__ == '__main__':
    unittest.main()
