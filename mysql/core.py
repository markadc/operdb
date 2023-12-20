# -*- coding: utf-8 -*-

import re
import time

import pymysql
from dbutils.pooled_db import PooledDB
from loguru import logger
from pymysql.cursors import DictCursor


class MysqlHandler:
    def __init__(self, cfg: dict):
        cfg.setdefault('host', 'localhost')
        cfg.setdefault('port', 3306)
        cfg.setdefault('charset', 'utf8mb4')
        cfg.setdefault('maxconnections', 4)
        cfg.setdefault('mincached', 0)
        cfg.setdefault('maxcached', 0)
        cfg.setdefault('maxusage', 0)
        cfg.setdefault('blocking', True)
        self.cfg = cfg
        self.pool = PooledDB(pymysql, **self.cfg)

    def open_connect(self, dict_cursor=False):
        """打开连接"""
        con = self.pool.connection()
        cur = con.cursor(DictCursor) if dict_cursor else con.cursor()
        return cur, con

    @staticmethod
    def close_connect(cur, con):
        """关闭连接"""
        if cur:
            cur.close()
        if con:
            con.close()

    @staticmethod
    def log(name, sql, msg, level):
        """输出日志"""
        sql = re.sub('\s+', ' ', sql).strip()
        match level:
            case "WARNING":
                logger.warning(
                    '''
                    name    {}
                    sql     {}
                    msg     {}
                    '''.format(name, sql, msg)
                )
            case "ERROR":
                logger.error(
                    '''
                    name    {}
                    sql     {}
                    msg     {}
                    '''.format(name, sql, msg)
                )
            case _:
                ...

    def exe_sql(self, sql: str, args=None, query_all=None, dict_cursor=True) -> int | bool | dict | list | tuple:
        """
        执行SQL
        Args:
            sql: SQL语句
            args: 配合SQL语句的参数
            query_all: 为True调用fetchall，为False调用fetchone
            dict_cursor: 是否选择dict游标

        Returns:
            默认返回受影响的行数，SQL执行失败返回False，根据mode返回fetchone或者fetchall的数据
        """
        cur, con = None, None
        try:
            cur, con = self.open_connect(dict_cursor)
            line = cur.execute(sql, args=args)
            con.commit()
        except Exception as e:
            self.log('exe_sql', sql, e, level='ERROR')
            if con:
                con.rollback()
            return False
        else:
            return line if query_all is None else cur.fetchall() if query_all else cur.fetchone()
        finally:
            self.close_connect(cur, con)

    def exem_sql(self, sql: str, args=None) -> int | bool:
        """
        批量执行SQL
        Args:
            sql: SQL语句
            args: 配合SQL语句的参数

        Returns:
            默认返回受影响的行数，SQL执行失败返回False
        """
        cur, con = None, None
        try:
            cur, con = self.open_connect()
            line = cur.executemany(sql, args=args)
            con.commit()
        except Exception as e:
            self.log('exem_sql', sql, e, level='ERROR')
            if con:
                con.rollback()
            return False
        else:
            return line
        finally:
            self.close_connect(cur, con)

    @staticmethod
    def make_part(src: list | dict, add=True, mid=', ') -> str:
        """
        制作SQL语句的一部分
        Args:
            src: 源头
            add: 是否补充引号，当src是list时生效
            mid: 每个元素之间的连接

        Returns:
            SQL语句的一部分
        """
        if isinstance(src, (list, tuple, set)):
            part = mid.join(["'{}'".format(v) if add else str(v) for v in src])
            return part
        elif isinstance(src, dict):
            some = []
            for k, v in src.items():
                if v is True:
                    one = '{} is not null'.format(k)
                elif v is False or v is None:
                    one = '{} is null'.format(k)
                else:
                    one = "{}='{}'".format(k, v)
                some.append(one)
            part = mid.join(some)
            return part

    def check_values(self, table: str, values: list, field='id') -> tuple:
        """
        检查一些值
        Args:
            table: 表名
            values: 字段的多个值
            field: 哪一个字段

        Returns:
            (不存在的一些值，已存在的一些值)
        """
        tail = self.make_part(values)
        sql = 'select {} from {} where {} in ({})'.format(field, table, field, tail)
        datas = self.exe_sql(sql, query_all=2)
        old = [data[field] for data in datas]
        new = list(set(values) - set(old))
        return new, old

    def update(self, table: str, new: str | dict, cond: str | dict, limit: int = None) -> int:
        """
        更新
        Args:
            table: 表名
            new: 更新数据
            cond: 更新条件
            limit: 只更新多少条，为None则全部更新

        Returns:
            受影响的行数
        """
        mdy = new if isinstance(new, str) else self.make_part(new)
        cond = cond if isinstance(cond, str) else self.make_part(cond, mid=' and ')
        tail = '' if limit is None else 'limit {}'.format(limit)
        sql = 'update {} set {} where {} {}'.format(table, mdy, cond, tail)
        return self.exe_sql(sql)

    def update_some(self, table: str, new: str | dict, field: str, values: list) -> int:
        """
        更新一些数据，根据某个字段的多个值
        Args:
            table: 表名
            new: 更新数据
            field: 以此字段为条件进行更新
            values: field的多个值

        Returns:
            受影响的行数

        """
        mdy = new if isinstance(new, str) else self.make_part(new)
        where = '{} in ({})'.format(field, self.make_part(values))
        sql = 'update {} set {} where {}'.format(table, mdy, where)
        return self.exe_sql(sql)

    def query_some(self, table: str, select: str | list, field: str, values: list) -> list:
        """
        查询一些数据，根据某个字段的多个值
        Args:
            table: 表名
            select: 查询的字段
            field: 以此字段为条件进行查询
            values: field的多个值

        Returns:
            查询到的数据
        """
        selects = select if isinstance(select, str) else self.make_part(select, add=False)
        cond = '{} in ({})'.format(field, self.make_part(values))
        sql = 'select {} from {} where {}'.format(selects, table, cond)
        return self.exe_sql(sql, query_all=2)

    def add_one(self, table: str, item: dict, update: str = None, unique='id') -> int:
        """
        添加一条数据
        Args:
            table: 表名
            item: 这条数据
            update: 数据重复时更新数据
            unique: 有唯一索引的字段

        Returns:
            受影响的行数
        """
        f = ', '.join(item.keys())
        v = ', '.join(['%s'] * len(item.keys()))
        u = update or '{}={}'.format(unique, unique)
        args = tuple(item.values())
        sql = 'insert into {}({}) value({}) ON DUPLICATE KEY UPDATE {}'.format(table, f, v, u)
        line = self.exe_sql(sql, args=args)
        return line

    def add_many(self, table: str, items: list, update: str = None, unique='id') -> int:
        """
        添加多条数据
        Args:
            table: 表名
            items: 多条数据，每条数据的结构需一致
            update: 数据重复时更新数据
            unique: 有唯一索引的字段

        Returns:
            受影响的行数
        """
        f = ', '.join(items[0].keys())
        v = ', '.join(['%s'] * len(items[0].keys()))
        u = update or '{}={}'.format(unique, unique)
        args = [tuple(item.values()) for item in items]
        sql = 'insert into {}({}) value({}) ON DUPLICATE KEY UPDATE {}'.format(table, f, v, u)
        line = self.exem_sql(sql, args=args)
        return line

    def add_items(self, table: str, items: list, to_check: str) -> int:
        """
        添加多条数据，会自动过滤掉和指定字段在数据库中有重复值的数据
        Args:
            table: 表名
            items: 多条数据
            to_check: 指定检查每条数据中的哪一个字段

        Returns:
            受影响的行数
        """
        some = [v[to_check] for v in items]
        new, exists = self.check_values(table, some, field=to_check)
        items2 = [v for v in items if v[to_check] in new]
        return self.add_many(table, items2, unique=to_check) if items2 else 0

    def exists(self, table: str, **kwargs) -> int:
        """
        查询这条数据是否存在\n
        exists('user', name='CLOS', gender=18)
        Returns:
            存在返回1，反之0
        """
        cond = self.make_part(kwargs, mid=' and ')
        sql = 'select 1 from {} where {} limit 1'.format(table, cond)
        return self.exe_sql(sql)

    def update_one(self, table: str, one: dict, depend: str) -> int:
        """
        更新一条数据
        Args:
            table: 表名
            one: 这条数据，其字段需含有depend
            depend: 依赖此字段为条件进行更新

        Returns:
            受影响的行数
        """
        dv = one.pop(depend)
        temp = []
        args = []
        for k, v in one.items():
            temp.append('{}=%s'.format(k))
            args.append(v)
        s = ', '.join(temp)
        args.append(dv)
        sql = 'update {} set {} where {}=%s'.format(table, s, depend)
        return self.exe_sql(sql, args=args)

    @staticmethod
    def ensure_items(items: list, must: str):
        """
        校验items，item缺少字段或者结构不一致则引发异常
        Args:
            items: 多条数据
            must: 每条数据中必须存在的字段
        """
        fields = None
        for item in items:
            if fields is None:
                fields = set(item)
            else:
                assert must in fields, '缺少字段{}'.format(must)
                assert fields == set(item), '字段不一致'

    def update_many(self, table: str, many: list, depend: str) -> int:
        """
        批量更新数据
        Args:
            table: 表名
            many: 多条数据，每条数据其字段需含有depend
            depend: 依赖此字段为条件进行更新

        Returns:
            受影响的行数
        """
        self.ensure_items(many, depend)
        ks = list(many[0].keys())
        ks.remove(depend)
        mid = ', '.join(['{}=%s'.format(k) for k in ks])
        sql = 'update {} set {} where {}=%s'.format(table, mid, depend)
        args = []
        for one in many:
            vs = [one[k] for k in ks]
            vs.append(one[depend])
            args.append(vs)
        return self.exem_sql(sql, args)

    def quick_update(self, table, items: list, depend: str):
        """批量更新，只执行了1条SQL"""

        self.ensure_items(items, depend)
        keys = list(items[0].keys())
        keys.remove(depend)

        head = 'update {} set'.format(table)

        mid = ''
        args = []
        for key in keys:
            mid += '\t{} = case {}\n\t'.format(key, depend)
            for data in items:
                mid += 'when %s then %s '
                args.append(data[depend])
                args.append(data[key])
            else:
                mid += 'end,\n'
        mid = mid[:-2]

        values = ["'{}'".format(data[depend]) for data in items]
        tail = 'where {} in ({})'.format(depend, ', '.join(values))

        sql = '\n'.join([head, mid, tail])
        return self.exe_sql(sql, args=args)

    def random(self, table, limit=1) -> dict | list:
        """返回随机数据"""
        sql = 'select * from {} where id >= (rand() * (select max(id) from {})) limit {}'.format(table, table, limit)
        datas = self.exe_sql(sql, query_all=2 if limit > 1 else 1)
        return datas

    def query(self, table, pick='*', limit=None, **kwargs) -> list:
        """查询数据"""
        tail = '' if limit is None else 'limit {}'.format(limit)
        cond = 'where {}'.format(self.make_part(kwargs, mid=' and ')) if kwargs else ''
        sql = 'select {} from {} {} {}'.format(pick, table, cond, tail)
        datas = self.exe_sql(sql, query_all=2)
        return datas

    def query_count(self, table, **kwargs) -> int:
        """查询数量"""
        cond = 'where {}'.format(self.make_part(kwargs, mid=' and ')) if kwargs else ''
        sql = 'select count(1) from {} {}'.format(table, cond)
        count = self.exe_sql(sql, query_all=1, dict_cursor=False)[0]
        return count

    def get_min(self, table, field):
        """获取最小值"""
        sql = 'select min({}) from {}'.format(field, table)
        value = self.exe_sql(sql, query_all=1, dict_cursor=False)[0]
        return value

    def get_max(self, table, field):
        """获取最大值"""
        sql = 'select max({}) from {}'.format(field, table)
        value = self.exe_sql(sql, query_all=1, dict_cursor=False)[0]
        return value

    @staticmethod
    def _print(datas: list):
        """仅配合scan"""
        for data in datas:
            print(data)

    def scan(
            self, table: str, sort_field='id', pick='*',
            start: int = None, end: int = None,
            dealer=None, add_cond=None,
            once=1000, rest=0.05,
            max_query_times=None, log=True
    ):
        """
        扫描数据，每一批数据可以交给回调函数处理
        Args:
            table: 表名
            sort_field: 进行排序的字段
            pick: 查询哪些字段
            start: 排序字段的最小值
            end: 排序字段的最大值
            add_cond: 补充的SQL条件
            once: 每一批查询多少条
            rest: 每一批查询的间隔
            dealer: 每一批数据的回调函数
            log: 是否输出查询日志
            max_query_times: 最大查询次数
        """

        times = 0  # 查询了多少次
        dealer = dealer or self._print  # 具体的回调函数
        start, end = start or self.get_min(table, sort_field), end or self.get_max(table, sort_field)  # 查询区间

        fstq = True  # 第一次查询
        while True:
            symbol, cond = '>=' if fstq else '>', '' if add_cond is None else 'and ' + add_cond
            sql = '''
                select {} from {}
                where {} {} {} and {} <= {} {}
                order by {}
                limit {}
            '''.format(
                pick, table,
                sort_field, symbol, start, sort_field, end, cond,
                sort_field,
                once
            )

            result: list = self.exe_sql(sql, query_all=2)
            if result is False:
                self.log('scan', sql, '执行失败', level='ERROR')
                return
            if not result:
                self.log('scan', sql, '查询为空', level='WARNING')
                return

            # 输出查询日志
            if log is True:
                params = sort_field, symbol, start, once, len(result), result[0][sort_field], result[-1][sort_field]
                logger.info('{}{}{}  期望{}得到{}  具体{}到{}'.format(*params))

            # 查询出来的数据交给回调函数处理
            if len(result) == once:
                dealer(result)
                start = result[-1][sort_field]
                if start == end:
                    break
            else:
                dealer(result)
                break

            times += 1
            if max_query_times and times >= max_query_times:  # 达到最大查询次数了
                break

            fstq = False
            time.sleep(rest)  # 每一轮查询之间的间隔

    def delete_one(self, table, field, value):
        """删除一条"""
        sql = "delete from {} where {}='{}'".format(table, field, value)
        return self.exe_sql(sql)

    def delete_many(self, table, field, values: list):
        """删除多条"""
        part = self.make_part(values)
        sql = 'delete from {} where {} in ({})'.format(table, field, part)
        return self.exe_sql(sql)

    def gen_test_table(self, table: str, once=1000, total=10000):
        """生成测试表"""
        import random
        from faker import Faker

        faker = Faker("zh_cn")
        n = 0

        def create_table():
            """新建测试表"""
            sql = '''
                create table {}
                (
                    id          int NOT NULL    AUTO_INCREMENT,
                    name        varchar(20)     DEFAULT NULL,
                    gender      varchar(1)      DEFAULT NULL,
                    age         int(3)          DEFAULT NULL,
                    phone       varchar(11)     DEFAULT NULL,
                    ssn         varchar(18)     DEFAULT NULL,
                    job         varchar(200)    DEFAULT NULL,
                    salary      int(8)          DEFAULT NULL,
                    company     varchar(200)    DEFAULT NULL,
                    address     varchar(200)    DEFAULT NULL,
                    mark        varchar(1)      DEFAULT NULL,
                    primary key (id)
                ) 
                ENGINE=InnoDB    DEFAULT CHARSET=utf8mb4;
            '''.format(table)
            return False if self.exe_sql(sql) is False else True

        def get_item():
            """获取一条数据"""
            item = {
                'name': faker.name(),
                'gender': random.choice(['男', '女']),
                'age': faker.random.randint(18, 60),
                'phone': faker.phone_number(),
                'ssn': faker.ssn(),
                'job': faker.job(),
                'salary': faker.random_number(digits=4),
                'company': faker.company(),
                'address': faker.address(),
                'mark': faker.random_letter()
            }
            return item

        def into_mysql(dst, count):
            """数据进入MySQL"""
            items = [get_item() for _ in range(count)]
            line = self.add_many(dst, items)
            nonlocal n
            n += line
            logger.success('MySQL  插入{}  累计{}'.format(line, n))

        if not create_table():
            return

        if total < once:
            into_mysql(table, total)
            return

        for _ in range(total // once):
            into_mysql(table, once)

        if other := total % once:
            into_mysql(table, other)
