# 持续更新中...

# Python解释器

- python3.10+

# 使用poper

### 连接MySQL

```python
from poper import MysqlHandler

mysql_cfg = {
    'user': 'admin',
    'passwd': 'admin@1',
    'db': 'test'
}
handler = MysqlHandler(mysql_cfg)
```

### 生成测试表

```python
handler.gen_test_table("test_20240105")
```

### 执行SQL

```python
data = handler.exe_sql('select * from people limit 1', query_all=False)
print(data)
datas = handler.exe_sql('select * from people where name="Tony" and age=18', query_all=True)
print(datas)
```

### 具体演示

```python
from poper import MysqlHandler

mysql_cfg = {
    'user': 'admin',
    'passwd': 'admin@1',
    'db': 'test'
}
handler = MysqlHandler(mysql_cfg)

# 获取当前数据库的所有表名称
tables = handler.exe_sql('show tables', query_all=True, dict_cursor=False)
print(tables)

# 返回表的数量
count = handler.query_count('people')
print(count)

# 返回数量，这里指定了条件
count1 = handler.query_count('people', age=18)
count2 = handler.query_count('people', age=18, name='Thomas')
print(count1, count2)

# 获取随机数据
res1 = handler.random('people')
res2 = handler.random('people', limit=3)
print(res1, res2)


def show(datas):
    for some in enumerate(datas, start=1):
        print('第{}条  {}'.format(*some))


# 遍历数据，id范围为101~222，每轮扫描100条，每轮的回调函数为show
handler.scan('people', sort_field='id', start=101, end=222, once=100, dealer=show)

# 遍历整张表，默认每轮扫描1000条，默认只打印数据
# handler.scan('people')
```