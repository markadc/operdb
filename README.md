# python解释器

python3.10+

# 使用sqoper

```python
from sqoper import MysqlHandler

mysql_cfg = {
    'user': 'admin',
    'passwd': 'admin@1',
    'db': 'test'
}

handler = MysqlHandler(mysql_cfg)

res = handler.exe_sql('show databases', query_all=True, dict_cursor=False)
print(res)

res = handler.exe_sql('show tables', query_all=True, dict_cursor=False)
print(res)

res = handler.random('people')
print(res)

res = handler.random('people', limit=3)
print(res)
```
