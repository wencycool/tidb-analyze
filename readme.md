```shell
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -h
usage: tidb_analyze.py [-h] [-H HOST] [-P PORT] [-u USER] [-p [PASSWORD]] [-d DATABASE] [-t TIMEOUT] [--preview]

analyze slow log

options:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  database host (default: 127.0.0.1)
  -P PORT, --port PORT  database port (default: 4000)
  -u USER, --user USER  database user (default: root)
  -p [PASSWORD], --password [PASSWORD]
                        database password (default: None)
  -d DATABASE, --database DATABASE
                        database name (default: information_schema)
  -t TIMEOUT, --timeout TIMEOUT
                        timeout (default: 43200)
  --preview             开启预览模式，不搜集统计信息搜集 (default: False)
```

```shell
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -H 192.168.31.201 -P 4000 -u root -p 
password:
2023-12-30 21:44:15,056 - INFO - 统计信息搜集失败的对象数为: 0
2023-12-30 21:44:16,328 - INFO - 从来没搜集过统计信息的表(不包含分区)数为: 0
2023-12-30 21:44:16,587 - INFO - 需要做统计信息搜集的对象数为: 0

```