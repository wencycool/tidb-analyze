# 自动统计信息搜集补充策略
策略：
- 在mysql.analyze_jobs中一直未成功搜集的表（或分区）需重新搜集
- 健康度低于90的表(或者分区)需重新搜集
- 从来没搜集过统计信息的表(或者分区)需搜集
- 做过drop stats的表需要重新搜集
- 对于分区表，如果只是部分分区失败则只搜集失败的分区，否则搜集整个表
- 排除blob、clob、lob、text、midieum字段类型（这些字段不做统计信息搜集）
- 按照table_rows升序搜集
- 待统计信息表如果在最近慢日志中出现过，则优先搜集（优先级大于table_rows）
- 规定统计信息搜集时间窗口

扩展功能：
- 优先搜集慢查询中的表

版本要求
- tidb.version >= 6.5

---

### **查询语句**

**在mysql.analyze_jobs中未成功搜集的表（或分区）需重新搜集**
```sql
with table_need_analyze as (select table_schema, table_name,partition_name,start_time, fail_reason -- 找出最近7天统计信息搜集报错过，且报错后没有成功做过统计信息的表
                            from (select table_schema, -- 找出最近7天统计信息搜集报错过的表
                                         table_name,
                                         partition_name,
                                         start_time,
                                         fail_reason,
                                         row_number() over(partition by table_schema,table_name,partition_name order by start_time desc) as nbr
                                  from mysql.analyze_jobs
                                  where state = 'failed') a
                            where nbr = 1
                              and (table_schema, table_name, partition_name) not in (select a.table_schema,
                                                                            a.table_name, -- 对于报错的表，找出比报错时间更近的一次成功的统计信息搜集的表是否存在，如果不存在则需要做统计信息搜集，此处是找到比报错时间更近的一次成功的统计信息搜集的表
                                                                            a.partition_name
                                                                     from mysql.analyze_jobs a,
                                                                          (select table_schema, table_name, partition_name, start_time, fail_reason
                                                                           from (select table_schema, -- 找出最近7天统计信息搜集报错过的表
                                                                                        table_name,
                                                                                        partition_name,
                                                                                        start_time,
                                                                                        fail_reason,
                                                                                        row_number() over(partition by table_schema,table_name order by start_time desc) as nbr
                                                                                 from mysql.analyze_jobs
                                                                                 where state = 'failed') a
                                                                           where nbr = 1) b
                                                                     where a.table_schema = b.table_schema
                                                                       and a.table_name = b.table_name
                                                                       and a.start_time > b.start_time
                                                                       and a.state != 'failed'
group by a.table_schema, a.table_name, a.partition_name
    )
    )
select table_schema, table_name,partition_name,start_time, fail_reason from table_need_analyze;
```

**健康度低于90的表（或分区）需重新搜集**

```sql
show stats_healthy where healthy < 90;
```

**从来没搜集过统计信息的表(或者分区)需搜集**
```sql
select table_schema,table_name from INFORMATION_SCHEMA.tables where table_type = 'BASE TABLE' and (tidb_table_id,create_time) in (
    select table_id,tidb_parse_tso(version) from mysql.stats_meta where snapshot = 0
    )
```
> mysql.stats_meta中只包含表信息，无分区信息

**做过drop stats表需要重新搜集**
```sql
select table_schema,table_name from INFORMATION_SCHEMA.tables where table_schema not in ('mysql') and table_type = 'BASE TABLE' and (tidb_table_id) not in (
    -->show stats_meta<--
    )
```


**所有包含blob、clob、lob、text、midieum字段类型的表**

```sql
with table_with_blob as (select table_schema, table_name, table_rows
                         from information_schema.tables
                         where table_type = 'BASE TABLE'
                           and (table_schema, table_name) in (select table_schema, table_name
                                                              from information_schema.columns
                                                              where data_type in
                                                                    ('mediumtext', 'longtext', 'blob', 'text',
                                                                     'mediumblob', 'json', 'longblob')
                                                              group by table_schema, table_name))
select a.table_schema, a.table_name, a.table_rows, b.col_list
      from table_with_blob a,
           (select table_schema,
                   table_name,
                   group_concat(
                           case
                               when data_type not in
                                    ('mediumtext', 'longtext', 'blob', 'text', 'mediumblob', 'json', 'longblob')
                                   then column_name
                               end order by ordinal_position separator ',') as col_list
            from information_schema.columns
            where (table_schema, table_name) in (select table_schema, table_name from table_with_blob)
            group by table_schema, table_name) b
      where a.table_schema = b.table_schema
        and a.table_name = b.table_name;
```

**统计信息相关表的保留时期**

- stats_healthy: 当表消失后，自动删除
- mysql.stats_meta: 当表消失后，不会自动删除，通过version版本控制,tidb_parse_tso(version)获取时间戳，可通过tidb_table_id,version和INFORMATION_SCHEMA.tables的tidb_table_id,create_time关联
- INFORMATION_SCHEMA.tables 当表消失后，自动删除

