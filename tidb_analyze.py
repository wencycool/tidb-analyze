import datetime
import time
import sys
import logging as log
import argparse
import re
import pymysql
import dbutils
from dbutils.pooled_db import PooledDB

# 用于缓存大对象查询结果，避免重复查询
tables_with_blob_dict_cache = None
tables_with_blob_dict_executed = False

# 用户缓存分区表查询结果，避免重复查询
partition_tables_cache = None
partition_tables_executed = False

# 缓存表的记录数
tables_rows_cache = {}
table_rows_executed = False


# todo 考虑当超时或者遇到ctrl+c后终止正在执行的统计信息搜集任务

# 获取统计信息搜集失败的对象（包括表和分区）
def get_analyze_failed_objects(conn: pymysql.connect):
    """
    This function retrieves the objects (including tables and partitions) for which the collection of statistical information has failed.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema, name, partition name, start time, and failure reason of an object for which the collection of statistical information has failed.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
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
    """
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, partition_name, start_time, fail_reason = row
            log.debug(
                f"上次统计信息搜集失败的对象: {table_schema}.{table_name}，分区名: {partition_name}，失败原因: {fail_reason}，上次统计信息搜集时间: {start_time}")
            # 将分区表的partition_name置为:global
            if partition_name == '':
                is_partition, succ, error = is_partition_table(conn, table_schema, table_name)
                if succ and is_partition:
                    partition_name = 'global'
            result.append((table_schema, table_name, partition_name, start_time, fail_reason))
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    log.info(f"统计信息搜集失败的对象数为: {len(result)}")
    return result, True, None


# 健康度低于90的表(或者分区)需重新搜集
def get_analyze_low_healthy_objects(conn: pymysql.connect, threshold: int = 90):
    """
    This function retrieves the tables (or partitions) that have a health score lower than the specified threshold.
    The health score is a measure of the quality of the statistics collected for a table or partition.

    Parameters:
    conn (pymysql.connect): The database connection object.
    threshold (int, optional): The health score threshold. Tables (or partitions) with a health score lower than this value will be retrieved. Defaults to 90.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema, name, partition name, and health score of a table (or partition) that has a health score lower than the threshold.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    if threshold < 0 or threshold > 100:
        threshold = 90
    sql_text = f"show stats_healthy where healthy < {threshold};"
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, partition_name, healthy = row
            log.debug(
                f"健康度低于{threshold}的表(或者分区): {table_schema}.{table_name}，分区名: {partition_name}，健康度: {healthy}")
            result.append((table_schema, table_name, partition_name, healthy))
    except Exception as e:
        log.error(f"execute sql:{sql_text},error:{e}")
        return None, False, e
    finally:
        cursor.close()
    log.info(f"健康度低于{threshold}的表(或者分区)数为: {len(result)}")
    return result, True, None


'''
mysql> show stats_meta where row_count=0;
+---------+------------+----------------+---------------------+--------------+-----------+
| Db_name | Table_name | Partition_name | Update_time         | Modify_count | Row_count |
+---------+------------+----------------+---------------------+--------------+-----------+
| tpch    | orders     |                | 2024-03-28 15:39:39 |            0 |         0 |
| tpch    | lineitem   |                | 2024-03-28 15:39:39 |            0 |         0 |
| tpch    | t1         |                | 2024-03-28 21:32:35 |            0 |         0 |
| tpch    | part       |                | 2024-03-28 21:42:47 |            0 |         0 |
+---------+------------+----------------+---------------------+--------------+-----------+
4 rows in set (0.00 sec)
'''


# 获取drop stats <tabname>的表需要重新搜集
def get_analyze_drop_stats_objects(conn: pymysql.connect):
    """
    This function retrieves the tables that have been dropped from the statistics.
    It does this by comparing the tables in the database with the tables in the statistics.
    If a table is in the database but not in the statistics, it means that the table has been dropped from the statistics.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema and name of a table that has been dropped from the statistics.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
    show stats_meta;
    """
    cursor = conn.cursor()
    result = []
    # 将stats_meta中的表存入字典
    stats_meta_dict = {}
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, partition_name, update_time, modify_count, row_count = row
            stats_meta_dict[(table_schema, table_name)] = True
    except Exception as e:
        log.error(f"execute sql:{sql_text},error:{e}")
        return None, False, e
    finally:
        cursor.close()
    # 获取所有表
    sql_text = """
    select table_schema,table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema not in ('mysql');
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name = row
            if (table_schema, table_name) not in stats_meta_dict:
                log.debug(f"无统计信息的表: {table_schema}.{table_name}")
                result.append((table_schema, table_name, ''))
    except Exception as e:
        log.error(f"execute sql:{sql_text},error:{e}")
        return None, False, e
    finally:
        cursor.close()
    log.info(f"无统计信息表数为: {len(result)}")
    return result, True, None


# 从来没搜集过统计信息的表(不包含分区)需搜集
def get_analyze_never_analyzed_objects(conn: pymysql.connect):
    """
    This function is used to get the tables that have never been analyzed.
    It only includes non-partitioned tables.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema and name of a table that has never been analyzed.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
    select table_schema,table_name from INFORMATION_SCHEMA.tables where table_type = 'BASE TABLE' and (tidb_table_id,create_time) in (
    select table_id,tidb_parse_tso(version) from mysql.stats_meta where snapshot = 0
    )
    """
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name = row
            result.append((table_schema, table_name))
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    return result, True, None
    sql_text = """
    select table_schema,table_name from INFORMATION_SCHEMA.tables where table_type = 'BASE TABLE' and (tidb_table_id,create_time) in (
    select table_id,tidb_parse_tso(version) from mysql.stats_meta where snapshot = 0
    )
    """
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name = row
            log.debug(f"从来没搜集过统计信息的表(不包含分区): {table_schema}.{table_name}")
            result.append((table_schema, table_name))
    except Exception as e:
        log.warning(f"执行sql失败: {sql_text},msg:{e}")
        return None, False, e
    finally:
        cursor.close()
    log.info(f"从来没搜集过统计信息的表(不包含分区)数为: {len(result)}")
    return result, True, None


# 查询出包含blob字段的表，并生成排除大字段的列
def get_tables_with_blob_dict(conn: pymysql.connect):
    """
    This function retrieves tables that contain blob fields and generates columns excluding large fields.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - dict: A dictionary where the key is a tuple (table_schema, table_name) and the value is a string of column names excluding large fields.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    """
    查询出包含blob字段的表，并生成排除大字段的列
    :param conn:
    :return: 返回结果（table_schema, table_name, col_list），是否成功，错误信息
    """
    global tables_with_blob_dict_cache
    global tables_with_blob_dict_executed

    # 如果已经执行过，直接返回缓存的结果
    if tables_with_blob_dict_executed:
        return tables_with_blob_dict_cache

    # 否则，执行函数并将结果存入缓存
    sql_text = f"""
    with table_with_blob as (select table_schema, table_name, table_rows
                             from information_schema.tables
                             where table_type = 'BASE TABLE'
                               and (table_schema, table_name) in (select table_schema, table_name
                                                                  from information_schema.columns
                                                                  where data_type in
                                                                        ('mediumtext', 'longtext', 'blob', 'text',
                                                                         'mediumblob', 'json', 'longblob')
                                                                  group by table_schema, table_name))

    select table_schema,
                       table_name,
                       group_concat(
                               case
                                   when data_type not in
                                        ('mediumtext', 'longtext', 'blob', 'text', 'mediumblob', 'json', 'longblob')
                                       then column_name
                                   end order by ordinal_position separator ',') as col_list
                from information_schema.columns
                where (table_schema, table_name) in (select table_schema, table_name from table_with_blob)
                group by table_schema, table_name
    """
    cursor = conn.cursor()
    cursor.execute("set group_concat_max_len=102400;")
    cursor.close()
    cursor = conn.cursor()
    result = {}
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, col_list = row
            result[(table_schema, table_name)] = col_list
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()

    # 将结果存入缓存，并将执行标志设为True
    tables_with_blob_dict_cache = result, True, None
    tables_with_blob_dict_executed = True

    return tables_with_blob_dict_cache


# 避免使用information_schema.partitions表，因为该表会随着分区表的分区数量增加而增加，导致查询速度变慢
# 如果该表未分区表，那么不做统计信息搜集，只做其分区的统计信息搜集，会自动做global merge stats
def is_partition_table(conn: pymysql.connect, table_schema: str, table_name: str):
    """
    This function checks if a given table in a specific schema is a partitioned table.

    Parameters:
    conn (pymysql.connect): The database connection object.
    table_schema (str): The name of the schema where the table is located.
    table_name (str): The name of the table to check.

    Returns:
    tuple: A tuple containing the following elements:
        - bool: A boolean value indicating whether the table is a partitioned table.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = f"""
    show create table `{table_schema}`.`{table_name}`
    """
    cursor = conn.cursor()
    result = None
    try:
        cursor.execute(sql_text)
        for row in cursor:
            result = row[1]
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    return "PARTITION BY" in result, True, None


# 获取数据库中所有分区表（非分区表的partion_name为空）
def get_all_partition_tables(conn: pymysql.connect):
    """
    This function retrieves all partitioned tables from the database.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - dict: A dictionary where the key is a tuple (table_schema, table_name) and the value is a boolean indicating whether the table is a partitioned table.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
    select table_schema,table_name,count(*) as cnt from information_schema.partitions group by table_schema, table_name;
    """
    global partition_tables_cache
    global partition_tables_executed
    if partition_tables_executed:
        return partition_tables_cache, True, None
    cursor = conn.cursor()
    result = {}
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, cnt = row
            if cnt > 1:
                result[(table_schema, table_name)] = True
            else:
                result[(table_schema, table_name)] = False
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    partition_tables_cache = result
    partition_tables_executed = True
    return partition_tables_cache, True, None


# 获取表的记录数
def get_all_tables_rows(conn: pymysql.connect):
    """
    This function retrieves the number of rows for all tables in the database.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - dict: A dictionary where the key is a tuple (table_schema, table_name) and the value is the number of rows in the table.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    global tables_rows_cache
    global table_rows_executed
    if table_rows_executed:
        return tables_rows_cache, True, None
    sql_text = f"""
    select table_schema,table_name,table_rows from information_schema.tables where table_type='BASE TABLE'
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, table_rows = row
            tables_rows_cache[(table_schema, table_name)] = table_rows
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    table_rows_executed = True
    return tables_rows_cache, True, None


# 获取需要做统计信息搜集的对象（包括表和分区）
# 如果是分区表，那么只做其分区的统计信息搜集，会自动做global merge stats
def collect_need_analyze_objects(conn: pymysql.connect):
    """
    This function collects the objects that need to be analyzed. It retrieves the objects (including tables and partitions)
    for which the collection of statistical information has failed, tables (or partitions) that have a health score lower
    than the specified threshold, tables that have been dropped from the statistics, and tables that have never been analyzed.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    list: A list of tuples. Each tuple contains the schema, name, partition name, and column list of an object that needs to be analyzed.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    object_dict = {}
    # 获取统计信息搜集失败的对象（包括表和分区）
    result, succ, msg = get_analyze_failed_objects(conn)
    if succ:
        for table_schema, table_name, partition_name, start_time, fail_reason in result:
            object_dict[(table_schema, table_name, partition_name)] = False
    # 获取健康度低于90的表(或者分区)需重新搜集
    result, succ, msg = get_analyze_low_healthy_objects(conn)
    if succ:
        for table_schema, table_name, partition_name, healthy in result:
            object_dict[(table_schema, table_name, partition_name)] = False
    # 获取drop stats <tabname>的表需要重新搜集
    result, succ, msg = get_analyze_drop_stats_objects(conn)
    if succ:
        for table_schema, table_name, partition_name in result:
            object_dict[(table_schema, table_name, partition_name)] = False
    # 获取从来没搜集过统计信息的表(不包含分区)需搜集
    result, succ, msg = get_analyze_never_analyzed_objects(conn)
    partition_tables_dict, succ1, msg1 = get_all_partition_tables(conn)
    if not succ1:
        raise Exception(f"获取分区表失败: {msg1}")
    if succ:
        for table_schema, table_name in result:
            # 如果是分区表则partition标记为global
            if (table_schema, table_name) in partition_tables_dict:
                if partition_tables_dict[(table_schema, table_name)]:
                    object_dict[(table_schema, table_name, 'global')] = False
                else:
                    object_dict[(table_schema, table_name, '')] = False
    # object_dict中的表为待做统计信息搜集的对象
    # 去掉分区表，只做分区的统计信息搜集
    # 采用list(object_dict.keys())的方式，避免在遍历时删除元素导致的异常
    keys = list(object_dict.keys())
    for table_schema, tablename, partition_name in keys:
        if partition_name == 'global':
            del object_dict[(table_schema, tablename, partition_name)]
    # 获取包含blob字段的表，并生成排除大字段的列
    # object_dict值为可以做统计信息的字段，如果是False说明表中没有blob字段
    tables_with_blob_dict, succ, msg = get_tables_with_blob_dict(conn)
    if succ:
        for table_schema, table_name, partition_name in object_dict:
            if (table_schema, table_name) in tables_with_blob_dict:
                object_dict[(table_schema, table_name, partition_name)] = tables_with_blob_dict[
                    (table_schema, table_name)]
    result = []  # 包含（table_schema, table_name, partition_name, col_list）的列表
    for table_schema, table_name, partition_name in object_dict:
        result.append(
            (table_schema, table_name, partition_name, object_dict[(table_schema, table_name, partition_name)]))
    return result


# 生成统计信息搜集语句
def gen_need_analyze_sqls(conn: pymysql.connect, slow_query_table_first=False, order=True):
    """
    This function generates SQL statements for the objects that need to be analyzed. It retrieves the objects (including tables and partitions)
    for which the collection of statistical information has failed, tables (or partitions) that have a health score lower
    than the specified threshold, tables that have been dropped from the statistics, and tables that have never been analyzed.
    It then generates SQL statements for these objects.

    Parameters:
    conn (pymysql.connect): The database connection object.
    slow_query_table_first (bool, optional): If set to True, the function will prioritize tables that appear in the slow query log. Defaults to False.
    order (bool, optional): If set to True, the function will order the objects by the number of rows in the table, prioritizing smaller tables. Defaults to True.

    Returns:
    list: A list of tuples. Each tuple contains the schema, name, partition name, column list, and the generated SQL statement for an object that needs to be analyzed.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    # 获取需要做统计信息搜集的对象
    need_analyze_objects = collect_need_analyze_objects(conn)
    # 如果存在(table_schema,table_name,'')则不单独执行分区统计信息搜集，否则统一执行分区统计信息搜集
    # 对need_analyze_objects按照table_schema,table_name,partition_name升序排列
    from operator import itemgetter
    need_analyze_objects.sort(key=itemgetter(0, 1, 2))
    # 生成统计信息搜集语句
    result = []
    last_table_name = None  # 记录上一个表名，后续分区不执行统计信息搜集
    first = True
    for table_schema, table_name, partition_name, col_list in need_analyze_objects:
        if partition_name == '':
            last_table_name = (table_schema, table_name)
            sql_text = f"analyze table `{table_schema}`.`{table_name}`"
        else:
            # 如果当前分区的表已经做过统计信息搜集，那么不需要重复做统计信息搜集
            if (table_schema, table_name) == last_table_name:
                continue
            # todo 如果表是分区表，那么只做其分区的统计信息搜集，会自动做global merge
            #  stats，但分区是串行执行，存在效率问题？如果表中所有分区都需要做统计信息搜集，那么是否可以直接做表的统计信息搜集？做成 analyze table xxx partition p0,p1,p2形式
            sql_text = f"analyze table `{table_schema}`.`{table_name}` partition `{partition_name}`"
        if col_list:
            sql_text = sql_text + f" columns {col_list}"
        result.append((table_schema, table_name, partition_name, col_list, sql_text))
    if order:
        # 按照表记录数大小排序，先做记录数小的表的统计信息搜集
        tables_rows_dict, succ, msg = get_all_tables_rows(conn)
        for i in range(len(result)):
            table_schema, table_name, partition_name, col_list, sql_text = result[i]
            table_rows = 0
            if (table_schema, table_name) not in tables_rows_dict:
                log.warning(f"表记录数不存在: {table_schema}.{table_name}")
            else:
                table_rows = tables_rows_dict[(table_schema, table_name)]
            result[i] = (table_schema, table_name, partition_name, table_rows, col_list, sql_text)
        # todo 添加slow_query相关的统计信息搜集优先级
        if succ:
            result.sort(key=lambda x: x[3])
    # 优先给慢日志表中的表做统计信息搜集
    if slow_query_table_first:
        table_in_slow_log = get_tablename_from_slow_log(conn)
        # 在table_in_slow_log中的表优放在result的最前面
        for table_name in table_in_slow_log:
            for i in range(len(result)):
                if table_name == result[i][1]:
                    result.insert(0, result.pop(i))
                    break
    return result, True, None

# 从慢日志表中获取SQL语句中的表名
def get_tablename_from_slow_log(conn: pymysql.connect):
    """
    This function retrieves table names from the slow query log in the database.

    The function executes a SQL query to fetch user, database, query time, and the query itself from the slow query log.
    It limits the results to 10000 and only fetches unique queries (based on their digest) that were executed by external users
    and have been logged in the last day.

    After fetching the data, it iterates over the rows and for each row, it extracts all table names from the query using
    the `get_all_tablename` function. The table names are then added to the result list.

    Finally, it filters the result list to only include tables that actually exist in the database. This is done by fetching
    all table names from the database using the `get_all_tables_from_database` function and checking if each table in the
    result list exists in the database.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema and name of a table that was referenced in a slow query.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
    select user,db,query_time,Query from (select user,db,query_time,Query,row_number() over (partition by digest) as nbr from INFORMATION_SCHEMA.slow_query where is_internal=0 and  `Time` > DATE_SUB(NOW(),INTERVAL 1 DAY) limit 10000)a where nbr = 1
    """
    cursor = conn.cursor()
    result = []  # 返回(db,table_name)
    try:
        cursor.execute(sql_text)
        for row in cursor:
            user, db, query_time, query = row
            tablist = get_all_tablename(query)
            # 对tablist去重
            tablist = list(set(tablist))
            for table_name in tablist:
                result.append(table_name)
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    # 对result去重
    result = list(set(result))
    # 从数据库中获取所有表名
    all_tables, success, error = get_all_tables_from_database(conn)
    if not success:
        return None, False, error
    # 将all_tables转换为字典
    all_tables_dict = {}
    # 如果表名重复，以最后一次为准
    for table_schema, table_name in all_tables:
        all_tables_dict[table_name] = table_schema
    # 对result进行过滤，只保留数据库中存在的表模式和表名
    result = [(all_tables_dict[table_name], table_name) for table_name in result if table_name in all_tables_dict]
    return result, True, None

def do_analyze(pool: dbutils.pooled_db.PooledDB, start_time="20:00", end_time="08:00", slow_query_table_first=False,
               order=True,
               preview=False, parallel=1):
    """
    执行统计信息搜集
    :param pool: 数据库连接池
    :param start_time: 统计信息搜集开始时间,格式为:23:03
    :param end_time: 统计信息搜集结束时间,格式为:23:03,如果end_time < start_time,那么表示跨天，比如start_time=23:03,end_time=01:03说明当前时间在这个时间段内可做统计信息搜集
    :param order: 是否按照表记录数大小排序，如果为True，那么会按照表记录数大小排序，先做记录数小的表的统计信息搜集
    :param preview: 是否预览，如果为True，那么只打印统计信息搜集语句，不执行
    :return: 执行中是否报错True or False; ####返回结果（table_schema, table_name, partition_name, col_list, sql_text, succ, msg）
    """
    conn = pool.connection()
    result, succ, msg = gen_need_analyze_sqls(conn, slow_query_table_first, order)
    if preview:
        log.info(f"当前脚本为预览模式，不会真正做统计信息搜集")
    log.info(f"需要做统计信息搜集的对象数为: {len(result)}")
    if not succ:
        return False
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=parallel) as exector:
        for table_schema, table_name, partition_name, table_rows, col_list, sql_text in result:
            if preview:
                log.info(f"预览: {sql_text}，搜集前表记录数: {table_schema}.{table_name} = {table_rows}")
            else:
                def to_exec(pool: dbutils.pooled_db.PooledDB, sql_text, table_schema, table_name, table_rows,
                            start_time, end_time):
                    if not in_time_range(start_time, end_time):
                        msg = f"当前时间:{datetime.datetime.now()}，不在指定时间范围内[{start_time}-{end_time}]，不执行统计信息搜集: {sql_text}，表记录数: {table_schema}.{table_name} = {table_rows}"
                        log.warning(msg)
                        return False
                    conn = pool.connection()
                    try:
                        t1 = time.time()
                        cursor = conn.cursor()
                        cursor.execute(sql_text)
                        t2 = time.time()
                        log.info(
                            f"执行: {sql_text}，搜集前表记录数: {table_schema}.{table_name} = {table_rows}，耗时: {round(t2 - t1, 2)}秒")
                        cursor.close()
                    except Exception as e:
                        log.error(f"执行:{sql_text},失败，msg:{e}")
                    conn.close()

                # todo 添加exector中队列深度，需要采用生产者消费者模式结合queue来保证未执行的SQL队列不过大
                exector.submit(to_exec, pool, sql_text, table_schema, table_name, table_rows, start_time, end_time)
    return True


def in_time_range(start_time, end_time):
    """
    This function checks if the current time falls within a specified time range.

    Parameters:
    start_time (str): The start of the time range in the format %H:%M.
    end_time (str): The end of the time range in the format %H:%M.

    Returns:
    bool: True if the current time is within the specified range, False otherwise.

    Note:
    If the start_time is greater than the end_time, it means the time range spans across two days.
    For example, start_time=23:00, end_time=01:00 means the time range is from 23:00 of the current day to 01:00 of the next day.
    """
    # 如果未设置开始时间，或开始时间等于结束时间，则返回True
    if not start_time or start_time == end_time:
        return True
    start_time = datetime.datetime.strptime(start_time, "%H:%M")
    start_hour = start_time.hour + 1 / 60 * start_time.minute
    end_time = datetime.datetime.strptime(end_time, "%H:%M")
    end_hour = end_time.hour + 1 / 60 * end_time.minute
    now_time = datetime.datetime.now()
    now_hour = now_time.hour + 1 / 60 * now_time.minute
    if start_hour < end_hour:
        if start_hour <= now_hour <= end_hour:
            return True
        else:
            return False
    else:
        if start_hour <= now_hour or now_hour <= end_hour:
            return True
        else:
            return False


# todo 优化正则表达式，支持获取模式名
def get_all_tablename(sql_text):
    """
    This function extracts all table names from a given SQL query.

    Parameters:
    sql_text (str): The SQL query from which to extract table names.

    Returns:
    list: A list of table names extracted from the SQL query.

    Note:
    The function uses regular expressions to find the table names. It looks for patterns that match SQL syntax for referencing tables.
    """
    tablist = []
    # pattern_text='from\s+?("?(?P<first>\w+?)\s*?"?\.)?"?(?P<last>\w+) *"?'
    pattern_text = '(from|delete\s+from|update)\s+("?\w+"?\.)?"?(?P<last>\w+)"?'
    while len(sql_text) > 0:
        pattern_tab = re.search(pattern_text, sql_text, re.I)
        if pattern_tab is not None:
            tablist.append(pattern_tab.group("last"))
            sql_text = sql_text[pattern_tab.end():]
        else:
            return tablist
    return tablist


# 从慢日志表中获取SQL语句中的表名
def get_all_tables_from_database(conn: pymysql.connect):
    """
    This function retrieves all table names from the database.

    Parameters:
    conn (pymysql.connect): The database connection object.

    Returns:
    tuple: A tuple containing the following elements:
        - list: A list of tuples. Each tuple contains the schema and name of a table in the database.
        - bool: A boolean value indicating whether the operation was successful.
        - None/Exception: If an error occurred during the operation, it returns the exception; otherwise, it returns None.

    Raises:
    Exception: An exception is raised if there is an error executing the SQL query.
    """
    sql_text = """
    select user,db,query_time,Query from (select user,db,query_time,Query,row_number() over (partition by digest) as nbr from INFORMATION_SCHEMA.slow_query where is_internal=0 and  `Time` > DATE_SUB(NOW(),INTERVAL 1 DAY) limit 10000)a where nbr = 1
    """
    cursor = conn.cursor()
    result = []  # 返回(db,table_name)
    try:
        cursor.execute(sql_text)
        for row in cursor:
            user, db, query_time, query = row
            tablist = get_all_tablename(query)
            # 对tablist去重
            tablist = list(set(tablist))
            for table_name in tablist:
                result.append(table_name)
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    # 对result去重
    result = list(set(result))
    # 从数据库中获取所有表名
    all_tables, success, error = get_all_tables_from_database(conn)
    if not success:
        return None, False, error
    # 将all_tables转换为字典
    all_tables_dict = {}
    # 如果表名重复，以最后一次为准
    for table_schema, table_name in all_tables:
        all_tables_dict[table_name] = table_schema
    # 对result进行过滤，只保留数据库中存在的表模式和表名
    result = [(all_tables_dict[table_name], table_name) for table_name in result if table_name in all_tables_dict]
    return result, True, None


def with_timeout(timeout, func, *args, **kwargs):
    """
    This function executes a given function with a specified timeout. If the function execution exceeds the timeout,
    an exception is raised. This function is specifically designed for Linux systems.

    Parameters:
    timeout (int): The maximum time (in seconds) for the function to run.
    func (function): The function to be executed.
    *args: Variable length argument list for the function to be executed.
    **kwargs: Arbitrary keyword arguments for the function to be executed.

    Returns:
    Any: The return value of the function that was executed.

    Raises:
    Exception: An exception is raised if the function execution exceeds the timeout.
    """
    # 判断当前系统是否为linux
    if not sys.platform == 'linux':
        return func(*args, **kwargs)
    import resource
    # 为避免对象过多，限制真实物理内存为5GB，如果超过5GB，会抛出MemoryError
    try:
        resource.setrlimit(resource.RLIMIT_RSS, (5368709120, 5368709120))
    except Exception as e:
        log.warning(f"setrlimit failed, error: {e}")
        exit(1)
    import signal
    def timeout_handler(signum, frame):
        raise Exception("timeout")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    try:
        func(*args, **kwargs)
    except Exception as e:
        log.warning(f"analyze failed, error: {e}")
    finally:
        signal.alarm(0)


def timeout_handler(signum, frame):
    """
    This function is a signal handler that raises an exception when a timeout signal is received.

    Parameters:
    signum (int): The signal number that was received.
    frame (frame object): The current stack frame.

    Raises:
    Exception: An exception is raised when a timeout signal is received.
    """
    raise Exception("timeout")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='analyze tidb tables',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', help='database host', default='127.0.0.1')
    parser.add_argument('-P', '--port', help='database port', default=4000, type=int)
    parser.add_argument('-u', '--user', help='database user', default='root')
    parser.add_argument('-p', '--password', help='database password', nargs='?')
    parser.add_argument('-d', '--database', help='database name', default='information_schema')
    parser.add_argument('--preview', help='开启预览模式，不搜集统计信息搜集', action='store_true')
    parser.add_argument('--slow-log-first', help="当表在slow_query中优先做统计信息搜集", action='store_true')
    parser.add_argument('--start-time', help="统计信息允许的开始时间窗口,生产环境可设置为20:00", required=False)
    parser.add_argument('--end-time',
                        help="统计信息允许的结束时间窗口,生产环境推荐设置为06:00,表示次日06点后不会执行统计信息搜集语句，但不会杀掉正在执行的最后一个统计信息语句",
                        required=False)
    parser.add_argument('--parallel', help="统计信息搜集并发数，最多可并发10个", type=int, default=1)
    parser.add_argument('-t', '--timeout', help="整个统计信息搜集最大时间，超过该时间则超时退出,单位为秒",
                        default=12 * 3600, type=int)
    args = parser.parse_args()
    parallel = 10 if args.parallel > 10 else args.parallel
    log.basicConfig(level=log.INFO,
                    format='%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    if args.password is None:
        args.password = input("password:")
    try:
        # 创建数据库连接池
        pool = PooledDB(creator=pymysql, maxconnections=parallel + 1, blocking=True, host=args.host, port=args.port,
                        user=args.user, password=args.password, database=args.database)
        slow_query_table_first = False
        preview = False
        if args.slow_log_first:
            slow_query_table_first = True
        if args.preview:
            preview = True
        with_timeout(args.timeout, do_analyze, pool, start_time=args.start_time, end_time=args.end_time,
                     slow_query_table_first=slow_query_table_first, order=True, preview=preview, parallel=parallel)
        pool.close()
    except Exception as e:
        log.error(f"connect to database failed, error: {e}")
