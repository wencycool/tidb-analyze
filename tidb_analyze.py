import datetime
import time
import sys
import pymysql
import logging as log
import argparse
import re
# 用于缓存大对象查询结果，避免重复查询
tables_with_blob_dict_cache = None
tables_with_blob_dict_executed = False

# 用户缓存分区表查询结果，避免重复查询
partition_tables_cache = None
partition_tables_executed = False

# 缓存表的记录数
tables_rows_cache = {}
table_rows_executed = False

# 获取统计信息搜集失败的对象（包括表和分区）
def get_analyze_failed_objects(conn: pymysql.connect):
    """
    获取统计信息搜集失败的对象（包括表和分区）
    :param conn: 数据库连接
    :return: 返回结果（table_schema, table_name,partition_name,start_time, fail_reason），是否成功，错误信息
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
            table_schema, table_name,partition_name,start_time, fail_reason = row
            log.debug(
                f"上次统计信息搜集失败的对象: {table_schema}.{table_name}，分区名: {partition_name}，失败原因: {fail_reason}，上次统计信息搜集时间: {start_time}")
            # 将分区表的partition_name置为:global
            if partition_name == '':
                is_partition, succ, error = is_partition_table(conn, table_schema, table_name)
                if succ and is_partition:
                    partition_name = 'global'
            result.append((table_schema, table_name,partition_name,start_time, fail_reason))
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    log.info(f"统计信息搜集失败的对象数为: {len(result)}")
    return result, True, None


# 健康度低于90的表(或者分区)需重新搜集
def get_analyze_low_healthy_objects(conn: pymysql.connect, threshold: int = 90):
    if threshold < 0 or threshold > 100:
        threshold = 90
    sql_text = f"show stats_healthy where healthy < {threshold};"
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            table_schema, table_name, partition_name, healthy = row
            log.debug(f"健康度低于{threshold}的表(或者分区): {table_schema}.{table_name}，分区名: {partition_name}，健康度: {healthy}")
            result.append((table_schema, table_name, partition_name, healthy))
    except Exception as e:
        log.error(f"execute sql:{sql_text},error:{e}")
        return None, False, e
    finally:
        cursor.close()
    log.info(f"健康度低于{threshold}的表(或者分区)数为: {len(result)}")
    return result, True, None

# 从来没搜集过统计信息的表(不包含分区)需搜集
def get_analyze_never_analyzed_objects(conn: pymysql.connect):
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

    select a.table_schema, a.table_name, b.col_list
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
    """
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
    判断是否是分区表
    :param conn:
    :param table_schema:
    :param table_name:
    :return: 返回是否是分区表，是否成功，错误信息
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
    获取所有分区表
    :param conn:
    :return: 返回结果dict[(table_schema,table_name)] = [是否分区表]，是否成功，错误信息
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
    获取表的记录数
    :param conn:
    :param table_schema:
    :param table_name:
    :return: 返回表的记录数，是否成功，错误信息
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
    object_dict = {}
    # 获取统计信息搜集失败的对象（包括表和分区）
    result,succ,msg = get_analyze_failed_objects(conn)
    if succ:
        for table_schema, table_name,partition_name,start_time, fail_reason in result:
            object_dict[(table_schema, table_name,partition_name)] = False
    # 获取健康度低于90的表(或者分区)需重新搜集
    result, succ, msg = get_analyze_low_healthy_objects(conn)
    if succ:
        for table_schema, table_name, partition_name, healthy in result:
            object_dict[(table_schema, table_name, partition_name)] = False
    # 获取从来没搜集过统计信息的表(不包含分区)需搜集
    result, succ, msg = get_analyze_never_analyzed_objects(conn)
    partition_tables_dict,succ1,msg1 = get_all_partition_tables(conn)
    if not succ1:
        raise Exception(f"获取分区表失败: {msg1}")
    if succ:
        for table_schema, table_name in result:
            # 如果是分区表则partition标记为global
            if (table_schema,table_name) in partition_tables_dict:
                if partition_tables_dict[(table_schema,table_name)]:
                    object_dict[(table_schema, table_name, 'global')] = False
                else:
                    object_dict[(table_schema, table_name, '')] = False
    # object_dict中的表为待做统计信息搜集的对象
    # 去掉分区表，只做分区的统计信息搜集
    for table_schema,tablename,partition_name in object_dict:
        if partition_name == 'global':
            del object_dict[(table_schema,tablename,partition_name)]
    # 获取包含blob字段的表，并生成排除大字段的列
    # object_dict值为可以做统计信息的字段，如果是False说明表中没有blob字段
    tables_with_blob_dict, succ, msg = get_tables_with_blob_dict(conn)
    if succ:
        for table_schema,table_name,partition_name in object_dict:
            if (table_schema,table_name) in tables_with_blob_dict:
                object_dict[(table_schema,table_name,partition_name)] = tables_with_blob_dict[(table_schema,table_name)]
    result = []  # 包含（table_schema, table_name, partition_name, col_list）的列表
    for table_schema,table_name,partition_name in object_dict:
        result.append((table_schema,table_name,partition_name,object_dict[(table_schema,table_name,partition_name)]))
    return result

# 生成统计信息搜集语句
def gen_need_analyze_sqls(conn: pymysql.connect, slow_query_table_first=False, order=True):
    """
    生成统计信息搜集语句
    :param conn:
    :param slow_query_table_first: 是否优先做慢日志表中的表的统计信息搜集
    :param order: 是否按照表记录数大小排序，如果为True，那么会按照表记录数大小排序，先做记录数小的表的统计信息搜集
    :return: 返回结果（table_schema, table_name, partition_name, table_rows, col_list, sql_text），是否成功，错误信息
    """
    # 获取需要做统计信息搜集的对象
    need_analyze_objects = collect_need_analyze_objects(conn)
    # 生成统计信息搜集语句
    result = []
    for table_schema, table_name, partition_name, col_list in need_analyze_objects:
        if partition_name == '':
            sql_text = f"analyze table `{table_schema}`.`{table_name}`"
        else:
            sql_text = f"analyze table `{table_schema}`.`{table_name}` partition(`{partition_name}`)"
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
            result[i] = (table_schema, table_name, partition_name,table_rows, col_list, sql_text)
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
                    result.insert(0,result.pop(i))
                    break
    return result, True, None


def do_analyze(conn: pymysql.connect, start_time="20:00", end_time="08:00", slow_query_table_first=False,order=True, preview=False):
    """
    执行统计信息搜集
    :param conn:
    :param start_time: 统计信息搜集开始时间,格式为:23:03
    :param end_time: 统计信息搜集结束时间,格式为:23:03,如果end_time < start_time,那么表示跨天，比如start_time=23:03,end_time=01:03说明当前时间在这个时间段内可做统计信息搜集
    :param order: 是否按照表记录数大小排序，如果为True，那么会按照表记录数大小排序，先做记录数小的表的统计信息搜集
    :param preview: 是否预览，如果为True，那么只打印统计信息搜集语句，不执行
    :return: 返回结果（table_schema, table_name, partition_name, col_list, sql_text, succ, msg）
    """
    result, succ, msg = gen_need_analyze_sqls(conn,slow_query_table_first, order)
    log.info(f"需要做统计信息搜集的对象数为: {len(result)}")
    if not succ:
        return None, False, msg
    cursor = conn.cursor()
    try:
        for table_schema, table_name, partition_name, table_rows, col_list, sql_text in result:
            if preview:
                log.info(f"预览: {sql_text}，搜集前表记录数: {table_schema}.{table_name} = {table_rows}")
            else:
                if not in_time_range(start_time, end_time):
                    msg = f"当前时间:{datetime.datetime.now()}，不在指定时间范围内[{start_time}-{end_time}]，不执行统计信息搜集: {sql_text}，表记录数: {table_schema}.{table_name} = {table_rows}，后面表均不执行"
                    log.warning(msg)
                    return None, msg
                t1 = time.time()
                cursor.execute(sql_text)
                t2 = time.time()
                log.info(f"执行: {sql_text}，搜集前表记录数: {table_schema}.{table_name} = {table_rows}，耗时: {round(t2 - t1,2)}秒")
                conn.commit()
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    return result, True, None

def in_time_range(start_time, end_time):
    """
    判断当前时间是否在指定时间范围内，比如判断当前时间是否在 8:00-20:00 之间，对于start_time23:00，end_time 7:00的情况，end_time需要设置为第二天的时间，比如 7:00
    :param start_time: 开始时间，格式为 %H:%M
    :param end_time: 结束时间，格式为 %H:%M
    :return: True or False
    """
    start_time = datetime.datetime.strptime(start_time, "%H:%M")
    start_hour = start_time.hour + 1/60 * start_time.minute
    end_time = datetime.datetime.strptime(end_time, "%H:%M")
    end_hour = end_time.hour + 1/60 * end_time.minute
    now_time = datetime.datetime.now()
    now_hour = now_time.hour + 1/60 * now_time.minute
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

def get_all_tables_from_database(conn: pymysql.connect):
    """
    获取数据库中所有表名
    :param conn:
    :return: 返回结果（table_schema, table_name），是否成功，错误信息
    """
    sql_text = """
    select  table_schema, table_name
    from information_schema.tables where table_type = 'BASE TABLE' and lower(table_schema) not in ('mysql','information_schema','performance_schema','sys')
    """
    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql_text)
        for row in cursor:
            result.append((row[0], row[1]))
    except Exception as e:
        return None, False, e
    finally:
        cursor.close()
    return result, True, None

# 从慢日志表中获取SQL语句中的表名
def get_tablename_from_slow_log(conn:pymysql.connect):
    """
    从慢日志表中获取SQL语句中的表名
    :param conn:
    :return: 返回结果（table_name），是否成功，错误信息
    """
    sql_text = """
    select user,db,query_time,Query from (select user,db,query_time,Query,row_number() over (partition by digest) as nbr from INFORMATION_SCHEMA.slow_query where is_internal=0 and  `Time` > DATE_SUB(NOW(),INTERVAL 1 DAY) limit 10000)a where nbr = 1
    """
    cursor = conn.cursor()
    result = []  # 返回(db,table_name)
    try:
        cursor.execute(sql_text)
        for row in cursor:
            user,db,query_time,query = row
            tablist = get_all_tablename(query)
            #对tablist去重
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
    raise Exception("timeout")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='analyze tidb tables',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', help='database host', default='127.0.0.1')
    parser.add_argument('-P', '--port', help='database port', default=4000,type=int)
    parser.add_argument('-u', '--user', help='database user', default='root')
    parser.add_argument('-p', '--password', help='database password', nargs='?')
    parser.add_argument('-d', '--database', help='database name', default='information_schema')
    parser.add_argument('--preview', help='开启预览模式，不搜集统计信息搜集', action='store_true')
    parser.add_argument('--slow-log-first',help="当表在slow_query中优先做统计信息搜集",action='store_true')
    parser.add_argument('--start-time',help="统计信息允许的开始时间窗口",default="20:00")
    parser.add_argument('--end-time',help="统计信息允许的结束时间窗口",default="06:00")
    parser.add_argument('-t','--timeout',help="整个统计信息搜集最大时间，超过该时间则超时退出,单位为秒",default=12 * 3600,type=int)
    args = parser.parse_args()
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if args.password is None:
        args.password = input("password:")
    try:
        conn = pymysql.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            charset='utf8mb4'
        )
        slow_query_table_first = False
        preview = False
        if args.slow_log_first:
            slow_query_table_first = True
        if args.preview:
            preview = True
        with_timeout(args.timeout, do_analyze,conn,start_time=args.start_time,end_time=args.end_time,slow_query_table_first=slow_query_table_first,order=True,preview=preview)
        conn.close()
    except Exception as e:
        log.error(f"connect to database failed, error: {e}")