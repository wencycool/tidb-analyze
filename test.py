#!/usr/bin/env python3
import logging

# 删除数据库中所有表的统计信息数据

import pymysql


# 从数据库中查询所有表名
def get_all_tables(conn: pymysql.connections.Connection):
    """
    This function retrieves all table names from the database.

    Parameters:
    conn (pymysql.connections.Connection): The database connection object.

    Returns:
    list: A list of tuples where each tuple contains the schema and name of a table.
    """
    # Create a cursor object using the connection
    cursor = conn.cursor()

    # Execute the SQL query to fetch all table names
    cursor.execute("select table_schema,table_name from information_schema.tables where table_schema in ('test','tpch');")

    # Fetch all the rows from the executed SQL query
    tables = cursor.fetchall()

    # Close the cursor object
    cursor.close()

    # Return the list of tables
    return tables


def drop_stats(table):
    return f"drop stats `{table[0]}`.`{table[1]}`"


if __name__ == "__main__":
    # 设置日志级别为INFO，并打印日志时间和日志信息和行号
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')
    conn = pymysql.connect(host='192.168.31.201', port=4000, user='root', password='', database='mysql')
    tables = get_all_tables(conn)
    logging.info(f"当前数据库中共有{len(tables)}张表")
    for table in tables:
        full_table_name=f"{table[0]}.{table[1]}"
        sql = drop_stats(table)
        # 执行SQL语句
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.close()
        # 每删除20张表打印一次进度
        if tables.index(table) % 20 == 0 and tables.index(table) != 0:
            logging.info(f"已删除{tables.index(table)}张表的统计信息")
    conn.close()
    logging.info("统计信息数据已删除")