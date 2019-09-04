import cx_Oracle

# 连接数据库
def get_connection():
    conn = cx_Oracle.connect('scott/cf6024584@localhost/ORCL', mode=cx_Oracle.SYSDBA)  # 用户名/密码@服务器地址/数据库名称,指定连接模式为SYSDBA
    return conn
# 关闭连接
def free(conn, cursor):
    cursor.close()
    conn.close()