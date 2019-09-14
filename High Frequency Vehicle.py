from jdbc.Connect import get_connection, free
import datetime
import pandas as pd
from jdbc.Start_End_time_list import Week_Period

def High_frequency_vehicles(conn, start_time):

    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    # 按日期查询一周内上下学期间车辆的出行次数，并返回出行次数大于等于5次的车牌号及其总体出行次数

    query_time_interval = []
    query_sql = ("SELECT HPHM, COUNT(JGSJ) FROM SJCJ_T_CLXX_LS WHERE SSID='HK-107' AND CDBH IN ('1','2','3','4') "
                 "AND TO_CHAR(JGSJ,'HH24') IN ('6','7','11','13','14','16','17')"
                 " AND JGSJ BETWEEN to_date('%s','yyyy-mm-dd') AND to_date('%s','yyyy-mm-dd') ")%()





if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻
    ## 开发测试用
    conn = None
    query_res = High_frequency_vehicles(conn)
    print(query_res)