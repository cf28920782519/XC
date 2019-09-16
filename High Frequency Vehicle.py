from jdbc.Connect import get_connection, free
import datetime
import pandas as pd
from jdbc.Start_End_time_list import Week_Period, Get_Holidays_during_Aweek, Add_serval_days


def High_frequency_vehicles(conn, start_time):

    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    # 按日期查询一周内上下学期间车辆的出行次数，并返回出行次数大于等于5次的车牌号及其总体出行次数

    query_time_interval = Week_Period(start_time)   # 根据给定的start_time，生成一个列表，包含该周的起始日期和终止日期
    # 查询车牌号码和经过时间记录数（仅上下学期间）
    query_sql = ("SELECT HPHM, COUNT(JGSJ) FROM SJCJ_T_CLXX_LS WHERE SSID='HK-107' AND CDBH IN ('1','2','3','4') "
                 "AND TO_CHAR(JGSJ,'HH24') IN ('6','7','11','13','14','16','17')"
                 " AND JGSJ BETWEEN to_date('%s','yyyy-mm-dd hh24:mi:ss') AND to_date('%s','yyyy-mm-dd hh24:mi:ss') ") % (query_time_interval[0], query_time_interval[1])

    cr.execute(query_sql)   # 执行查询
    query_res_total = cr.fetchall() # 查询结果从游标中提取并赋值给变量query_res_total
    dataframe_res_total = pd.DataFrame(list(query_res_total),columns=['HPHM', 'JGSJ_numbers'])  # 查询结果转化成pd的dataframe

    holiday_list = Get_Holidays_during_Aweek(start_time)    # 根据给定的start_time，生成该周的休息日 eg：['2019-05-011 00:00:00', '2019-05-12 00:00:00']

    holiday_query_list = [] # 将休息日的查询结果转化为dataframe格式，存入该列表
    for i in range(len(holiday_list)):
        query_sql_for_holiday = ("SELECT HPHM, COUNT(JGSJ) FROM SJCJ_T_CLXX_LS WHERE SSID='HK-107' AND CDBH IN ('1','2','3','4') "
                 "AND TO_CHAR(JGSJ,'HH24') IN ('6','7','11','13','14','16','17')"
                 " AND JGSJ BETWEEN to_date('%s','yyyy-mm-dd hh24:mi:ss') AND to_date('%s','yyyy-mm-dd hh24:mi:ss') ") % (holiday_list[i], Add_serval_days(holiday_list[i], 1)) # 查休息日当天的出行情况
        cr.execute(query_sql_for_holiday)   # 执行查询
        query_res_holiday = cr.fetchall()   # 查询结果提取
        To_dataframe = pd.DataFrame(list(query_res_holiday),columns=['HPHM', 'JGSJ_numbers'])   # 转为dataframe
        holiday_query_list.append(To_dataframe)   # 加入列表holiday_query_list
    # 将休息日的查询结果合成为一个dataframe
    df_holiday = pd.concat(holiday_query_list, ignore_index=True)  # 将所有查询结果合并（上下合并）
    df_holiday = df_holiday.groupby('HPHM').sum().reset_index() # 按HPHM列的内容进行表内合并，对其他列执行求和的操作

    #









if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻
    ## 开发测试用
    conn = None
    query_res = High_frequency_vehicles(conn)
    print(query_res)