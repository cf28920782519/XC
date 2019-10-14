import pandas as pd
from Connect import get_connection, free
from Get_travel_month import get_travel_month, whether_school_period_drive
import datetime


# import pysnooper

# 查询date之后的高频出行车辆的车牌号
# @pysnooper.snoop(va='res')
def get_high_fre_vehicles(conn, date):
    if conn == None:
        conn = get_connection()
    cr = conn.cursor()  # 生成连接的游标

    query_sql = ("SELECT HPHM FROM HIGH_FRE_VEHICLES WHERE START_DATE>= TO_DATE('%s','YYYY-MM-DD')") % (date)
    cr.execute(query_sql)
    res = cr.fetchall()
    free(conn, cr)
    lis_res = []
    for ele in res:
        lis_res.append(ele[0])

    HPHM_during_school_period = []  # 上下学期间的高频车列表
    for hphm in lis_res:    # 高频车列表中的全部车辆
        conn = None
        month_list_tem = get_travel_month(conn, hphm)   # 获取给定车牌的全部出行月份列表，具体代码见Get_travel_month.py
        flag = whether_school_period_drive(month_list_tem)  # 判断是否为出行月份含9月但不含7、8月的车牌列表，具体代码见Get_travel_month.py
        if flag == 1:   # 标记为1
            HPHM_during_school_period.append(hphm)  # 添加到 上下学期间的高频车列表
        else:
            continue    # 继续循环，不操作

    return HPHM_during_school_period



if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻
    conn = None
    HPHM_during_school_period = get_high_fre_vehicles(conn, '2019-09-01')
    # HPHM_with_big_travel_time = query_big_travel_time(conn,HPHM_during_school_period,'2019-09-01')
    print(HPHM_during_school_period)
    print(len(HPHM_during_school_period))

    result = pd.DataFrame(data=HPHM_during_school_period)
    result.to_csv('E:\\HPHM_during_school_period.csv')

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)