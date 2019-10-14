import pandas as pd
from Connect import get_connection, free
from Get_travel_month import get_travel_month, whether_school_period_drive
import datetime


# import pysnooper


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

    HPHM_during_school_period = []
    for hphm in lis_res:
        conn = None
        month_list_tem = get_travel_month(conn, hphm)
        flag = whether_school_period_drive(month_list_tem)
        if flag == 1:
            HPHM_during_school_period.append(hphm)
        else:
            continue

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