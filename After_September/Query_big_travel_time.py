from Connect import get_connection, free
import pandas as pd

def query_big_travel_time(conn, HPHM_during_school_period, date):
    if conn == None: conn = get_connection()
    cr = conn.cursor()  # 生成连接的游标

    HPHM_with_big_travel_time = []
    for hphm in HPHM_during_school_period:
        query_sql_1 = ("SELECT NVL(MAX(TRAVEL_TIME),0) from TRAVEL_TIME_HK93TOHK107 WHERE HPHM='%s' "
                       "AND JGSJ_x>= TO_DATE('%s','YYYY-MM-DD')") % (hphm, date)
        cr.execute(query_sql_1)
        query_res_1 = cr.fetchone()

        query_sql_2 = ("SELECT NVL(MAX(TRAVEL_TIME),0) from TRAVEL_TIME_HK92TOHK107 WHERE HPHM='%s' "
                       "AND JGSJ_x>= TO_DATE('%s','YYYY-MM-DD')") % (hphm, date)
        cr.execute(query_sql_2)
        query_res_2 = cr.fetchone()

        query_sql_3 = ("SELECT NVL(MAX(TRAVEL_TIME),0) from TRAVEL_TIME_HK93TOHK92 WHERE HPHM='%s' "
                       "AND JGSJ_x>= TO_DATE('%s','YYYY-MM-DD')") % (hphm, date)
        cr.execute(query_sql_3)
        query_res_3 = cr.fetchone()

        query_res = max(query_res_1[0], query_res_2[0], query_res_3[0])
        
        if query_res >= 300:
            HPHM_with_big_travel_time.append(hphm)
        else:
            continue
    return HPHM_with_big_travel_time

if __name__ == '__main__':
    conn = None
    HPHM_during_school_period = pd.read_csv('E:\\Python_Project\\Pandas_Exercise\\After_September\\HPHM_during_school_period.csv')
    HPHM_during_school_period = HPHM_during_school_period['0'].tolist()
    # print(HPHM_during_school_period)
    # HPHM_TEST = ['皖PM218C', '皖PM5131', '皖PM6115', '皖PM6678', '皖PM7826']
    HPHM_with_big_travel_time = query_big_travel_time(conn,HPHM_during_school_period,'2019-09-01')
    # print(HPHM_with_big_travel_time)

    result = pd.DataFrame(data=HPHM_with_big_travel_time)
    result.to_csv('E:\\HPHM_with_big_travel_time.csv')
