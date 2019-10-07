from jdbc.Connect import get_connection
import datetime
import pandas as pd
from jdbc.Convert_strTo_time_then_str import Convert_strTo_time_then_str




def travel_time_not_contain_pickups(conn, start_time, end_time, table_name):
    if conn == None: conn = get_connection()
    cr = conn.cursor()

    # # 查询给定表名（路段旅行时间表）在给定时间段内的路段旅行时间

    query_sql = ("SELECT * FROM %s WHERE TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN %s AND %s ORDER BY JGSJ_x")\
                % (table_name, start_time, end_time)
    cr.execute(query_sql)

    query_res_total = cr.fetchall()
    df_res_total = pd.DataFrame(list(query_res_total), columns=['HPHM', 'HPZL_y', 'JGSJ_x', 'JGSJ_y', 'TRAVEL_TIME', 'DATE_TYPES'])











