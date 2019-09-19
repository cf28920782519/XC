from jdbc.Connect import get_connection
import datetime
import pandas as pd

# # 条件1：工作日上下学期间频繁占用路段，周末很少或者几乎不用
def Work_day_frequency_vehicles(conn):

    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    # 查询 HIGH_FRE_VEHICLES表的HPHM、TOTAL_NUM、WORK_NUM列
    query_sql = "SELECT HPHM, TOTAL_NUM, WORK_NUM FROM HIGH_FRE_VEHICLES"

    cr.execute(query_sql)  # 执行查询
    query_res_total = cr.fetchall()  # 查询结果从游标中提取并赋值给变量query_res_total
    df_res_total = pd.DataFrame(list(query_res_total),columns=['HPHM', 'TOTAL_NUM', 'WORK_NUM'])    # 查询结果转化成pd的dataframe
    df_res = df_res_total.groupby('HPHM').sum().reset_index()


    return df_res

if __name__ == '__main__':
    conn = None
    print(Work_day_frequency_vehicles(conn))
