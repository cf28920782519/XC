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
    df_res = df_res_total.groupby('HPHM').sum() # 按HPHM分组合并，将其他列HPHM相同的值求和
    df_res = df_res.drop(index=(df_res.loc[1.0*(df_res['WORK_NUM']/df_res['TOTAL_NUM']) < 0.9].index))    # 保留工作日出行频数占总频数的90%以上的车牌
    df_res = df_res.sort_values(by=['TOTAL_NUM'], ascending=False).reset_index()    # 将上面的结果按降序排列



    return df_res

if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    df_res = Work_day_frequency_vehicles(conn)
    df_res.to_csv('D:/Result.csv')
    print(df_res)

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)

