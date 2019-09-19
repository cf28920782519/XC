from jdbc.Connect import get_connection
import datetime
import pandas as pd
from jdbc.Start_End_time_list import Week_Period, Get_Holidays_during_Aweek, Add_serval_days, Start_Time_List

# # 将一周内的高频车找出并分别统计其工作日和休息日的出行次数
def High_frequency_vehicles(conn, start_time):

    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    # 按日期查询一周内上下学期间车辆的出行次数，并返回出行次数大于等于5次的车牌号及其总体出行次数

    query_time_interval = Week_Period(start_time)   # 根据给定的start_time，生成一个列表，包含该周的起始日期和终止日期
    # 查询车牌号码和经过时间记录数（仅上下学期间）
    query_sql = ("SELECT HPHM, COUNT(JGSJ) FROM SJCJ_T_CLXX_LS WHERE SSID='HK-107' AND CDBH IN ('1','2','3','4') "
                 "AND TO_CHAR(JGSJ,'HH24') IN ('06','07','16','17')"
                 " AND JGSJ BETWEEN to_date('%s','yyyy-mm-dd hh24:mi:ss') AND to_date('%s','yyyy-mm-dd hh24:mi:ss') GROUP BY HPHM ") % (query_time_interval[0], query_time_interval[1])

    cr.execute(query_sql)   # 执行查询
    query_res_total = cr.fetchall() # 查询结果从游标中提取并赋值给变量query_res_total
    dataframe_res_total = pd.DataFrame(list(query_res_total),columns=['HPHM', 'TOTAL_numbers'])  # 查询结果转化成pd的dataframe

    holiday_list = Get_Holidays_during_Aweek(start_time)    # 根据给定的start_time，生成该周的休息日 eg：['2019-05-011 00:00:00', '2019-05-12 00:00:00']

    holiday_query_list = [] # 将休息日的查询结果转化为dataframe格式，存入该列表
    for i in range(len(holiday_list)):
        query_sql_for_holiday = ("SELECT HPHM, COUNT(JGSJ) FROM SJCJ_T_CLXX_LS WHERE SSID='HK-107' AND CDBH IN ('1','2','3','4') "
                 "AND TO_CHAR(JGSJ,'HH24') IN ('06','07','16','17')"
                 " AND JGSJ BETWEEN to_date('%s','yyyy-mm-dd hh24:mi:ss') AND to_date('%s','yyyy-mm-dd hh24:mi:ss') GROUP BY HPHM ") % (holiday_list[i], Add_serval_days(holiday_list[i], 1)) # 查休息日当天的出行情况
        cr.execute(query_sql_for_holiday)   # 执行查询
        query_res_holiday = cr.fetchall()   # 查询结果提取
        To_dataframe = pd.DataFrame(list(query_res_holiday),columns=['HPHM', 'HOLIDAY_numbers'])   # 转为dataframe
        holiday_query_list.append(To_dataframe)   # 加入列表holiday_query_list
    # 将休息日的查询结果合成为一个dataframe
    df_holiday = pd.concat(holiday_query_list, ignore_index=True)  # 将所有查询结果合并（上下合并）
    df_holiday = df_holiday.groupby('HPHM').sum().reset_index() # 按HPHM列的内容进行表内合并，对其他列执行求和的操作

    # 将df_holiday和dataframe_res_total整合
    # df_holiday.rename(columns={'HOLIDAY_numbers': 'holiday_numbers'}, inplace=True)  # 重新命名指定列的列名
    df_holiday_total_tem = pd.concat([dataframe_res_total, df_holiday], ignore_index=True, sort=True)   # 上下合并2个表格，空白地方填NaN
    # print(df_holiday_total_tem)
    df_holiday_total = df_holiday_total_tem.groupby('HPHM').sum().reset_index()    # 按HPHM列的内容，进行表内整合，对其他列执行求和的操作
    df_holiday_total['WORK_numbers'] = df_holiday_total['TOTAL_numbers'].sub(df_holiday_total['HOLIDAY_numbers'])   # TOTAL_numbers列减去HOLIDAY_numbers列，赋值给新增的WORK_numbers列
    df_holiday_total['START_DATE'] = datetime.datetime.strptime(query_time_interval[0], '%Y-%m-%d %H:%M:%S')    # 新增一列用于表征计算起始的日期
    df_holiday_total['END_DATE'] = datetime.datetime.strptime(query_time_interval[1], '%Y-%m-%d %H:%M:%S')  # 新增一列用于表征统计截止的日期（不包括当天）

    # 通过上述流程，将同一周内出行的车牌及工作日、休息日出行次数统计完成
    # 下面将统计、筛选高频车（一周内出行5次以上，工作日出行3次以上的作为高频车）
    df_holiday_total = df_holiday_total.drop(index=(df_holiday_total.loc[df_holiday_total['TOTAL_numbers']<5].index))
    df_holiday_total = df_holiday_total.drop(
        index=(df_holiday_total.loc[df_holiday_total['WORK_numbers'] < 3].index)).reset_index() # 删去工作日出行次数小于3次的
    df_holiday_total = df_holiday_total.drop(
        index=(df_holiday_total.loc[df_holiday_total['HPHM'] == '车牌'].index)).reset_index() # 删去车牌识别失败的行
    df_holiday_total = df_holiday_total[['HPHM','TOTAL_numbers','WORK_numbers','HOLIDAY_numbers','START_DATE','END_DATE']] #提取对应列重构dataframe
    return df_holiday_total

# # 将一周内的高频车查询的dataframe转为list，方便写入oracle
def dataframe_Tolist(df_holiday_total):
    HPHM = df_holiday_total['HPHM'].tolist()
    TOTAL_NUM = df_holiday_total['TOTAL_numbers'].tolist()
    WORK_NUM = df_holiday_total['WORK_numbers'].tolist()
    HOLIDAY_NUM = df_holiday_total['HOLIDAY_numbers'].tolist()
    START_DATE = df_holiday_total['START_DATE'].tolist()
    END_DATE = df_holiday_total['END_DATE'].tolist()

    A = [HPHM, TOTAL_NUM, WORK_NUM, HOLIDAY_NUM, START_DATE, END_DATE]
    return list(zip(*A))

# # 写入数据库
def Insert_db(conn, table_name, result):
    if conn == None:
        conn = get_connection()
    cr = conn.cursor()
    # print('diaoyong')

    # sql = "INSERT INTO SYS.TRAVEL_TIME_HK93TOHK92(HPHM, HPZL_y, JGSJ_x, JGSJ_y, TRAVEL_TIME, DATE_TYPES) VALUES (:1, :2, :3, :4, :5, :6)"
    sql = ("INSERT INTO %s(HPHM, TOTAL_NUM, WORK_NUM, HOLIDAY_NUM, START_DATE, END_DATE) VALUES (:1, :2, :3, :4, :5, :6)") % (table_name)

    # # 如果插入数据库失败，可以取消下面3行注释，看报的错误是什么
    # cr.executemany(sql, result)
    # conn.commit()
    # print('insert successfully!')

    # # 批量运算时用下面的代码
    try:
        cr.executemany(sql, result)
        conn.commit()
        print('insert successfully!')
    except:
        conn.rollback()

    # 关闭游标、关闭数据库连接
    cr.close()
    conn.close()
    return 0







if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻
    conn = None
    # # 开发测试用
    # df_holiday_total = High_frequency_vehicles(conn,'2019-05-01 00:00:00')
    # print('整合表\r\n',df_holiday_total)
    # result = dataframe_Tolist(df_holiday_total)
    # print('result\r\n',result)
    # Insert_db(conn, 'HIGH_FRE_VEHICLES', result)
    # # print('总体出行\r\n', dataframe_res_total)

    # # 批量计算用
    start_time_list = Start_Time_List('2019-04-29 00:00:00', '2019-08-27 00:00:00')
    for i in range(len(start_time_list)):
        df_holiday_total = High_frequency_vehicles(conn, start_time_list[i])
        result = dataframe_Tolist(df_holiday_total)
        print('week: ', i)
        Insert_db(conn, 'HIGH_FRE_VEHICLES', result)


    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)