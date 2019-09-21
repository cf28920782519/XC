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
    df_res_c1 = df_res.sort_values(by=['TOTAL_NUM'], ascending=False).reset_index()    # 将上面的结果按降序排列



    return df_res_c1

# # 输入一个车牌号码，输出它在这4个月中，每天06:45-07:20这个时间段内出行次数
def Query_Morning_Travel_NUM(conn, HPHM):
    # if am == 'am':
    #     JGSJ_x = ('06','07')
    # else: JGSJ_x = ('16','17','18')

    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    query_sql_1 = ("SELECT COUNT(a.JGSJ_x) FROM TRAVEL_TIME_HK93TOHK107 a "
                 "WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '06:45' AND '07:20'") % (HPHM)

    query_sql_2 = ("SELECT  COUNT(b.JGSJ_x) FROM TRAVEL_TIME_HK92TOHK107 b "
                 "WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '06:45' AND '07:20'") % (HPHM)
    cr.execute(query_sql_1)
    res_1 = cr.fetchall()
    cr.execute(query_sql_2)
    res_2 =cr.fetchall()
    res = res_1[0][0] + res_2[0][0]
    return res

# # 条件2：只保留早晨上学送娃的车牌号
# 输入：经过条件1筛选之后的输出结果；输出：早晨上学送娃时间段的车牌号
def Send_to_school_plates(df_res_c1):
    conn = None
    # df_res_c1['SEND_KIDS'] = df_res_c1.apply(Query_Morning_Travel_NUM(conn, df_res_c1['HPHM']), axis=1)
    HPHM = df_res_c1['HPHM'].tolist()   # 将HPHM列转为list
    WORK_NUM = df_res_c1['WORK_NUM'].tolist()
    TOTAL_NUM = df_res_c1['TOTAL_NUM'].tolist()
    SEND_KIDS = [Query_Morning_Travel_NUM(conn,hphm) for hphm in HPHM]  # 将HPHM列表中的hphm作为参数输入Query_Morning_Travel_NUM函数
    DATA = {"HPHM":HPHM, "WORK_NUM":WORK_NUM, "TOTAL_NUM":TOTAL_NUM, "SEND_KIDS":SEND_KIDS} # 将计算结果整理成字典
    df_res_c2 = pd.DataFrame(DATA,columns=['HPHM', 'WORK_NUM', 'TOTAL_NUM', 'SEND_KIDS'])   # 转为dataframe结构
    df_res_c2 = df_res_c2.drop(index=(df_res_c2.loc[df_res_c2['SEND_KIDS']<5].index))   # 送小孩次数小于5次的删去
    df_res_c2 = df_res_c2.sort_values(by=['SEND_KIDS'], ascending=False).reset_index()  # 按送小孩次数降序排列
    df_res_c2 = df_res_c2[['HPHM', 'SEND_KIDS']]    # 提取HPHM和SEND_KIDS列作为输出的dataframe
    # print(SEND_KIDS)
    return df_res_c2

# # 查询单辆车在下午放学期间最经常出现的旅行时间（6个子路段中，样本数最多的子路段的平均旅行时间）
def Query_Afternoon_Travel_Time(conn, HPHM):
    if conn == None: conn = get_connection()    # 建立数据库连接
    cr = conn.cursor()  # 生成连接的游标

    # 同时查6个表中的平均旅行时间和样本数（晚上放学期间）
    query_sql = ("SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK107TOHK92 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'"
                 "UNION SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK107TOHK93 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'"
                 "UNION SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK92TOHK107 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'"
                 "UNION SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK92TOHK93 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'"
                 "UNION SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK93TOHK92 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'"
                 "UNION SELECT AVG(TRAVEL_TIME),COUNT(TRAVEL_TIME) FROM TRAVEL_TIME_HK93TOHK107 WHERE HPHM='%s' AND TO_CHAR(JGSJ_x,'HH24:MI') BETWEEN '16:30' AND '18:00'")\
                % (HPHM, HPHM, HPHM, HPHM, HPHM, HPHM)

    cr.execute(query_sql)
    query_res = cr.fetchall()
    dataframe_res = pd.DataFrame(list(query_res), columns=['AVG_T', 'SAM_NUM']) # 查询结果转为dataframe格式
    dataframe_res = dataframe_res.sort_values(by=['SAM_NUM'], ascending=False).reset_index()    # 将查询结果按样本数量降序排列
    dataframe_res = dataframe_res[['AVG_T', 'SAM_NUM']] # 仅提取'AVG_T', 'SAM_NUM'列
    print(dataframe_res)
    # 统计对应车牌在6个子路段上的总样本数时，取消下面代码注释
    travel_time, sample_numbers = dataframe_res['AVG_T'][0], dataframe_res['SAM_NUM'].sum()    # 将dataframe中的第一行，赋值给travel_time和sample_numbers（样本数是指该车牌在6个子路段上的总共样本数）
    # 统计用于计算平均旅行时间的样本数，取消下面代码注释
    # travel_time, sample_numbers = dataframe_res['AVG_T'][0], dataframe_res['SAM_NUM'][0]    # 当需要统计：用于计算平均旅行时间的样本数时取消注释
    return travel_time, sample_numbers

# # 条件3：统计给定车牌列表（dataframe）的车辆，下午放学期间的路段旅行时间
def Pick_Up_Kids(df_res_c2):
    # df_res_c2 = pd.read_csv('D:/Result2.csv')
    HPHM = df_res_c2['HPHM'].tolist()   # 将输入的dataframe里面的HPHM列转为列表
    TRAVEL_TIME_LIST = []   # 保存平均旅行时间
    SAMPLE_LIST = []    # 保存样本个数（可修改Query_Afternoon_Travel_Time的倒数第二行，实现总体样本和用于计算平均旅行时间样本数的切换）
    # 统计下午放学期间，高频车的平均路段旅行时间，选取的是样本个数最多的子路段上的平均旅行时间
    for i in range(len(HPHM)):
        travel_time, sample_numbers = Query_Afternoon_Travel_Time(conn, HPHM[i])    # 统计结果赋值给travel_time, sample_numbers
        TRAVEL_TIME_LIST.append(travel_time)
        SAMPLE_LIST.append(sample_numbers)

    DATA = {"HPHM": HPHM, "TRAVEL_TIME": TRAVEL_TIME_LIST, "SAMPLE_NUM": SAMPLE_LIST}   # 将上面3个list转为字典
    df_res_c3 = pd.DataFrame(DATA,columns=['HPHM', 'TRAVEL_TIME', 'SAMPLE_NUM'])    # 转为dataframe
    df_res_c3 = df_res_c3.sort_values(by=['TRAVEL_TIME'], ascending=False).reset_index()    # 按平均旅行时间降序排列并重置索引
    df_res_c3 = df_res_c3[['HPHM', 'TRAVEL_TIME', 'SAMPLE_NUM']]    # 提取'HPHM', 'TRAVEL_TIME', 'SAMPLE_NUM'构成dataframe

    return df_res_c3





if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    df_res_c1 = Work_day_frequency_vehicles(conn)
    # print(df_res_c1)

    # df_res_c2 = Send_to_school_plates(df_res_c1)
    # print(df_res_c2)
    # df_res_c2.to_csv('D:/Result3.csv')
    # print(df_res)
    # print(Query_Morning_Travel_NUM(conn, '皖PWA816'))

    # travel_time, sample_numbers = Query_Afternoon_Travel_Time(conn, '皖PN0919')
    # print(travel_time, sample_numbers)
    # print(type(travel_time),type(sample_numbers))
    df_res_c3 = Pick_Up_Kids(df_res_c1)
    print(df_res_c3)
    df_res_c3.to_csv('D:/Result_all_vehicles.csv')

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)

