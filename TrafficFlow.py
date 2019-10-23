from jdbc.Connect import get_connection
import datetime
import pandas as pd
import numpy as np
from jdbc.Convert_strTo_time_then_str import Convert_strTo_time_then_str


# 输入：查询卡口的编号（'HK-107'）；查询时段的开始和结束时间（字符串形式，'2019-10-08 16:00:00'）；车道编号格式：[('1','2'...)]
def Query_ls(conn, SSID, start_time, end_time, cdbh):
    if conn == None: conn = get_connection()    # conn为None时，建立数据库连接
    cr = conn.cursor()  # 建立查询游标
    # 从卡口流水表中查询给定卡口编号、时段和车道组编号的JGSJ
    query_sql = (
                    "SELECT JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID = '%s' AND CDBH IN %s AND TO_CHAR(JGSJ,'YYYY-MM-DD HH24:MI:SS') BETWEEN '%s' AND '%s'") % (
                    SSID, cdbh[0], start_time, end_time)
    cr.execute(query_sql)   # 执行查询
    query_res = cr.fetchall()   # 提取查询结果，赋予变量query_res，查询结果形式：[(结果1),(结果2),(结果3)...]
    query_res = [i[0] for i in query_res]   # 重新组织查询结果，变成[结果1,结果2,...]
    series_res = pd.Series(data=query_res, dtype='datetime64[ns]')  # 将jgsj列转为Series格式，数据类型指定为datetime64
    result = pd.Series(np.ones(len(query_res)), index=series_res)   # 将查询到的JGSJ时间列设置为索引，Series的值为1（索引—1辆车）
    ls_query_result = result.sort_index(ascending=True) # 将构建好的Series按索引升序排序（输出结果时间升序）
    return ls_query_result  # 返回流水查询结果


# 求x和y的最小公倍数
def lcm(x, y):
    #  获取最大的数
    if x > y:
        greater = x
        smaller = y
    else:
        greater = y
        smaller = x
    if smaller == 0: return 0   # 当x和y中有一个数位0时，返回0
    else:

        while (True):
            if ((greater % x == 0) and (greater % y == 0)):
                lcm = greater
                break
            greater += 1

        return lcm  # 返回最小公倍数


# 输入一个datetime格式的数，返回一个抹去秒值的整datetime（即datetime向下圆整）
def Round_datetime(date_time):
    tem = str(date_time)[:-2]+'00'
    tem = pd.to_datetime(tem, format='%Y-%m-%d %H:%M:%S')
    return tem  # 返回结果的格式为 YYYY-MM-DD HH24:MI:SS


# 统计流量函数（滑动时间窗），输入为流水查询结果，可任意输入统计的周期和滑动时间窗口长度，返回流量统计结果
def Flow_statistical(ls_query_result, timedelta, step_length):
    if timedelta >= step_length:    # 判断统计时段是否大于等于滑动时间窗长度
        if step_length != 0:    # 滑动时间窗步长不为0时
            low_cm = lcm(timedelta, step_length)    # 滑动时间窗步长与统计周期的最小公倍数
            loop_num = int(low_cm / step_length)    # 流量统计的循环次数
        else: loop_num = 1  # 步长为0，只统计1次（即 不滑动）
        str_timedelta = str(timedelta) + 'T'    # 这里'T'表示抽样频率为分钟，其他时间频率可参看pd.resample()参数
        resample_list = []  # 重新抽样统计之后，结果存储列表
        for i in range(loop_num):
            sample_step_length = step_length * i   # 第i次循环的滑动时间窗步长
            # 参数base指定整个序列滑动时间窗的起点（base参数的单位与str_timedelta相同，是一个int型的数）
            # label指定了重新抽样之后，时间标签显示为区间右侧
            resample_list.append(ls_query_result.resample(str_timedelta, base=sample_step_length, label='right').sum())
        result_tem = pd.concat(resample_list)   # 将重新抽样统计后的结果合并（axis=0，竖直方向直接堆叠在一起）
        result_tem = result_tem.sort_index(ascending=True)  # 按索引升序排序（输出结果时间升序）

        # 下面要微调统计结果，使得统计区间与滑动时间窗统计相符（去掉样本数不够的统计值）
        # 索引时间戳显示的是统计时间段闭区间右侧的值，所以开始的时间戳应为——向下圆整：流水查询结果的时间索引中的最小值 + 统计周期长度
        start_period = Round_datetime(ls_query_result.index.min()) + datetime.timedelta(minutes=timedelta)
        # 索引时间戳显示的是统计时间段闭区间右侧的值，所以结束的时间戳应为——向上圆整：流水查询结果的时间索引中的最大值
        end_period = Round_datetime(ls_query_result.index.max()) + datetime.timedelta(minutes=1)
        result = result_tem[(result_tem.index >= start_period) & (result_tem.index <= end_period)]  # 按索引切片
        return result   # 返回流量统计结果
    else:
        print("Error: 时间窗步长大于统计周期") # 报错，不统计


if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    query_result = Query_ls(conn, 'HK-107', '2019-10-08 16:00:00', '2019-10-08 17:00:00',[('1','2')])
    # print(result)
    # query_result.to_csv('query_ls.csv')
    # print(result.resample('5T', label='right').sum())
    flow_result = Flow_statistical(query_result, 5, 2)  # 第二个参数表示：统计的周期（单位：min）；第三个参数是：滑动时间窗的长度
    print(flow_result)

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)
