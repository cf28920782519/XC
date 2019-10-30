from jdbc.Connect import get_connection
import datetime
import pandas as pd
import numpy as np

# 输入参数：表名，车辆类型编码，起止时刻（字符串eg: '2019-10-08 16:00:00'）
# 输出：查询结果，dataframe，3列：'HPZL_Y', 'JGSJ_X', 'TRAVEL_TIME'
def Travel_time_query(conn, table_name, car_type, start_time, end_time):
    if conn == None: conn = get_connection()
    cr = conn.cursor()

    query_sql = (
                    "SELECT HPZL_Y, JGSJ_X, TRAVEL_TIME FROM %s WHERE HPZL_Y='%s' AND "
                    "TO_CHAR(JGSJ_X,'YYYY-MM-DD HH24:MI:SS') BETWEEN '%s' AND '%s' ORDER BY JGSJ_X ") % (
                    table_name, car_type, start_time, end_time)
    cr.execute(query_sql)
    query_res = cr.fetchall()
    query_res = pd.DataFrame(data=query_res, columns=['HPZL_Y', 'JGSJ_X', 'TRAVEL_TIME'])

    return query_res

# 输入一个datetime格式的数，返回一个抹去秒值的整datetime（即datetime向下圆整）
def Round_datetime(date_time):
    tem = str(date_time)[:-2]+'00'
    tem = pd.to_datetime(tem, format='%Y-%m-%d %H:%M:%S')
    return tem  # 返回结果的格式为 YYYY-MM-DD HH24:MI:SS

# 输入旅行时间查询结果（dataframe，格式见Travel_time_query函数的输出）；另外2个参数均为int型，单位min
# 可通过修改loop_num里面的'm'调整时间的单位
def Slice_by_period_timedelta(query_res, peirod, step_length):
    # JGSJ_list = query_res['JGSJ_X'].tolist()
    # travel_time_list_total = query_res['TRAVEL_TIME'].tolist()
    query_res = query_res[['JGSJ_X', 'TRAVEL_TIME']]    # 重新组织query_res，只保留2列
    # 计算循环次数（查询结果时间列的最小值与最大值之差，转换为分钟数之后，再整除统计周期时长period）
    loop_num = ((query_res['JGSJ_X'].max()-query_res['JGSJ_X'].min()) / np.timedelta64(1, 'm')) // peirod
    # 根据滑动时间窗的步长，计算统计起始时刻，为datetime64型
    start_statistic_period = Round_datetime(query_res['JGSJ_X'].min()) + datetime.timedelta(minutes=step_length)
    start_period_list = []  # 所有的统计开始时刻列表
    end_period_list = []    # 所有的统计结束时刻列表
    for i in range(int(loop_num)):  # loop_num转为int型
        start_period_list.append(start_statistic_period + datetime.timedelta(minutes=i*peirod)) # 每次循环的开始时刻至列表
        end_period_list.append(start_statistic_period + datetime.timedelta(minutes=(i+1) * peirod))  # 每次循环的结束时刻

    # # 将整个的查询结果，按[开始时刻, 结束时刻)进行切片，并保留到列表query_res_slice_list中
    query_res_slice_list = []
    for i in range(len(start_period_list)):
        # tem计算的是第i次循环的[开始时刻, 结束时刻)期间的旅行时间列表
        tem = query_res[(query_res.JGSJ_X >= start_period_list[i])&(query_res.JGSJ_X <= end_period_list[i])]['TRAVEL_TIME'].tolist()
        query_res_slice_list.append(tem)    # 将tem添加到列表query_res_slice_list中

    return query_res_slice_list, end_period_list    # 返回切片后的列表集合以及各个统计时间段的结束时间列表

# 输入：一个列表
# 输出：列表中所有元素的筛选之后的平均数
# 可通过修改travel_critical_val = min(x) + 180  来改变筛选原则
def Travel_time_critical_val(x):
    if len(x) != 0:
        travel_critical_val = min(x) + 180
        x_new = [val for val in x if val <= travel_critical_val]
        average_travel_time = np.mean(x_new)
    else: average_travel_time = 0   # 当输入的列表为空时，平均旅行时间设置为0，代表没有观测值
    return average_travel_time  # 返回平均旅行时间


# 统计路段平均速度，输入：返回切片后的列表集合[[旅行时间观测值序列1],[旅行时间观测值序列1]...]；各个统计时间段的结束时间列表；路段长度（单位：米）
# 旅行时间单位是：秒
def Road_average_speed(query_res_slice_list, end_period_list, road_length):
    average_travel_time_list = [Travel_time_critical_val(ele) for ele in query_res_slice_list]  # 求各个时段的平均旅行时间
    road_speed_list = []    # 路段速度列表
    for i in range(len(average_travel_time_list)):
        if average_travel_time_list[i] != 0:
            road_speed_list.append(3.6 * road_length/average_travel_time_list[i])   # 米/秒 → km/h
        else: road_speed_list.append(0) # 统计时段内无观测值时，填入0
    end_period_series = pd.Series(data=end_period_list, dtype='datetime64[ns]') # 将统计时段列表转为Series，指定数据类型为：datetime64[ns]
    road_average_speed = pd.Series(data=road_speed_list, index=end_period_series)   # 路段平均速度Series
    return road_average_speed   # 返回结果



if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    # # 跑代码时，注释下面2行中的1行
    # query_res = Travel_time_query(None, 'TRAVEL_TIME_HK93TOHK107', '02','2019-10-29 16:00:00', '2019-10-29 18:00:00')
    query_res = Travel_time_query(None, 'TRAVEL_TIME_HK92TOHK107', '02', '2019-10-29 16:00:00', '2019-10-29 18:00:00')

    # 统计时间段为5分钟，滑动时间窗的步长为2，仅统计1次；可通过for循环迭代实现每2min统计一次
    query_res_slice_list, end_period_list = Slice_by_period_timedelta(query_res, 5, 0)

    # # 跑代码时，注释下面2行中的1行
    road_speed = Road_average_speed(query_res_slice_list, end_period_list, 440) # 指定路段长度为440米（HK-93到HK-107）
    # road_speed = Road_average_speed(query_res_slice_list, end_period_list, 228)  # 指定路段长度为228米（HK-92到HK-107）
    print(road_speed)

    # # 跑代码时，注释下面2行中的1行
    # road_speed.to_csv('average_speed_1028_HK92TOHK107.csv')
    road_speed.to_csv('average_speed_1028_HK93TOHK107.csv')
    # query_min_travel_time = query_res.groupby(by='HPZL_Y').resample('5T', base=2, on='JGSJ_X').TRAVEL_TIME.min()
    # print(list(query_min_travel_time['02'].values))
    # print(query_min_travel_time)
    # print(road_speed[road_speed <= 15])
    # print(road_speed.apply(lambda x: 2 if x <= 15 else 0).sum())    # 求拥堵时长

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)
