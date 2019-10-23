import datetime
import pandas as pd
import math
import numpy as np

def Convert_strTo_time_then_str(start_time,timedelta=0):
    tem = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    tem = tem + datetime.timedelta(minutes=timedelta)
    tem = datetime.datetime.strftime(tem, '%Y-%m-%d %H:%M:%S')
    return tem

def Start_End_time_list(start_time,date_length): # 输入start_time格式为'2019-05-02 16:00:00'；date_length为整型，表示时间跨度
    start_time_list = []
    end_time_list = []
    for i in range(date_length):
        st1 = Convert_strTo_time_then_str(start_time, i*1440)
        st2 = Convert_strTo_time_then_str(start_time, -10+i*1440)
        lis1 = [st1, st2]
        start_time_list.append(lis1)
        ed1 = Convert_strTo_time_then_str(start_time, 120+i*1440)
        ed2 = Convert_strTo_time_then_str(start_time, 130+i*1440)
        lis2 = [ed1, ed2]
        end_time_list.append(lis2)
    return start_time_list, end_time_list

def Data_Combine():
    df1 = pd.DataFrame({'HPHM':list('bbacaabd'), 'numbers': range(8)})
    print(df1)
    df2 = pd.DataFrame({'HPHM': list('abe'), 'numbers': range(3)})
    df2.rename(columns={'numbers': 'holiday_numbers'}, inplace=True)
    print(df2)

    df3 = pd.concat([df1, df2], ignore_index=True, sort=True)  # 直接合并2个表格（上下合并）
    print(df3)

    df4 = df3.groupby('HPHM').sum().reset_index()   # 按HPHM列的内容进行表内合并，对其他列执行求和的操作
    print(df4)



    return 0


def Start_Time_List(start_time, end_time):
    start_day = start_time[:10]
    end_day = end_time[:10]
    start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
    end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
    period = (end_day-start_day).days
    weeks = math.ceil(1.0*period/7)
    st_week_day = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").weekday()
    if st_week_day != 0:
        start_time = Convert_strTo_time_then_str(start_time, -1440*st_week_day)
    start_time_list = []
    for i in range(weeks):
        start_time_list.append()


    return

# 求x和y的最小公倍数
def lcm(x, y):
    #  获取最大的数
    if x > y:
        greater = x
    else:
        greater = y

    while (True):
        if ((greater % x == 0) and (greater % y == 0)):
            lcm = greater
            break
        greater += 1

    return lcm

def Flow_statistical(ls_query_result, timedelta, step_length):
    if timedelta > step_length:
        low_cm = lcm(timedelta, step_length)
        loop_num = low_cm / step_length

def Round_datetime(date_time):
    tem = str(date_time)[:-2]+'00'
    tem = pd.to_datetime(tem, format='%Y-%m-%d %H:%M:%S')
    return tem

if __name__ == '__main__':
    # print(Convert_strTo_time_then_str('2019-05-02 16:00:00',-10))
    # print(type(Convert_strTo_time_then_str('2019-05-02 16:00:00',-10)))
    # print(Start_End_time_list('2019-05-02 16:00:00',124))
    # Data_Combine()
    # print(Start_Time_List('2019-04-29 00:00:00', '2019-08-27 00:00:00'))
    ls_query_result = pd.read_csv('D:\\Python_Project\\XC\\query_ls.csv', header=None)
    ls_query_result[0] = pd.to_datetime(ls_query_result[0], format='%Y-%m-%d %H:%M:%S')
    ls_list = ls_query_result[0].to_list()
    ls_result = pd.Series(np.ones(len(ls_list)), index=ls_list)
    # new_ele = pd.Series(data=[0], index=Round_datetime(ls_result.index[0]))
    starttime = Round_datetime(ls_result.index[0])
    ls_result_with_timedelta = ls_result[(ls_result.index >= starttime+datetime.timedelta(minutes=2))]
    # print(ls_result_with_timedelta)
    ls_sample = ls_result.resample('5T', base=2).sum()
    print(ls_sample)
    # print(ls_result_with_timedelta.resample('5T',).sum())
    ls_result.loc[starttime] = 0
    ls_result = ls_result.sort_index()
    # print(ls_result.resample('5T').sum())

    # print(str(ls_result.index[0])[:-2]+'00')
    # print(type(Round_datetime(ls_result.index[0])))


