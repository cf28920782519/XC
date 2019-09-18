import datetime, math
from jdbc.Convert_strTo_time_then_str import Convert_strTo_time_then_str
from jdbc.holiday import holiday


# 用于旅行时间计算Travel_time.py生成起始时间和终止时间
# 返回2个二维数组，list中的每个元素示例：
# start:['2019-05-02 16:00:00', '2019-05-02 15:50:00']; end:  ['2019-05-02 18:00:00', '2019-05-02 18:10:00']
def Start_End_time_list(start_time,date_length): # 输入start_time格式为'2019-05-02 16:00:00'；date_length为整型，表示时间跨度
    start_time_list = []
    end_time_list = []
    for i in range(date_length):
        st1 = Convert_strTo_time_then_str(start_time, i*1440)
        st2 = Convert_strTo_time_then_str(start_time, -10+i*1440)
        lis1 = [st1, st2]
        start_time_list.append(lis1)
        ed1 = Convert_strTo_time_then_str(start_time, 90+i*1440)
        ed2 = Convert_strTo_time_then_str(start_time, 100+i*1440)
        lis2 = [ed1, ed2]
        end_time_list.append(lis2)
    return start_time_list, end_time_list

def Week_Period(start_time):        # start: '2019-05-02 00:00:00'，返回start_time日期所在的周
    st_week_day = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").weekday() # 输入的起始时间是周几
    # print(st_week_day)
    week_period = []
    if st_week_day != 0:    # 0-6是代表从周一到周日
        new_start_time = Convert_strTo_time_then_str(start_time, -1440*st_week_day)
        week_period.append(new_start_time)
        end_time = Convert_strTo_time_then_str(new_start_time, 10080)
        week_period.append(end_time)
    else:
        week_period.append(start_time)
        end_time = Convert_strTo_time_then_str(start_time, 10080)
        week_period.append(end_time)

    return week_period

def Add_serval_days(start_time, date_length):   # start_time格式同上，date_length是个int类型，返回一个表征时间的字符串
    end_time = Convert_strTo_time_then_str(start_time, 1440*date_length)
    return end_time

def Get_Holidays_during_Aweek(start_time): # 输入一个时间，返回该周对应的休息日列表
    st_week_day = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").weekday()
    if st_week_day != 0:
        start_time = Convert_strTo_time_then_str(start_time, -1440*st_week_day)

    holidays_list = []
    for i in range(7):
        tem = Add_serval_days(start_time, i)
        if holiday(tem[0:10]) != 0:
            holidays_list.append(tem)

    return holidays_list

# 输入起始日期和截止日期的时间，生成每周一的起始时间列表，用于High Frequency Vehicle.py的输入
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
        start_time_list.append(Add_serval_days(start_time, i*7))

    return start_time_list





if __name__ == '__main__':
    start_time_list, end_time_list = Start_End_time_list('2019-05-01 06:30:00',2)
    print(start_time_list)
    print(end_time_list)
    # week_period = Week_Period('2019-05-01 00:00:00')
    # print(week_period)
    #
    # holiday_list = Get_Holidays_during_Aweek('2019-05-01 00:00:00')
    # print(holiday_list)
    # print(Start_Time_List('2019-04-29 00:00:00', '2019-08-27 00:00:00'))

