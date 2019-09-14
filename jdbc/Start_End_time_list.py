import datetime
from jdbc.Convert_strTo_time_then_str import Convert_strTo_time_then_str


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
        ed1 = Convert_strTo_time_then_str(start_time, 120+i*1440)
        ed2 = Convert_strTo_time_then_str(start_time, 130+i*1440)
        lis2 = [ed1, ed2]
        end_time_list.append(lis2)
    return start_time_list, end_time_list

def Week_Period(start_time):        # start: '2019-05-02 00:00:00'
    st_week_day = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").weekday() # 输入的起始时间是周几
    print(st_week_day)
    week_period = []
    if st_week_day != 0:
        new_start_time = Convert_strTo_time_then_str(start_time, 1440*(7-st_week_day))
        week_period.append(new_start_time)
        end_time = Convert_strTo_time_then_str(new_start_time,10080)
        week_period.append(end_time)
    else:
        week_period.append(start_time)
        end_time = Convert_strTo_time_then_str(start_time, 10080)
        week_period.append(end_time)

    return week_period

if __name__ == '__main__':
    week_period = Week_Period('2019-05-01 00:00:00')
    print(week_period)
