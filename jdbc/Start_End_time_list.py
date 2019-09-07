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
