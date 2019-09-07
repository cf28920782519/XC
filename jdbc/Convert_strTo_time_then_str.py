import datetime

# 输入start_time格式示例为'2019-05-02 16:00:00',timedelta单位为分钟
def Convert_strTo_time_then_str(start_time,timedelta=0):
    tem = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    tem = tem + datetime.timedelta(minutes=timedelta)
    tem = datetime.datetime.strftime(tem, '%Y-%m-%d %H:%M:%S')
    return tem