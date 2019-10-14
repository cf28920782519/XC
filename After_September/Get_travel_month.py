from jdbc.Connect import get_connection, free
import pysnooper

# 输入一个车牌，返回该车牌在路段上的旅行时间的月份（输出格式[int1,int2,int3]）
def get_travel_month(conn, hphm):
    if conn == None: conn = get_connection()
    cr = conn.cursor()  # 生成连接的游标
    # 查询语句,查询的3个表，都可以忽略信号配时的影响（路中卡口）
    query_sql_1 = ("SELECT JGSJ_x from TRAVEL_TIME_HK93TOHK107 WHERE HPHM='%s'") % (hphm)
    query_sql_2 = ("SELECT JGSJ_x from TRAVEL_TIME_HK92TOHK107 WHERE HPHM='%s'") % (hphm)
    query_sql_3 = ("SELECT JGSJ_x from TRAVEL_TIME_HK93TOHK92 WHERE HPHM='%s'") % (hphm)
    cr.execute(query_sql_1) # 执行查询语句1
    res1 = cr.fetchall()    # 查询结果赋值res1
    cr.execute(query_sql_2) # 执行查询语句2
    res2 = cr.fetchall()    # 查询结果赋值res2
    cr.execute(query_sql_3) # 执行查询语句3
    res3 = cr.fetchall()    # 查询结果赋值res3
    res = res1 + res2 + res3    # 查询的3个list合并，结果示例 [(res1),(res2),...]
    free(conn, cr)  # 关闭查询
    lis_res = []    # 提取查询到的旅行时间（datetime格式），并存储至该list
    for ele in res: # 查询结果合并list中的每个元素（tuple），将它的第一个值添加到上一行的list
        lis_res.append(ele[0])
    month_list = [x.month for x in lis_res] # 提取lis_res中每个元素的月份信息并存在一个列表中
    return month_list   # 返回结果

# 下面的判断用于判断车辆是否为接小孩的车辆，判断逻辑是：
# 8月份不出行，7月份出行次数占7、8、9这3个月总出行次数的10%以下
# return 0表示该输入不是接小孩车辆
# @pysnooper.snoop()
def whether_school_period_drive(month_list):
    if len(month_list) == 0:    # 如果给定的出行月份列表为空，则返回0
        return 0
    else:
        if 8 not in month_list: # 如果出行月份的列表中不含8月
            fre_in_7 = 1.0 * (month_list.count(7)) / len(month_list)    # 计算7月出行次数占比
            if fre_in_7 < 0.1:  # 占比小于0.1
                if 9 in month_list: return 1    # 含有9月份出行   返回1
            else:   # 其他情况返回0
                return 0
        else:   # 含有8月份出行，返回0
            return 0


if __name__ == '__main__':
    conn = None
    lis_res = get_travel_month(conn, '皖PWA816')
    print(whether_school_period_drive(lis_res))
