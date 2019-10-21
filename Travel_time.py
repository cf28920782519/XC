from jdbc.Connect import get_connection, free
import datetime
import pandas as pd
from jdbc.holiday import holiday
from jdbc.Start_End_time_list import Start_End_time_list
import numpy as np


def plate_match(conn, SSID, CDBH, start_time, end_time):
    # SSID=['HK-92','HK-107','HK-93'] [终点，起点], start_time是一个list，形式为：['2019-7-2 17:00:00', '2019-7-2 16:00:00']
    date_types = holiday(start_time[0][0:10])
    if conn == None:
        conn = get_connection()  # 建立数据库连接

    cr = conn.cursor()  # 生成连接的游标
    # 查询路段下游的车牌和经过时间，使用时从下面4个终点里面选择一个取消注释（注释掉其他3个终点）
    query_plate_low = (
                          "SELECT HPHM, HPZL, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in %s AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')") % (
                      SSID[0], CDBH[0], start_time[0], end_time[0])

    cr.execute(query_plate_low)
    query_res_low = cr.fetchall()
    dataframe_res_low = pd.DataFrame(list(query_res_low), columns=['HPHM', 'HPZL', 'JGSJ'])

    # 查询路段上游的车牌和经过时间
    # query_plate_upper = ("SELECT HPHM, HPZL, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('1','2','3','4','5','6','10','11','12') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID[1],start_time[0],end_time[0])
    query_plate_upper = (
                            "SELECT HPHM, HPZL, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in %s AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')") % (
                            SSID[1], CDBH[1], start_time[0], end_time[0])
    cr.execute(query_plate_upper)
    query_res_upper = cr.fetchall()
    dataframe_res_upper = pd.DataFrame(list(query_res_upper), columns=['HPHM', 'HPZL', 'JGSJ'])

    # 上下游匹配并计算路段旅行时间,merge之后jgsj自动分成2列:jgsj_x（下游）和jgsj_y（上游）
    merge_ls = pd.merge(dataframe_res_low, dataframe_res_upper, on='HPHM')
    merge_ls = merge_ls.drop(index=(merge_ls.loc[(merge_ls['HPHM'] == '车牌')].index)).reset_index()  # 删除hphm列中，值为车牌的行
    merge_ls = merge_ls.drop(
        index=(merge_ls.loc[merge_ls['JGSJ_x'] < merge_ls['JGSJ_y']].index)).reset_index()  # 如果下游检测时间小于上游检测时间，说明匹配错误
    merge_ls['JGSJ_x'] = pd.to_datetime(merge_ls['JGSJ_x'],format = '%Y-%m-%d %H:%M:%S')
    merge_ls['JGSJ_y'] = pd.to_datetime(merge_ls['JGSJ_y'], format='%Y-%m-%d %H:%M:%S')
    merge_ls['travel_time'] = merge_ls['JGSJ_x'].sub(merge_ls['JGSJ_y'])  # jgsj_x - jgsj_y，即下游检测时间-上游检测时间
    # merge_ls['travel_time'] = pd.to_numeric(merge_ls['travel_time'].dt.seconds, downcast='integer') # 计算结果从timedelta转为int，以秒为单位
    merge_ls['travel_time'] = (merge_ls['travel_time'] / np.timedelta64(1, 's')).astype(
        int)  # 计算结果从timedelta转为int，以秒为单位
    merge_ls = merge_ls.sort_values(by=['JGSJ_y'], ascending=True)  # 将dataframe按照JGSJ_y的排序
    merge_ls = merge_ls.drop_duplicates('JGSJ_x', 'last', inplace=False)  # 按JGSJ_x，去除该列下面的重复行,删除重复项并保留最后一次出现的项
    merge_ls = merge_ls.drop_duplicates('JGSJ_y', 'last', inplace=False)  # 按JGSJ_y，去除该列下面的重复行,删除重复项并保留最后一次出现的项

    # print(merge_ls['travel_time'].dtypes)
    merge_ls = merge_ls[['HPHM', 'HPZL_y', 'JGSJ_x', 'JGSJ_y', 'travel_time']]  # 提取列表中的5列组成新的merge_ls
    merge_ls['date_types'] = date_types  # 新增一列，用于表征日期性质，0：工作日；1：周末；2：节假日

    free(conn, cr)
    return merge_ls


# 将dataframe转换成list，方便导入oracle
def dataframe_Tolist(merge_ls):
    HPHM = merge_ls['HPHM'].tolist()
    HPZL_y = merge_ls['HPZL_y'].tolist()
    JGSJ_x = merge_ls['JGSJ_x'].tolist()
    JGSJ_y = merge_ls['JGSJ_y'].tolist()
    TRAVEL_TIME = merge_ls['travel_time'].tolist()
    DATE_TYPES = merge_ls['date_types'].tolist()

    a = [HPHM, HPZL_y, JGSJ_x, JGSJ_y, TRAVEL_TIME, DATE_TYPES]
    return list(zip(*a))


# # 写入数据库
def Insert_db(conn, table_name, result):
    if conn == None:
        conn = get_connection()
    cr = conn.cursor()
    # print('diaoyong')

    # sql = "INSERT INTO SYS.TRAVEL_TIME_HK93TOHK92(HPHM, HPZL_y, JGSJ_x, JGSJ_y, TRAVEL_TIME, DATE_TYPES) VALUES (:1, :2, :3, :4, :5, :6)"
    sql = ("INSERT INTO %s(HPHM, HPZL_y, JGSJ_x, JGSJ_y, TRAVEL_TIME, DATE_TYPES) VALUES (:1, :2, :3, :4, :5, :6)") % (
        table_name)

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
    ## 开发测试用
    # conn = None
    # query_res = plate_match(conn, ['HK-92', 'HK-93'], ['2019-05-03 16:00:00', '2019-05-03 15:50:00'], ['2019-05-03 18:00:00', '2019-05-03 18:10:00'])
    # result = dataframe_Tolist(query_res)
    # Insert_db(conn, result)
    ## 跑批量数据用的
    start_time_list, end_time_list = Start_End_time_list('2019-10-08 15:55:00', 14)
    for i in range(len(start_time_list)):
        conn = None

        # # 下面是包含路中卡口的4个子路段
        # query_res = plate_match(conn, ['HK-92', 'HK-107'],[('1','2','3'), ('1','2')], start_time_list[i], end_time_list[i])   # 起：HK-107；终：HK-92。跑该子段时取消注释
        # query_res = plate_match(conn, ['HK-93', 'HK-107'], [('7', '8', '9'), ('3', '4')], start_time_list[i],
        #                         end_time_list[i])     # 起：HK-107；终：HK-93。跑该子段时取消注释

        # query_res = plate_match(conn, ['HK-107', 'HK-93'], [('1', '2'), ('1','2','3','4','5','6','10','11','12')], start_time_list[i],
        #                         end_time_list[i])   # 起：HK-93；终：HK-107。跑该子段时取消注释

        query_res = plate_match(conn, ['HK-107', 'HK-92'], [('3', '4'), ('4','5','6','7','8','9','10')], start_time_list[i],
                                end_time_list[i])   # 起：HK-92；终：HK-107。跑该子段时取消注释

        # # 下面是不包含路中卡口的路段2个方向
        # query_res = plate_match(conn, ['HK-92', 'HK-93'], [('1', '2', '3'), ('1','2','3','4','5','6','10','11','12')], start_time_list[i],
        #                         end_time_list[i])   # 起：HK-93；终：HK-92。跑该子段时取消注释

        # query_res = plate_match(conn, ['HK-93', 'HK-92'], [('7', '8', '9'), ('4', '5', '6', '7', '8', '9', '10')],
        #                         start_time_list[i],
        #                         end_time_list[i])  # 起：HK-92；终：HK-93。跑该子段时取消注释
        # print(query_res)
        result = dataframe_Tolist(query_res)
        # print(result)
        # print(len(result))
        print('day: ', i)
        # # 下面是包含路中卡口的4个子路段
        # Insert_db(conn, 'TRAVEL_TIME_HK107TOHK92', result)    # 起：HK-107；终：HK-92。跑该子段时取消注释
        # Insert_db(conn, 'TRAVEL_TIME_HK107TOHK93', result)      # 起：HK-107；终：HK-93。跑该子段时取消注释
        # Insert_db(conn, 'TRAVEL_TIME_HK93TOHK107', result)      # 起：HK-93；终：HK-107。跑该子段时取消注释
        Insert_db(conn, 'TRAVEL_TIME_HK92TOHK107', result)  # 起：HK-92；终：HK-107。跑该子段时取消注释

        # # 下面是不包含路中卡口的路段2个方向
        # Insert_db(conn, 'TRAVEL_TIME_HK93TOHK92', result)  # 起：HK-93；终：HK-92。跑该子段时取消注释
        # Insert_db(conn, 'TRAVEL_TIME_HK92TOHK93', result)  # 起：HK-92；终：HK-93。跑该子段时取消注释

    endtime = datetime.datetime.now()
    print("the program runs : %d s" % (endtime - starttime).seconds)
