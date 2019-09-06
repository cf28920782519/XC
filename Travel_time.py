from jdbc.Connect import get_connection, free
import datetime
import pandas as pd
from jdbc.holiday import holiday

def plate_match(conn, SSID, start_time, end_time):
    # SSID=['HK-92','HK-93'] [下游，上游], start_time是一个list，形式为：['2019-7-2 17:00:00', '2019-7-2 16:00:00']
    date_type = holiday(start_time[0][0:10])
    if conn == None:
        conn = get_connection()  # 建立数据库连接

    cr = conn.cursor()  # 生成连接的游标
    # 查询路段下游的车牌和经过时间
    query_plate_low = ("SELECT HPHM, HPZL, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('1','2','3') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID[0],start_time[0],end_time[0])
    cr.execute(query_plate_low)
    query_res_low = cr.fetchall()
    dataframe_res_low = pd.DataFrame(list(query_res_low),columns=['HPHM', 'HPZL','JGSJ'])

    # 查询路段上游的车牌和经过时间
    query_plate_upper = ("SELECT HPHM, HPZL, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('10','2','3'，'4') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID[1],start_time[0],end_time[0])
    cr.execute(query_plate_upper)
    query_res_upper = cr.fetchall()
    dataframe_res_upper = pd.DataFrame(list(query_res_upper),columns=['HPHM', 'HPZL','JGSJ'])

    # 上下游匹配并计算路段旅行时间,merge之后jgsj自动分成2列:jgsj_x（下游）和jgsj_y（上游）
    merge_ls = pd.merge(dataframe_res_low, dataframe_res_upper, on='HPHM')
    merge_ls = merge_ls.drop(index=(merge_ls.loc[(merge_ls['HPHM'] == '车牌')].index)).reset_index()   # 删除hphm列中，值为车牌的行
    merge_ls = merge_ls.drop(index=(merge_ls.loc[merge_ls['JGSJ_x'] < merge_ls['JGSJ_y']].index)).reset_index() # 如果下游检测时间小于上游检测时间，说明匹配错误
    merge_ls['travel_time'] = merge_ls['JGSJ_x'].sub(merge_ls['JGSJ_y'])    # jgsj_x - jgsj_y，即下游检测时间-上游检测时间
    merge_ls['travel_time'] = pd.to_numeric(merge_ls['travel_time'].dt.seconds, downcast='integer') # 计算结果从timedelta转为int，以秒为单位

    # print(merge_ls['travel_time'].dtypes)
    merge_ls = merge_ls[['HPHM', 'HPZL_y', 'JGSJ_x', 'JGSJ_y', 'travel_time']] # 提取列表中的5列组成新的merge_ls
    merge_ls['date_type'] = date_type   # 新增一列，用于表征日期性质，0：工作日；1：周末；2：节假日

    free(conn, cr)
    return merge_ls

if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    query_res = plate_match(conn, ['HK-92','HK-93'], ['2019-05-02 16:00:00','2019-05-02 15:50:00'],['2019-05-02 18:00:00','2019-05-02 18:10:00'])
    print(query_res)


