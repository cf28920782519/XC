from jdbc.Connect import get_connection, free
import datetime
import pandas as pd

def plate_match(conn, SSID, start_time, end_time):
    # SSID=['HK-92','HK-93'] [下游，上游], start_time是一个list，形式为：['2019-7-2 17:00:00', '2019-7-2 16:00:00']
    if conn == None:
        conn = get_connection()  # 建立数据库连接

    cr = conn.cursor()  # 生成连接的游标
    # 查询路段下游的车牌和经过时间
    query_plate_low = ("SELECT HPHM, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('1','2','3') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID[0],start_time[0],end_time[0])
    cr.execute(query_plate_low)
    query_res_low = cr.fetchall()
    dataframe_res_low = pd.DataFrame(list(query_res_low),columns=['hphm', 'jgsj'])

    # 查询路段上游的车牌和经过时间
    query_plate_upper = ("SELECT HPHM, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('2','3'，'4') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID[1],start_time[0],end_time[0])
    cr.execute(query_plate_upper)
    query_res_upper = cr.fetchall()
    dataframe_res_upper = pd.DataFrame(list(query_res_upper),columns=['hphm', 'jgsj'])


    # 上下游匹配并计算路段旅行时间,merge之后jgsj自动分成2列:jgsj_x（下游）和jgsj_y（上游）
    merge_ls = pd.merge(dataframe_res_low, dataframe_res_upper, on='hphm')
    merge_ls = merge_ls.drop(index=(merge_ls.loc[(merge_ls['hphm'] == '车牌')].index)).reset_index()   # 删除hphm列中，值为车牌的行
    merge_ls = merge_ls.drop(index=(merge_ls.loc[merge_ls['jgsj_x'] < merge_ls['jgsj_y']].index)).reset_index() # 如果下游检测时间小于上游检测时间，说明匹配错误
    merge_ls['travel_time'] = merge_ls['jgsj_x'].sub(merge_ls['jgsj_y'])    # jgsj_x - jgsj_y，即下游检测时间-上游检测时间
    # merge_ls.drop(index=(merge_ls.loc[merge_ls['travel_time']<datetime.timedelta(days=-1,hours=23,minutes=59,seconds=59)].index)).reset_index()

    # 检验旅行时间的有效性
    travel_time_list = merge_ls['travel_time'].tolist()
    print(travel_time_list)



    free(conn, cr)
    return merge_ls

if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    query_res = plate_match(conn, ['HK-92','HK-93'], ['2019-05-02 16:00:00','2019-05-02 15:50:00'],['2019-05-02 18:00:00','2019-05-02 18:10:00'])
    print(query_res)


