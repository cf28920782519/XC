from jdbc.Connect import get_connection, free
import datetime

def plate_match(conn, SSID, start_time, end_time):
    # SSID='HK-92', start_time是一个list，形式为：["'2019-7-2 17:00:00'", "'2019-7-2 16:00:00'"]
    if conn == None:
        conn = get_connection()  # 建立数据库连接

    cr = conn.cursor()  # 生成连接的游标

    query_plate_low = ("SELECT HPHM, JGSJ FROM SJCJ_T_CLXX_LS WHERE SSID='%s' AND CDBH in ('1','2','3') AND JGSJ>=to_date('%s','yyyy-mm-dd hh24:mi:ss') AND JGSJ<=to_date('%s','yyyy-mm-dd hh24:mi:ss')")%(SSID,start_time[0],end_time[0])
    cr.execute(query_plate_low)
    query_res = cr.fetchall()
    cr.close()
    return query_res

if __name__ == '__main__':
    starttime = datetime.datetime.now()  # 统计程序的开始时刻

    conn = None
    query_res = plate_match(conn,'HK-92',['2019-05-02 16:00:00','2019-05-02 15:50:00'],['2019-05-02 18:00:00','2019-05-02 15:50:00'])
    print(query_res)


