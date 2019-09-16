"""
Author: Libo
Email: libo999@vip.qq.com
Date: 19-09-06 下午14:07
"""
# 返回结果解析：
# -1:报错
# 0: 工作日
# 1: 周末
# 2: 节日
# work字典里面（本该放假，但要工作，即被调休的工作日）

import datetime


def holiday(d, s="2019-01-01", e="2019-12-31"):
    hol = {"2019-01-01", "2019-02-04", "2019-02-05", "2019-02-06", "2019-02-07", "2019-02-08", "2019-02-09",
           "2019-02-10", "2019-04-05", "2019-04-06", "2019-04-07", "2019-05-01", "2019-05-02", "2019-05-03",
           "2019-05-04", "2019-06-07", "2019-06-08", "2019-06-09", "2019-09-13", "2019-09-14", "2019-09-15",
           "2019-10-01", "2019-10-02", "2019-10-03", "2019-10-04", "2019-10-05", "2019-10-06", "2019-10-07",
           }
    work = {"2019-02-02", "2019-02-03", "2019-04-28", "2019-05-05", "2019-09-29", "2019-10-12"}
    s1 = datetime.datetime.strptime(s, '%Y-%m-%d')
    e1 = datetime.datetime.strptime(e, '%Y-%m-%d')
    if not isinstance(d, str):
        print("Please input string date")
        return -1
    else:
        d1 = datetime.datetime.strptime(d, '%Y-%m-%d')
        if d1 > e1 or d1 < s1:
            print("not in 2019 year")
            return -1
        elif d in hol:
            return 2
        elif d in work:
            return 0
        elif d1.weekday() in (5, 6):
            return 1
        else:
            return 0


if __name__ == "__main__":
    day = "2019-09-07 16:00:00"[:10]
    print(holiday(day))