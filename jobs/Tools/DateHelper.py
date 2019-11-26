import datetime

# 日期转字符串
def date2str(y,m,d):
    d = datetime.datetime(year=y, month=m, day=d) #初始化datetime类的时间
    return  d.strftime('%Y-%m-%d')

# 今天的字符串格式
def today2str(type=3):
    if type==1:
        return datetime.datetime.today().strftime('%Y/%m/%d')
    elif type==2:
        return datetime.datetime.today().strftime('%Y%m%d')
    else:
        return datetime.datetime.today().strftime('%Y-%m-%d')

#把datetime转成字符串
def datetime_toString(dt):
    return dt.strftime("%Y-%m-%d")

# 字符串转日期
def str2date(str,type = 0):
    if type==1:
        return datetime.datetime.strptime(str, "%Y/%m/%d").date()
    elif type==2:
        return datetime.datetime.strptime(str, "%Y%m%d").date()
    else:
        return datetime.datetime.strptime(str, "%Y-%m-%d").date()

# 字符串格式的日期差
def strDateminus(day1,day2):
    d1 = datetime.datetime.strptime(day1, "%Y-%m-%d").date()
    d2 = datetime.datetime.strptime(day2, "%Y-%m-%d").date()
    d = d2-d1
    return d.days

# 获取字符串格式的明天的日期
def getTomorrow():
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    return datetime_toString(tomorrow)

def getNowstr():
    n = datetime.datetime.now()
    return str(n)

# 获取字符串格式的昨天的日期
def getYesterday():
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    return datetime_toString(yesterday)
