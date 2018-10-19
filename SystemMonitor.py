from heads import *

# 监控最新行情时差
def QueryLastestInterval():
    Df = pd.read_sql("select * from openquery(QB,'select round(TO_NUMBER(to_date(t.createtime,''yyyy-mm-dd hh24:mi:ss'') - to_date(t.createdatetime,''yyyy-mm-dd hh24:mi:ss''))* 24 * 60*60 +1) spans  from QBBBO t  where rownum=1  order by id desc')",Engine)
    return Df.ix[0,0]

# 监控最大行情时差
def QueryMaxInterval():
    Df = pd.read_sql("select * from openquery(QB,'select max(round(TO_NUMBER(to_date(t.createtime,''yyyy-mm-dd hh24:mi:ss'') - to_date(t.createdatetime,''yyyy-mm-dd hh24:mi:ss''))* 24 * 60 )) spans  from QBBBO t  ')",Engine)
    return Df.ix[0,0]

# 监控最新更新时间
def QueryLatestUpdate():
    Df = pd.read_sql("select * from openquery(QB,'select substr(max(createtime),11) ct , round(TO_NUMBER(sysdate- to_date(max(t.createtime),''yyyy-mm-dd hh24:mi:ss''))* 24 * 60*60+108) interval from QBBBO t  ')", Engine)
    return Df.to_json()