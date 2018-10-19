import pymssql
from sqlalchemy import create_engine

# 获取sqlserver连接句柄
def getConn(host,user,pwd,db):
    conn = pymssql.connect(host=host,user=user,password=pwd,database=db,charset="utf8")
    return conn

def getEngine(host,user,password,db,port='1433'):
    engine = create_engine('mssql+pymssql://'+user+':'+password+'@'+host+':'+port+'/'+db)
    return engine