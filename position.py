from heads import *

# 查询所有持仓
def getAllPosition():
    Df = pd.read_sql("SELECT * FROM [InvestSystem].[dbo].[checkPosition]  where date >= convert(nvarchar(8),getdate(),112) ",Engine)
    Df.date = Df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
    return Df.to_json(orient="records")

# 新建一条记录
def createNewPositon(df):
    df.to_sql('checkPosition', EngineIS, if_exists='append', index=False, index_label=df.columns,dtype={
        'trader': sqlalchemy.String,
        'bondcode': sqlalchemy.String,
        'bondname': sqlalchemy.String,
    })

# 删除一条记录
def deleteData(data):
    pd.read_sql_query("delete from checkPosition where id = '"+str(data['id'])+"' ",EngineIS)