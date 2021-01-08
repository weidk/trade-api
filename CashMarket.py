from heads import *

# 读取存单余额数据
def ReadNCDBalance(start,end):
    Df = pd.read_sql("select * FROM InvestSystem.dbo.NCDBalance where banktype !='政策性银行'  and date>='"+start+"' and date<='"+end+"'",Engine)
    Df.DATE = Df.DATE.astype('str')
    return Df.to_json(orient="records")

# 读取交易所协议回购数据
def ReadExchangeRate(start,end):
    Df = pd.read_sql("SELECT 名称 type,date,[加权平均利率(%)] rate  FROM [creditdb].[dbo].[Negotiatedrepurchase] where 名称 in ('R001','R007','R014','R021')  and date>='"+start+"' and date<='"+end+"' ORDER BY DATE",Engine)
    Df = Df[Df.rate>0]
    Df.date = Df.date.astype('str')
    return Df.to_json(orient="records")