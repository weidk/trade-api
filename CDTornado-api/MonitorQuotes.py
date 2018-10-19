from heads import *

# 查询所有需监控的持仓
def ReadMonitorBonds():
    Position = pd.read_sql("select * from POSITION ",EngineIS)


# 查询中介报价
def ReadBid(id=0):
    DF = pd.read_sql(
        "SELECT ID,BONDCODE,BROKERNAME,BID,OFR,latestTradePrice FROM [qbdb].[dbo].[QBBBO] where ID>"+str(id),
        Engine)
    NEWDF = DF[DF['ID'].isin(DF.groupby('BONDCODE')['ID'].max().values)]
    return NEWDF


# 筛选出持仓内的债券的报价
def FilterPositionQuotes(NEWDF,Position):
    PositionQuoteDf = NEWDF[NEWDF['BONDCODE'].isin(Position.BONDCODE)]
    return PositionQuoteDf
