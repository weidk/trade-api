from heads import *

# 城投成交按省分布
def CityDealsProvince(startTime,endTime):
    Df = pd.read_sql("SELECT PROVINCE name,round(sum(dealfacevalue),0) value FROM creditdb.dbo.BONDDEALDETAIL_vty where  len(DISTRICTandPLATFORM)>0 and TRANSACTTIME>='"+startTime+"' and   TRANSACTTIME<='"+endTime+"'  group by PROVINCE",Engine)
    Df.value = Df.value.astype('float')
    Df.name = Df.name.str.replace('省', '')
    return Df.to_json(orient="records")


# 行业成交走势
def IndustryDealsProvince(startTime,endTime):
    Df = pd.read_sql("SELECT [INDUSTRY],CONVERT(varchar(100), [TRANSACTTIME], 23) TDDATE,sum(DEALFACEVALUE) TOTALDEALS FROM [creditdb].[dbo].[BONDDEALDETAIL_vty] WHERE  len(DISTRICTandPLATFORM)=0 and  TRANSACTTIME>='"+startTime+"' and   TRANSACTTIME<='"+endTime+"'    group by [INDUSTRY],CONVERT(varchar(100), [TRANSACTTIME], 23) ORDER BY TDDATE,TOTALDEALS",Engine)
    INDUSTRY = Df.INDUSTRY.unique()
    TDDATE = Df.TDDATE.unique()
    for day in TDDATE:
        tempDf = pd.DataFrame(INDUSTRY,columns=['INDUSTRY'])
        tempDf['TDDATE'] = day
        tempDf['TOTALDEALS'] = 0
        Df = Df.append(tempDf,ignore_index=True)
    Gf = Df.groupby(['INDUSTRY','TDDATE']).sum()
    Gf = Gf.reset_index()
    Gf['TOTALDEALS'] = Gf.groupby('INDUSTRY').TOTALDEALS.cumsum()
    Gf['MA5-MA20'] = pd.rolling_mean(Gf.groupby('INDUSTRY').TOTALDEALS, 5).reset_index().TOTALDEALS-pd.rolling_mean(Gf.groupby('INDUSTRY').TOTALDEALS, 20).reset_index().TOTALDEALS
    return Gf.to_json(orient="records")

# 查询成交明细
def QueryDealsDetail(industry,tdday):
    Df = pd.read_sql("SELECT  [DEALBONDNAME],[DEALBONDCODE] ,[DEALYIELD],[CNBDYIELD] ,round([deviation],2) deviation ,[MATU],[DEALFACEVALUE],[TRANSACTTIME] ,[ISSUER] FROM [creditdb].[dbo].[BONDDEALDETAIL_vty] where CONVERT(varchar(100), [TRANSACTTIME], 23)='"+tdday+"'  AND [INDUSTRY] ='"+industry+"' order by DEALFACEVALUE desc",Engine)
    Df.TRANSACTTIME = Df.TRANSACTTIME.astype('str')
    return Df.to_json(orient="records")

# 查询换手率
def QueryTurnoverRate(startDate):
    Df = pd.read_sql("SELECT  CONVERT(datetime,[tradedate],112) tradedate,[rate],[totaldealamt],[unexpireamt],round([TurnoverRate],2) [TurnoverRate] FROM [creditdb].[dbo].[CreditTurnoverRate] where tradedate>='"+startDate+"'  and rate in ('AAA+','AAA','AAA-','AA+','AA','AA(2)','AA-') order by tradedate,rate ",Engine)
    Df.tradedate = Df.tradedate.astype('str')
    return Df.to_json(orient="records")

# 查询异常成交
def QueryAbnormalDeals(startTime,endTime,deviation,dealyield):
    Df = pd.read_sql("SELECT * FROM [creditdb].[dbo].[BONDDEALDETAIL_vty]  where  deviation>="+str(deviation)+"   and   DEALYIELD>="+str(dealyield)+"  and   CONVERT(varchar(100), [TRANSACTTIME], 23)>='"+startTime+"' and   CONVERT(varchar(100), [TRANSACTTIME], 23)<='"+endTime+"'",Engine)
    Df2 = pd.read_sql("SELECT distinct(dealbondname) bondname FROM [creditdb].[dbo].[BONDDEALDETAIL_vty]  where  (deviation>="+str(deviation)+"   or   DEALYIELD>="+str(dealyield)+")  and   CONVERT(varchar(100), [TRANSACTTIME], 23)<'"+startTime+"'",Engine)
    def isNew(name):
        if any(Df2.bondname == name):
            return False
        else:
            return True
    Df['ISNEW'] = Df.DEALBONDNAME.apply(isNew)
    Df = Df.sort_values('ISNEW',ascending=False)
    Df.TRANSACTTIME = Df.TRANSACTTIME.astype('str')
    return Df.to_json(orient="records")