from heads import *


# 获取所有分数数据
def ReadScore():
    SD = pd.read_sql("select * from MarketScore",EngineIS)
    SD.date = SD.date.astype(str)
    return SD.to_json(orient='records')


# 国开做市成交数据
def CalDealAmt(startDate,endDate):
    DealDF = pd.read_sql("select * from openquery(TEST,'select t.trademethod,t.selftradername,sum(t.totalfacevalue)/100000000 amt from marketanalysis.CSTPCBMEXECUTION t where  to_char(tradedate, ''yyyy-mm-dd'')  >= ''"+startDate+"''   and  to_char(tradedate, ''yyyy-mm-dd'')  <= ''"+endDate+"''   and t.typeofdeal=''尝试做市'' and t.bondname like ''%国开%''  group by t.selftradername,t.trademethod')",Engine)
    def changeDealType(type):
        switch={
            '撮合' : 'XBOND',
            '请求报价' : '请求',
            '一次点击成交' : '双边',
        }
        return switch.get(type,'')

    DealDF.TRADEMETHOD = DealDF.TRADEMETHOD.apply(changeDealType)

    def changeTrader(trader):
        switch={
            '葛剑鸣' : '资金室',
            '郭宇涵' : '资金室',
            '皮静娟' : '资金室',
        }
        return switch.get(trader,'投资')

    DealDF.SELFTRADERNAME = DealDF.SELFTRADERNAME.apply(changeTrader)
    DealDF.AMT = DealDF.AMT.astype(float)
    DealDF.columns = ['TRADEMETHOD', 'DEPARTMENT', 'AMT']
    DealDF = DealDF.groupby(['TRADEMETHOD', 'DEPARTMENT']).sum().reset_index()
    return DealDF.to_json(orient='records')

# 获取信用做市分数
def ReadCreditScore():
    SD = pd.read_sql("select * from openquery(TEST,'select to_char(quotedatetime, ''yyyy-mm-dd'') days, initiator,  (sum(buytotalfacevalue) / 100000000 + sum(selltotalfacevalue) / 100000000) amt  from marketanalysis.cmdscbmmarketmakerquote t  where to_char(quotedatetime, ''yyyy-mm-dd'') >= ''2018-03-01''  and length(t.bondcode) > 6  and t.initiator  in (''东海证券'',''东方证券'',''中信证券'',''中信建投证券'',''第一创业证券'')  and t.transtype = ''新报价'' group by initiator, to_char(quotedatetime, ''yyyy-mm-dd'') order by days')",Engine)
    SD.AMT = SD.AMT.astype('float')
    return SD.to_json(orient='records')