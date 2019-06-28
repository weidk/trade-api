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

# xbond主力
def XBondCounter(insname):
    if insname=='银行主力':
        Df = pd.read_sql(
            "select * from openquery(TEST1,'select t.tradedate,sum(t.buyfacevalue-t.sellfacevalue) netbuytotalfacevalue,sum(t.buydeals-t.selldeals) netdeals  from INSDEAL_VTY t  where t.counterpartyshortname in (''昆山农村商行'',''农业银行'',''渣打中国'',''洛阳银行'',''平安银行'')  group by tradedate  order by tradedate ')",
            Engine)
    elif insname=='券商主力':
        Df = pd.read_sql(
            "select * from openquery(TEST1,'select t.tradedate,sum(t.buyfacevalue-t.sellfacevalue) netbuytotalfacevalue,sum(t.buydeals-t.selldeals) netdeals  from INSDEAL_VTY t  where t.counterpartyshortname in (''国泰君安证券'',''浙商证券股份有限公司'',''山西证券'',''民生证券'',''东方证券'')  group by tradedate  order by tradedate ')",
            Engine)
    else:
        Df = pd.read_sql("select * from openquery(TEST1,'select t.tradedate,t.buyfacevalue-t.sellfacevalue netbuytotalfacevalue,t.buydeals-t.selldeals netdeals  from INSDEAL_VTY t  where t.counterpartyshortname in (''"+insname+"'')  order by tradedate ')",Engine)
    Df.NETBUYTOTALFACEVALUE = Df.NETBUYTOTALFACEVALUE.astype('float')
    Df.NETDEALS = Df.NETDEALS.astype('float')
    Df['SUMFACEVALUE'] = Df.NETBUYTOTALFACEVALUE.cumsum()
    Df['SUMDEALS'] = Df.NETDEALS.cumsum()
    return Df.ix[:,['TRADEDATE','SUMFACEVALUE','SUMDEALS']].to_json(orient='records')