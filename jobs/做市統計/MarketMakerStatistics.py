from heads import *

# 匯總交易方式
def changeDealType(type):
        switch = {
            '撮合': 'XBOND',
            '请求报价': '请求',
            '一次点击成交': '双边',
        }
        return switch.get(type, '')

# 按條線匯總
def changeTrader(trader):
        switch={
            '葛剑鸣' : '资金室',
            '郭宇涵' : '资金室',
            '高雅静' : '资金室',
            '皮静娟' : '资金室',
            '宋如' : '资金室',
        }
        return switch.get(trader,'投资')

# 匯總成交數據
def GroupDealData(startDate,endDate):
    DealDF = pd.read_sql(
        "select * from openquery(TEST,'select t.trademethod,t.selftradername,sum(t.totalfacevalue)/100000000 amt from marketanalysis.CSTPCBMEXECUTION t where  to_char(tradedate, ''yyyy-mm-dd'')  >= ''" + startDate + "''   and  to_char(tradedate, ''yyyy-mm-dd'')  <= ''" + endDate + "''   and t.typeofdeal=''尝试做市''   group by t.selftradername,t.trademethod')",
        Engine)

    # 匯總交易方式
    DealDF.TRADEMETHOD = DealDF.TRADEMETHOD.apply(changeDealType)
    # 按條線匯總
    DealDF.SELFTRADERNAME = DealDF.SELFTRADERNAME.apply(changeTrader)
    DealDF.AMT = DealDF.AMT.astype(float)
    DealDF.columns = ['TRADEMETHOD', 'DEPARTMENT', 'AMT']
    DealDF = DealDF.groupby(['TRADEMETHOD', 'DEPARTMENT']).sum().reset_index()
    return DealDF

# 匯總報價數據
def GroupQuoteData(startDate,endDate):
    QuoteDF = pd.read_sql(
        "select * from openquery(TEST,'select t.selftradername,sum(t.buyorderqty)/100000000 buyamt,sum(t.sellorderqty)/100000000 sellamt from marketanalysis.CSTPCBMMARKETMAKINGQUOTE  t  where t.transtype=''新报价''   and to_char(quotedate, ''yyyy-mm-dd'')  >= ''" + startDate + "''   and  to_char(quotedate, ''yyyy-mm-dd'')  <= ''" + endDate + "''   group by t.selftradername')",
        Engine)
    QuoteDF.SELFTRADERNAME = QuoteDF.SELFTRADERNAME.apply(changeTrader)
    QuoteDF.columns = ['DEPARTMENT', 'BUYAMT', 'SELLAMT']
    QuoteDF.BUYAMT = QuoteDF.BUYAMT.astype(float)
    QuoteDF.SELLAMT = QuoteDF.SELLAMT.astype(float)
    QuoteDF = QuoteDF.groupby('DEPARTMENT').sum().reset_index()
    return QuoteDF

# 寫入excel
def ToExcel(startDate,endDate):
    DealDF = GroupDealData(startDate, endDate)
    QuoteDF = GroupQuoteData(startDate, endDate)
    writer = pd.ExcelWriter('output.xlsx')
    DealDF.to_excel(writer, '成交')
    QuoteDF.to_excel(writer, '報價')
    writer.save()

if __name__ == '__main__':
    startDate = '2018-07-25'
    endDate = '2017-07-25'
    ToExcel(startDate, endDate)