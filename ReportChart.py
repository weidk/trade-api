from heads import *

# 获取收益数据
def ReadProfitDF(endDate):
    DF = pd.read_sql("select tradedate,fundacc,round((totalprofit_f+totalprofit_s)/10000,2) profit from marketmaker.dbo.fundanalysis where tradedate>='2019-01-01'  and tradedate<='"+endDate+"' order by tradedate",Engine73)
    DF = DF[DF.fundacc!='103']
    def changeFundacc(fund):
        switch ={
            '101':'信用方向',
            '102':'利率方向',
            '104':'转债方向',
            '201':'套利方向',
            '202':'战略组合',
            '301':'做市策略1',
            '302':'做市策略2',
        }
        return  switch.get(fund,'')

    DF.fundacc = DF.fundacc.apply(changeFundacc)
    SumDf = DF.groupby(['tradedate']).sum()
    SumDf = SumDf.reset_index('tradedate')
    SumDf['fundacc'] = '汇总'
    RstDf = pd.concat([DF,SumDf],ignore_index=True)
    RstDf = RstDf.sort('tradedate')
    return RstDf.to_json(orient='records')

# 持仓分布
def PositionComposition():
    PosDf = pd.read_sql("select stype,totalamount/100 amt from marketmaker.dbo.accposition2 where totalamount>0 and smarket !='ZJ' and tradedate = (select max(tradedate) from marketmaker.dbo.accposition2)",Engine73)
    SumDf = PosDf.groupby('stype').sum()
    SumDf = SumDf.reset_index('stype')

    def ClassifyType(sontype):
        switch ={
            '国债' : '利率债',
            '金融债' : '利率债',
            '同业存单' : '同业存单'
        }
        return switch.get(sontype,'信用债')
    SumDf['ftype'] = SumDf.stype.apply(ClassifyType)
    return SumDf.sort('ftype').to_json(orient = 'records')

# 浮动收益和实现收益
def ProfitDistrubution():
    FRDf = pd.read_sql("select sum(floatprofit) 浮动盈亏,sum(realprofit) 实现盈亏 from marketmaker.dbo.accposition2 where tradedate = (select max(tradedate) from marketmaker.dbo.accposition2)",Engine73)
    RstDf = FRDf.T
    RstDf = RstDf.reset_index()
    RstDf.columns = ['item','count']
    return RstDf.to_json(orient='records')

# 分债券类型统计浮动收益和实现收益
def ProfitDistrubutionGroupbyStype():
    PDf = pd.read_sql("select stype 债券类型,round(sum(floatprofit)/10000,2) 浮动盈亏,round(sum(realprofit)/10000,2) 实现盈亏 from marketmaker.dbo.accposition2 where tradedate = (select max(tradedate) from marketmaker.dbo.accposition2) group by stype order by 实现盈亏 desc",Engine73)
    RstDf1 = PDf.ix[:,[0,1]]
    RstDf1.columns = ['债券类型', '盈亏金额']
    RstDf1['盈亏类型'] = '浮动盈亏'
    RstDf2 = PDf.ix[:, [0, 2]]
    RstDf2.columns = ['债券类型', '盈亏金额']
    RstDf2['盈亏类型'] = '实现盈亏'
    RstDf = pd.concat([RstDf1,RstDf2])
    return RstDf.to_json(orient='records')

# 当日盈亏，总额，基点价值，久期
def AccDetail():
    DF = pd.read_sql("select * from marketmaker.dbo.dailyreport where date = (select max(date) from marketmaker.dbo.dailyreport)",Engine73)
    DF.ix[:,[1,2,3,4,5,8,9,10,12,14]] = DF.ix[:,[1,2,3,4,5,8,9,10,12,14]]/10000
    DF = DF.round(2)
    return DF.to_json(orient='records')