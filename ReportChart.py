from heads import *

# 获取收益数据
def ReadProfitDF(endDate):
    # DF = pd.read_sql("select tradedate,fundacc,round((totalprofit_f+totalprofit_s)/10000,2) profit from marketmaker.dbo.fundanalysis where tradedate>='2019-01-01'  and tradedate<='"+endDate+"' order by tradedate",Engine73)
    # DF = DF[DF.fundacc!='103']
    # def changeFundacc(fund):
    #     switch ={
    #         '101':'信用方向',
    #         '102':'利率方向',
    #         '104':'转债方向',
    #         '201':'套利方向',
    #         '202':'战略组合',
    #         '301':'做市策略1',
    #         '302':'做市策略2',
    #     }
    #     return  switch.get(fund,'')
    #
    # DF.fundacc = DF.fundacc.apply(changeFundacc)
    # SumDf = DF.groupby(['tradedate']).sum()
    # SumDf = SumDf.reset_index('tradedate')
    # SumDf['fundacc'] = '汇总'
    # RstDf = pd.concat([DF,SumDf],ignore_index=True)
    # RstDf = RstDf.sort('tradedate')
    Df = pd.read_sql("select tradedate,investtype,round(TotalProfit/10000,2) profit from marketmaker.dbo.FundAnalysis_Sum where tradedate<='" + endDate + "' order by tradedate",Engine73)
    # Df = pd.read_sql("select tradedate,investtype,round(TotalProfit/10000,2) profit from marketmaker.dbo.FundAnalysis_Sum  order by tradedate",Engine73)
    # DfFF = pd.read_sql( "select * from openquery(TEST1,'select substr(t.bondcode,4,1) type,  t.bondname,t.bondcode,t.issueday,t.allocationamt,b.term  from ibond.DSC_BONDSALEPROFIT t   inner join ibond.VIEW_DH b   on t.bondcode = b.code  where t.username in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')  and t.category = ''金融债''  and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)  and  to_date(t.issueday,''yyyy-mm-dd'')> trunc(sysdate,''y'') order by t.id desc ')", Engine)
    # DfFF = pd.read_sql( "select * from openquery(TEST1,'select substr(t.bondcode,4,1) type,  t.bondname,t.bondcode,t.issueday,t.allocationamt,b.term  from ibond.DSC_BONDSALEPROFIT t   inner join ibond.VIEW_DH b   on t.bondcode = b.code  where t.username in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')  and t.category = ''金融债''  and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3) and  to_date(t.issueday,''yyyy-mm-dd'')> ''2020-01-01'' order by t.id desc ')", Engine)
    DfFF = pd.read_sql("select * from openquery(TEST1,'select substr(t.bondcode,4,1) type,  t.bondname,t.bondcode,t.issueday,t.allocationamt,t.term  from  smp_dhzq_new.VTY_BOND_SALE_PROFIT   t where t.tradename in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')   and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)   and  to_date(t.issueday,''yyyy-mm-dd'')> trunc(sysdate,''y'') ')",Engine)
    def ff(type, term, amt):
        type = float(type)
        term = float(term)
        if type == 2:
            switch = {
                1: 0.08,
                2: 0.08,
                3: 0.15,
                5: 0.24,
                7: 0.29,
                10: 0.34,
                20: 0.5,
            }
            return switch.get(term, 0) * amt / 100
        else:
            switch = {
                1: 0.09,
                2: 0.09,
                3: 0.09,
                5: 0.14,
                7: 0.14,
                10: 0.19,
            }
            return switch.get(term, 0.04) * amt / 100
    if DfFF.shape[0]>0:
        DfFF['FF'] = DfFF.apply(lambda x: ff(x.TYPE, x.TERM, x.ALLOCATIONAMT), axis=1)
        FFGroup = DfFF.groupby('ISSUEDAY')['FF'].sum()
        FFGroup = FFGroup.cumsum()
        def adjustprofit(date,type,profit):
            if type in ['组合111','做市','全部']:
                newProfit = profit + FFGroup[FFGroup.index<=date].max()
                return newProfit
            else:
                return profit
        Df['profit'] = Df.apply(lambda x:adjustprofit(x.tradedate,x.investtype,x.profit),axis=1)
    return Df.to_json(orient='records')

def getInvestType():
    Df = pd.read_sql("select distinct investtype from marketmaker.dbo.FundAnalysis_Sum where weightedMoney != 0 ",Engine73)
    Df2 = pd.read_sql("select max(tradedate) from marketmaker.dbo.FundAnalysis_Sum where weightedMoney != 0",Engine73)
    maxDate = Df2.ix[0,0]
    return json.dumps({
        'filterItem': Df.to_dict(orient='records'),
        'maxDate': maxDate,
    })



# 获取收益数据
def ReadProfitTableDF(date):
    Df = pd.read_sql("select * from marketmaker.dbo.FundAnalysis_Sum where tradedate='" + date + "'  order by SortId",Engine73)
    Df = Df.round(2)
    return Df.to_json(orient='records')


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
    # return SumDf.sort('ftype').to_json(orient = 'records')
    return SumDf.sort_values('ftype').to_json(orient = 'records')

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


# 查詢返费
def ReadFF():
    # Df = pd.read_sql("select * from openquery(TEST1,'select substr(t.bondcode,4,1) type,  t.bondname,t.bondcode,t.issueday,t.allocationamt,round(b.term) term  from ibond.DSC_BONDSALEPROFIT t   inner join ibond.VIEW_DH b   on t.bondcode = b.code  where t.username in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')  and t.category = ''金融债''  and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)  and  to_date(t.issueday,''yyyy-mm-dd'')> trunc(sysdate,''y'') order by t.id desc ')",Engine)
    Df = pd.read_sql("select * from openquery(TEST1,'select *  from  smp_dhzq_new.VTY_BOND_SALE_PROFIT   t where t.tradename in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')   and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)  and  to_date(t.issueday,''yyyy-mm-dd'')> trunc(sysdate,''y'')  ')",Engine)
    # Df = pd.read_sql("select * from openquery(TEST1,'select *  from  smp_dhzq_new.VTY_BOND_SALE_PROFIT   t where t.tradename in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')   and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)   and  to_date(t.issueday,''yyyy-mm-dd'')> ''2020-01-01'' ')",Engine)
    def ff(type,term,amt):
        type = float(type)
        term = float(term)
        if type==2:
            switch = {
                1: 0.08,
                2: 0.08,
                3: 0.15,
                5: 0.24,
                7: 0.29,
                10: 0.34,
                20: 0.5,
            }
            # switch = {
            #     1: 0.09,
            #     2: 0.14,
            #     3: 0.14,
            #     5: 0.24,
            #     7: 0.27,
            #     10: 0.33,
            # }
            return switch.get(term, 0)*amt/100
        else:
            switch = {
                1: 0.09,
                2: 0.09,
                3: 0.09,
                5: 0.14,
                7: 0.14,
                10: 0.19,
            }
            return switch.get(term, 0.04) * amt / 100
    Df['FF'] = Df.apply(lambda x:ff(x.TYPE,x.TERM,x.ALLOCATIONAMT),axis=1)
    def WeightAmt(type,term,amt):
        type = float(type)
        term = float(term)
        if type == 2:
            switch = {
                1: 0.5,
                2: 0.5,
                3: 0.8,
                4: 1,
                5: 1,
                6: 1,
                7: 1.2,
                10: 1.2,
                20: 2,
            }
            weightAmt = amt * switch.get(term, 1.5)
            return weightAmt
        else:
            return amt
    Df['WEIGHTAMT'] = Df.apply(lambda x: WeightAmt(x.TYPE, x.TERM, x.ALLOCATIONAMT), axis=1)
    return Df.to_json(orient="records")

def ClassifyAcc(funacc):
        switch ={
            '101': '宏观对冲',
            '102': '信用方向',
            '103': '转债策略',
            '104': '转债方向',
            '105': '利率策略',
            '106': '风格轮动',
            '201': '组合111',
            '202': '组合222',
            '203': '组合333',
            '205': '组合555',
            '206': '组合666',
            '207': '组合777',
            '208': '组合888',
            '209': '组合999',
        }
        return switch.get(funacc,'')

# 按久期分组的基点价值
def DV01byDuration(date):
    Df = pd.read_sql("SELECT [FundAcc],SType,Direction,round([AdjDuration],0) duration,SUM(BPValue/10000) dv01  FROM [MarketMaker].[dbo].[AccPosition2] where TradeDate = '"+date+"'   and TotalAmount>0  group by [FundAcc],SType,Direction,round([AdjDuration],0)", Engine73)
    Df['FundAcc'] = Df.FundAcc.apply(ClassifyAcc)
    # Df['Direction'] = Df.Direction.apply(lambda x:-1 if x=='多' else 1)
    # Df['dv01'] = Df.dv01 * Df.Direction
    # Df['dv01'] = Df.apply(lambda x:x.dv01*10000 if x.SType[0]=='T' else x.dv01,axis=1)
    return Df.to_json(orient="records")

# 查询报表持仓明细
def PosDetails(date):
    Df = pd.read_sql("SELECT  [FundAcc] ,[SCode],[SName],[Direction],[TotalAmount],[TotalProfit] FROM [MarketMaker].[dbo].[AccPosition2] where TradeDate = '"+date+"'",Engine73)
    Df['FundAcc'] = Df.FundAcc.apply(ClassifyAcc)
    return Df.to_json(orient="records")

# 回撤及收益回撤比等风控数据
def DrawDown():
    Df = pd.read_sql("SELECT * FROM [Report].[dbo].[DrawDown] where tradedate>'2020-01-01' order by tradedate", Engine)
    Df.TradeDate = Df.TradeDate.astype('str')
    Df.MaxDrawdownDate = Df.MaxDrawdownDate.astype('str')
    Df.EarningtoMaxdrawdown = Df.apply(lambda x:0 if x.TradeDate<'2020-01-20' else x.EarningtoMaxdrawdown,axis=1)
    Df = Df.round(4)
    return Df.to_json(orient="records")

# 获取几点价值走势
def GetDV01Ts():
    Df = pd.read_sql("SELECT [TradeDate],[InvestType],[BpValue]  FROM [MarketMaker].[dbo].[FundAnalysis_Sum]   where  BpValue !=0 order by TradeDate", Engine73)
    Df.BpValue = round(Df.BpValue/10000)
    Df.TradeDate = Df.TradeDate.astype('str')
    return Df.to_json(orient="records")


