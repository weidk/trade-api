# 计算各个组合的回撤
import pandas as pd
import numpy as np
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import sqlalchemy
import time
import warnings
warnings.simplefilter(action = "ignore", category = Warning)

Engine = DB.getEngine('10.28.7.43', 'bond', 'bond', 'Report')
Engine73 = DB.getEngine('192.168.87.73', 'sa', 'tcl+nftx', 'MarketMaker')

# 读取原始数据
def ReadProfitData():
    Df1 = pd.read_sql("SELECT  TradeDate,InvestType,TotalProfit as NetProfit,WeightedPrinciple,WeightedMoney, sortid FROM [MarketMaker].[dbo].[FundAnalysis_Sum] order by TradeDate",Engine73)
    DfFF = pd.read_sql(
        "select * from openquery(TEST1,'select substr(t.bondcode,4,1) type,  t.bondname,t.bondcode,t.issueday,t.allocationamt,b.term  from ibond.DSC_BONDSALEPROFIT t   inner join ibond.VIEW_DH b   on t.bondcode = b.code  where t.username in (''翁应良'',''姚子剑'',''郭圣雨'',''李剑波'',''张智博'')  and t.category = ''金融债''  and t.allocationamt > 0 and substr(t.bondcode,4,1) in (2,3)  and  to_date(t.issueday,''yyyy-mm-dd'')> trunc(sysdate,''y'') order by t.id desc ')",
        Engine)

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

    DfFF['FF'] = DfFF.apply(lambda x: ff(x.TYPE, x.TERM, x.ALLOCATIONAMT), axis=1)
    FFGroup = DfFF.groupby('ISSUEDAY')['FF'].sum()
    FFGroup = FFGroup.cumsum()
    def adjustprofit(date,type,profit):
        if type in ['组合111','做市','全部']:
            newProfit = profit + FFGroup[FFGroup.index<=date].max()*10000
            return newProfit
        else:
            return profit
    Df1['NetProfit'] = Df1.apply(lambda x:adjustprofit(x.TradeDate,x.InvestType,x.NetProfit),axis=1)

    return Df1

# 读取已计算的数据
def ReadCaculatedData():
    Df2 = pd.read_sql("SELECT * FROM [Report].[dbo].[DrawDown] where tradedate>'2020-01-01' order by tradedate",Engine)
    return Df2

# 计算最大回撤、收益回撤比（全量）
def WithDraw(Df):
    # 按组合分组
    Gf = Df.groupby('InvestType')
    InvestTypeList = Df.InvestType.unique().tolist()
    NewDf = pd.DataFrame()
    for type in InvestTypeList:
        tempDf = Gf.get_group(type)
        tempDf = tempDf.reset_index(drop=True)
        tempDf['CumWithDraw'] = 0.00
        tempDf['CumWithDrawRate'] = 0.00
        tempDf['CumWithDrawRateMarketValue'] = 0.00
        tempDf['PerDayWithDraw'] = 0.00
        tempDf['PerDayWithDrawRate'] = 0.00
        tempDf['PerDayWithDrawRateMarketValue'] = 0.00
        tempDf['EarningtoMaxdrawdown'] = 0.00
        tempDf['MaxDrawdown'] = 0.00
        tempDf['MaxDrawdownRate'] = 0.00
        tempDf['MaxDrawdownRateMarketValue'] = 0.00
        tempDf['MaxDrawdownDate'] = ''
        for i in range(0, tempDf.shape[0]):
            if i == 0:
                tempDf.CumWithDraw[i] = 0
                tempDf.CumWithDrawRate[i] = 0
                tempDf.CumWithDrawRateMarketValue[i] = 0
                tempDf.PerDayWithDraw[i] = 0
                tempDf.PerDayWithDrawRate[i] = 0
                tempDf.PerDayWithDrawRateMarketValue[i] = 0
                tempDf.EarningtoMaxdrawdown[i] = 0
                tempDf.MaxDrawdown[i] = 0
                tempDf.MaxDrawdownRate[i] = 0
                tempDf.MaxDrawdownRateMarketValue[i] = 0
                tempDf.MaxDrawdownDate[i] = ''
            else:
                tempDf.CumWithDraw[i] = tempDf.NetProfit[i] - max(tempDf.NetProfit[0:i + 1])
                tempDf.CumWithDrawRate[i] = tempDf.CumWithDraw[i]/(max(tempDf.NetProfit[0:i + 1])+tempDf.WeightedPrinciple[i])
                tempDf.CumWithDrawRateMarketValue[i] = tempDf.CumWithDraw[i]/(max(tempDf.NetProfit[0:i + 1])+tempDf.WeightedMoney[i])
                netChange = tempDf.NetProfit[i] - tempDf.NetProfit[i - 1]
                if netChange < 0:
                    tempDf.PerDayWithDraw[i] = netChange
                else:
                    tempDf.PerDayWithDraw[i] = 0
                tempDf.PerDayWithDrawRate[i] = tempDf.PerDayWithDraw[i]/(tempDf.NetProfit[i-1]+tempDf.WeightedPrinciple[i])
                tempDf.PerDayWithDrawRateMarketValue[i] = tempDf.PerDayWithDraw[i]/(tempDf.NetProfit[i-1]+tempDf.WeightedMoney[i])
                tempDf.MaxDrawdown[i] = min(tempDf.CumWithDraw)
                tempDf.MaxDrawdownRate[i] = min(tempDf.CumWithDrawRate)
                tempDf.MaxDrawdownRateMarketValue[i] = min(tempDf.CumWithDrawRateMarketValue)
                if tempDf.MaxDrawdown[i]==0:
                    tempDf.EarningtoMaxdrawdown[i] = 0
                else:
                    tempDf.EarningtoMaxdrawdown[i] = -tempDf.NetProfit[i] / tempDf.MaxDrawdown[i]
                tempDf.MaxDrawdownDate = tempDf[tempDf.MaxDrawdown[i]==tempDf.CumWithDraw].TradeDate.iloc[0]
            print(tempDf.ix[i, 'InvestType'] + "  " + str(tempDf.ix[i, 'TradeDate']))
        NewDf = NewDf.append(tempDf)
    NewDf.reset_index(drop=True,inplace=True)
    NewDf = NewDf[NewDf.WeightedPrinciple>0]
    return NewDf

# 计算最大回撤、收益回撤比（增量）
def DrawdownAcc(Df1,Df2):
    Df1.TradeDate = Df1.TradeDate.astype('datetime64')
    date1 = Df1.TradeDate.max()
    date2 = Df2.TradeDate.max()
    NewDf = pd.DataFrame()
    if date1>date2:
        Df = Df2.append(Df1[Df1.TradeDate>date2])
        # 按组合分组
        Gf = Df.groupby('InvestType')
        InvestTypeList = Df.InvestType.unique().tolist()
        for type in InvestTypeList:
            tempDf = Gf.get_group(type)
            tempDf = tempDf.reset_index(drop=True)
            for i in tempDf[tempDf.CumWithDraw.isnull()].index:
                if i == 0:
                    tempDf.CumWithDraw[i] = 0
                    tempDf.CumWithDrawRate[i] = 0
                    tempDf.CumWithDrawRateMarketValue[i] = 0
                    tempDf.PerDayWithDraw[i] = 0
                    tempDf.PerDayWithDrawRate[i] = 0
                    tempDf.PerDayWithDrawRateMarketValue[i] = 0
                    tempDf.EarningtoMaxdrawdown[i] = 0
                    tempDf.MaxDrawdown[i] = 0
                    tempDf.MaxDrawdownRate[i] = 0
                    tempDf.MaxDrawdownRateMarketValue[i] = 0
                    tempDf.MaxDrawdownDate[i] = ''
                else:
                    tempDf.CumWithDraw[i] = tempDf.NetProfit[i] - max(tempDf.NetProfit[0:i + 1])
                    tempDf.CumWithDrawRate[i] = tempDf.CumWithDraw[i] / (max(tempDf.NetProfit[0:i + 1]) + tempDf.WeightedPrinciple[i])
                    tempDf.CumWithDrawRateMarketValue[i] = tempDf.CumWithDraw[i] / (max(tempDf.NetProfit[0:i + 1]) + tempDf.WeightedMoney[i])
                    netChange = tempDf.NetProfit[i] - tempDf.NetProfit[i - 1]
                    if netChange < 0:
                        tempDf.PerDayWithDraw[i] = netChange
                    else:
                        tempDf.PerDayWithDraw[i] = 0
                    tempDf.PerDayWithDrawRate[i] = tempDf.PerDayWithDraw[i] / (tempDf.NetProfit[i - 1] + tempDf.WeightedPrinciple[i])
                    tempDf.PerDayWithDrawRateMarketValue[i] = tempDf.PerDayWithDraw[i] / (tempDf.NetProfit[i - 1] + tempDf.WeightedMoney[i])
                    tempDf.MaxDrawdown[i] = min(tempDf.CumWithDraw)
                    tempDf.MaxDrawdownRate[i] = min(tempDf.CumWithDrawRate)
                    tempDf.MaxDrawdownRateMarketValue[i] = min(tempDf.CumWithDrawRateMarketValue)
                    if tempDf.MaxDrawdown[i] == 0:
                        tempDf.EarningtoMaxdrawdown[i] = 0
                    else:
                        tempDf.EarningtoMaxdrawdown[i] = -tempDf.NetProfit[i] / tempDf.MaxDrawdown[i]
                    tempDf.MaxDrawdownDate = tempDf[tempDf.MaxDrawdown[i] == tempDf.CumWithDraw].TradeDate.iloc[0]
                print(tempDf.ix[i,'InvestType']+"  "+str(tempDf.ix[i,'TradeDate']))
            NewDf = NewDf.append(tempDf)
        NewDf.reset_index(drop=True, inplace=True)
        NewDf = NewDf[NewDf.WeightedPrinciple > 0]
        NewDf = NewDf[NewDf.TradeDate>date2]
        NewDf = NewDf.sort_values(['sortid'])
    return NewDf


# 保存到数据库
def DataToSql(NewDf):
    NewDf.to_sql('DrawDown', Engine, if_exists='append', index=False, index_label=NewDf.columns,dtype={'TradeDate': sqlalchemy.DateTime,'MaxDrawdownDate': sqlalchemy.DateTime, 'InvestType': sqlalchemy.String})


if __name__ == '__main__':
        try:
            Df1 = ReadProfitData()
            Df2 = ReadCaculatedData()
            # NewDf = WithDraw(Df1)
            NewDf = DrawdownAcc(Df1, Df2)
            if NewDf.shape[0] > 0:
                DataToSql(NewDf)
                for date in NewDf.TradeDate.unique().astype('str').tolist():
                    print(date + '导入完成')
        except Exception:
            print("error")
            pass
