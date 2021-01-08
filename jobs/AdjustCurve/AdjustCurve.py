# 计算等效收益率曲线脚本
import pandas as pd
import numpy as np
import pymssql
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import warnings
warnings.simplefilter(action = "ignore", category = Warning)


def getEngine(host,user,password,db,port='1433'):
    engine = create_engine('mssql+pymssql://'+user+':'+password+'@'+host+':'+port+'/'+db)
    return engine

EngineIS = getEngine('10.28.7.43', 'bond', 'bond', 'InvestSystem')

def ReadParas():
    Df = pd.read_excel('参数.xlsx')
    return Df

def UpdateYield(Df,DateString):
    CurveNumList = ("''"+Df.曲线编号.astype('str')+"'',").sum()
    CurveNumList = "("+CurveNumList[:-1]+")"
    DfYield = pd.read_sql("select * from openquery(WINDNEW,'select b_anal_curvenumber,b_anal_curveterm,b_anal_yield from CBONDCURVECNBD t where  t.B_ANAL_CURVETYPE = 2  and b_anal_curvenumber in "+CurveNumList+" and trade_dt = ''"+DateString+"'' and  t.b_anal_curveterm in (1,3,5,7,10,30) ')",EngineIS)
    if DfYield.shape[0]>0:
        Df['曲线编号'] = Df['曲线编号'].astype('float')
        Df['期限'] = Df['期限'].astype('float')
        def calYield(number,term,p1,p2):
            try:
                y = DfYield[(DfYield.B_ANAL_CURVENUMBER==number) & (DfYield.B_ANAL_CURVETERM==term)].B_ANAL_YIELD.iloc[0]
                y = y - p1*100 - y*p2
                return round(y,4)
            except:
                return 0
        Df['adjYield'] = Df.apply(lambda x:calYield(x.曲线编号,x.期限,x.占资成本系数,x.税收成本系数),axis = 1)
        Df['TradeDate'] = DateString
        Df.to_sql('AdjYield', EngineIS, if_exists='append', index=False, index_label=Df.columns,
                dtype={'TradeDate': sqlalchemy.String,
                       '原始资产': sqlalchemy.String})
        print(DateString)



if __name__ == '__main__':
        try:
            Df = ReadParas()
            date1 = pd.read_sql('select max(TradeDate) date from AdjYield',EngineIS).ix[0,0]
            date2 = pd.read_sql("select * from openquery(WINDNEW,'select max(trade_dt) from CBONDCURVECNBD t  ')",EngineIS).ix[0,0]
            dateList = pd.date_range(date1, date2,closed='right')
            for date in dateList:
                DateString = date.date().strftime("%Y%m%d")
                UpdateYield(Df, DateString)
        except Exception:
            print("error")
            pass