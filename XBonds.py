from heads import *

# 读取原始数据
def QueryXBond():
    Df = pd.read_sql("select * from openquery(TEST1,'select * from XBONDREPORT_VTY t where t.selftradername in (''施晓乐'', ''郭圣雨'',''朱天启'',''dhztq'',''shixiaole'',''dhgsyu'')  order by tradedate')",EngineIS)
    # Df = pd.read_sql("select * from openquery(TEST1,'select * from XBONDREPORT_VTY t where TRADEDATE>=''2018-01-01''  order by tradedate')",EngineIS)
    Df.SELFTRADERNAME = Df.SELFTRADERNAME.str.replace('dhztq', '朱天启')
    Df.SELFTRADERNAME = Df.SELFTRADERNAME.str.replace('shixiaole', '施晓乐')
    Df.SELFTRADERNAME = Df.SELFTRADERNAME.str.replace('dhgsyu', '郭圣雨')
    Df.PROFIT = Df.PROFIT.astype('float')
    Df.AMT = Df.AMT.astype('float')
    Df = Df[(Df.PROFIT < 10000000) & (Df.PROFIT > -10000000)]
    Df.PROFIT = round(Df.PROFIT/10000,2)
    if datetime.datetime.now().hour<17:
        Df = Df[Df.TRADEDATE<dh.today2str()]
    grouped = Df.groupby('SELFTRADERNAME')
    Df['CUMSUMPROFIT'] = grouped['PROFIT'].cumsum()
    Df['CUMSUMAMT'] = grouped['AMT'].cumsum()
    return Df.to_json(orient="records")