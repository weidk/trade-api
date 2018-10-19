from heads import *

def Func(item):
    DF = pd.read_excel(item)
    Df = DF.iloc[1:9, 0:13]
    Df.columns = ['策略', '总盈亏', '票息盈亏', '净价盈亏', '较上日盈亏', '加权占资', '持有期收益率',
       '年化收益率', '融资成本', '净盈亏', '加权本金', '净年化收益率', '基点价值']
    Df['Date'] = item[6:14]
    lastBp = pd.read_sql("select top "+str(Df.shape[0])+" 策略,基点价值 oldbp from marketmaker.dbo.dailyreport where date < '" + item[6:14] + "' order by date desc",Engine)
    Df = pd.merge(Df,lastBp)
    Df['基点价值变动'] = Df['基点价值'] - Df['oldbp']
    Df = Df.ix[:,['策略', '总盈亏', '票息盈亏', '净价盈亏', '较上日盈亏', '加权占资', '持有期收益率', '年化收益率', '融资成本',
       '净盈亏', '加权本金', '净年化收益率', '基点价值', 'Date', '基点价值变动']]
    try:
        pd.read_sql_query("delete from DailyReport where Date = '" + item[6:14] + "'", con=Engine)
    except:
        pass

    Df.to_sql('DailyReport', Engine, if_exists='append', index=False, index_label=Df.columns,dtype={'Date': sqlalchemy.DateTime,'策略':sqlalchemy.String})




# --------  连接数据库   -----------
Engine = DB.getEngine('192.168.87.73', 'sa', 'tcl+nftx', 'MarketMaker')
# --------- 读取excel数据并导入数据库   --------------
Listfile = os.listdir()
for item in Listfile:
    try:
        if item.endswith('.xls'):
            Func(item)
    except:
        print(item)