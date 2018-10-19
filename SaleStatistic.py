from heads import *

# ----------------------------   获配额统计   ---------------------------
# 读取销售所有数据
def QueryRawSales(isPfrofit=False):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份, sum(t.allocationamt)/10000 获配额 from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''2015-01-01'' group by to_char(t.saledate, ''yyyy-mm'')   order by 月份')",Engine)
    Rst = ClassifyData(Df)
    return Rst
    # RawDf = pd.DataFrame()
    # for i in range(2015,datetime.datetime.today().year+1):
    #     tempDf = Df[(Df['月份']>=str(i)) & (Df['月份']<str(i+1))]
    #     tempDf.columns=[str(i),str(i)+'年']
    #     tempDf.index = list(range(tempDf.shape[0]))
    #     tempDf[str(i) + '年'] = tempDf[str(i) + '年'].astype('float')
    #     RawDf = pd.concat([RawDf,tempDf[str(i)+'年']],axis=1)
    # RawDf = RawDf.T
    # RawDf.columns = ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月']
    # RawDf = RawDf.reset_index()
    # return RawDf.to_json(orient="records")

# 读取销售利率债数据
def QueryInterestSales(isPfrofit=False):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份, sum(t.allocationamt)/10000 获配额 from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''2015-01-01''   and category in (''国债'',''金融债'')   group by to_char(t.saledate, ''yyyy-mm'')   order by 月份')",Engine)
    Rst = ClassifyData(Df)
    return Rst

# 读取销售信用债数据
def QueryCreditSales(isPfrofit=False):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份, sum(t.allocationamt)/10000 获配额 from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''2015-01-01''   and category not in (''国债'',''金融债'')   group by to_char(t.saledate, ''yyyy-mm'')   order by 月份')",Engine)
    Rst = ClassifyData(Df)
    return Rst

# 整理数据
def ClassifyData(Df):
    RawDf = pd.DataFrame()
    for i in range(2015, datetime.datetime.today().year + 1):
        tempDf = Df[(Df['月份'] >= str(i)) & (Df['月份'] < str(i + 1))]
        tempDf.columns = [str(i), str(i) + '年']
        tempDf.index = list(range(tempDf.shape[0]))
        tempDf[str(i) + '年'] = tempDf[str(i) + '年'].astype('float')
        RawDf = pd.concat([RawDf, tempDf[str(i) + '年']], axis=1)
    RawDf = RawDf.T
    RawDf.columns = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
    RawDf = RawDf.reset_index()
    return RawDf.to_json(orient="records")

# ----------------------------   交易员排名   ---------------------------
def QureyTraderAll(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0  group by username order by amt desc')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

def QureyTraderInterest(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0     and category in (''国债'',''金融债'')    group by username order by amt desc')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

def QureyTraderCredit(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0     and category not in (''国债'',''金融债'')    group by username order by amt desc')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

# ----------------------------   交易员属性   ---------------------------
def QueryTraderProps(start,end,tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select category ,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0  and username=''"+tradername+"''   group by category order by amt desc')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

# ----------------------------   机构排名   ---------------------------
def QureyInstituteAll(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select * from ( select t.organizationname  username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0  group by organizationname  order by amt desc ) where rownum<=50')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

def QureyInstituteInterest(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select * from ( select t.organizationname  username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0     and category in (''国债'',''金融债'')    group by organizationname  order by amt desc ) where rownum<=50')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

def QureyInstituteCredit(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select * from ( select  t.organizationname  username,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0     and category not in (''国债'',''金融债'')    group by  organizationname  order by amt desc ) where rownum<=50')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

# ----------------------------   机构属性   ---------------------------
def QueryInstituteProps(start,end,tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select category ,sum(t.allocationamt)/10000 amt  from ibond.dsc_bondsaleprofit t where to_char(t.saledate, ''yyyy-mm-dd'') >= ''"+start+"'' and  to_char(t.saledate, ''yyyy-mm-dd'') <= ''"+end+"'' and allocationamt>0  and  organizationname =''"+tradername+"''   group by category order by amt desc')",Engine)
    Df['AMT'] = Df['AMT'].astype('float')
    return Df.to_json(orient="records")

# ----------------------------   交易员认购量走势   ---------------------------
# 利率
def QueryTraderInterestTs(tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份,sum(t.allocationamt) / 10000 获配额  from ibond.dsc_bondsaleprofit t where  allocationamt>0  and username=''"+tradername+"''      and category in (''国债'',''金融债'')    group by to_char(t.saledate, ''yyyy-mm'')  order by 月份 ')",Engine)
    Df['获配额'] = Df['获配额'].astype('float')
    Rst = ClassifyTraderData(Df)
    return Rst

# 信用
def QueryTraderCreditTs(tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份,sum(t.allocationamt) / 10000 获配额  from ibond.dsc_bondsaleprofit t where  allocationamt>0  and username=''"+tradername+"''      and category not in (''国债'',''金融债'')    group by to_char(t.saledate, ''yyyy-mm'')  order by 月份 ')",Engine)
    Df['获配额'] = Df['获配额'].astype('float')
    Rst = ClassifyTraderData(Df)
    return Rst

# ----------------------------   机构认购量走势   ---------------------------
# 利率
def QueryInsInterestTs(tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份,sum(t.allocationamt) / 10000 获配额  from ibond.dsc_bondsaleprofit t where  allocationamt>0  and organizationname=''"+tradername+"''      and category in (''国债'',''金融债'')    group by to_char(t.saledate, ''yyyy-mm'')  order by 月份 ')",Engine)
    Df['获配额'] = Df['获配额'].astype('float')
    Rst = ClassifyTraderData(Df)
    return Rst

# 信用
def QueryInsCreditTs(tradername):
    Df = pd.read_sql("select * from openquery(IBOND,'select to_char(t.saledate, ''yyyy-mm'') 月份,sum(t.allocationamt) / 10000 获配额  from ibond.dsc_bondsaleprofit t where  allocationamt>0  and organizationname=''"+tradername+"''      and category not in (''国债'',''金融债'')    group by to_char(t.saledate, ''yyyy-mm'')  order by 月份 ')",Engine)
    Df['获配额'] = Df['获配额'].astype('float')
    Rst = ClassifyTraderData(Df)
    return Rst


# 整理数据
def ClassifyTraderData(Df):
    RawDf = pd.DataFrame()
    RawDf['ind'] = list(range(1, 13))
    RawDf.index = RawDf['ind']
    for i in range(pd.to_datetime(Df.ix[0,0]).year, datetime.datetime.today().year + 1):
        tempDf = Df[(Df['月份'] >= str(i)) & (Df['月份'] < str(i + 1))]
        if tempDf.shape[0]==0:
            continue
        tempDf.index = pd.to_datetime(tempDf['月份']).dt.month
        tempDf.columns = [str(i), str(i) + '年']
        tempDf[str(i) + '年'] = tempDf[str(i) + '年'].astype('float')
        RawDf = pd.concat([RawDf, tempDf[str(i) + '年']], axis=1)
    RawDf = RawDf.drop(['ind'], axis=1)
    RawDf = RawDf.T
    RawDf.columns = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
    RawDf = RawDf.reset_index()
    return RawDf.to_json(orient="records")