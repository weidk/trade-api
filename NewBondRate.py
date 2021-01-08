from heads import *
# db.getCollection('data_bond').find({"data.name":"18青海盐湖SCP001"}, {"_id":0,"data.name": 1, "data.maturity": 1, "data.rate": 1,"data.rank1":1 } )
# 获取期限内所有数据
def ReadNewbondData(start,end):
    NewBondDf = pd.read_sql("SELECT maturity , [rate],  [rank1] FROM [172.18.3.42,14331].[msnRobot].[dbo].[NewBond] where issueDay >='"+start+"'   and  issueDay <='"+end+"'  and market = '银行间债券' and category in ('中票','短融','超短融') and rate !='NULL' ",Engine)
    NewBondDf.rate = NewBondDf.rate.astype(float)
    NewBondDf.maturity = NewBondDf.maturity.str.split('+').str.get(0)
    NewBondDf.maturity = NewBondDf.maturity.astype(float)
    NewBondDf = NewBondDf.sort_values('maturity')
    def ChangeTerm(t):
        term  = ''
        if t< 0.25:
            # term = '90D'
            term = 0.25
        elif t>0.27 and t< 0.55 :
            # term = '180D'
            term = 0.5
        elif t>0.55 and t< 0.83 :
            # term = '270D'
            term = 0.75
        elif t==1 :
            # term = '1Y'
            term = 1
        elif t==2 :
            # term = '2Y'
            term = 2
        elif t==3 :
            # term = '3Y'
            term = 3
        elif t==4 :
            # term = '4Y'
            term = 4
        elif t==5 :
            # term = '5Y'
            term = 5
        return term
    NewBondDf.maturity = NewBondDf.maturity.apply(ChangeTerm)
    NewBondDf = NewBondDf.dropna()
    try:
        NewBondDf = NewBondDf[NewBondDf.maturity != '']
    except:
        pass
    return NewBondDf

# 分类别整理出极值点
def GetAreaData(RawNewBondDf):
    NewBondDf = RawNewBondDf.ix[:,['rank1', 'rate', 'maturity']]
    GroupedDf = NewBondDf.groupby(['maturity', 'rank1'])
    RstDf = pd.concat([GroupedDf.max(),GroupedDf.min()],axis=1)
    RstDf.columns = ['max','min']
    RstDf =  RstDf.reset_index()
    RstDf['range'] = RstDf.apply(lambda x:[x['max'],x['min']],axis=1)
    RstDf['name'] = RstDf.apply(lambda x: [RawNewBondDf[(RawNewBondDf['rate']==x['max']) & (RawNewBondDf['rank1']==x['rank1']) & (RawNewBondDf['maturity']==x['maturity'])].iloc[0,0],RawNewBondDf[(RawNewBondDf['rate']==x['min']) & (RawNewBondDf['rank1']==x['rank1']) & (RawNewBondDf['maturity']==x['maturity'])].iloc[0,0]], axis=1)
    RstDf = RstDf.ix[:, ['maturity', 'rank1', 'range','name']]
    return RstDf
    # RstDf.to_excel('123.xls')

# 输入起始日期和结束日期，计算绘图数据
def GetIssueBag(start,end):
    # NewBondDf = ReadNewbondData(start,end)
    RawNewBondDf = ReadNewbondFromMongo(start,end)
    RstDf = GetAreaData(RawNewBondDf)
    RstDf.columns = ['term','rank','yield','bondname']
    return RstDf.to_json(orient="records")

#从mongodb获取数据
def ReadNewbondFromMongo(start,end):
    cursor = crmDb['data_bond'].find({"data.issueDay":{ "$gte" : start, "$lte" : end},"data.market":"银行间债券","data.category":{"$in":['中票','短融','超短融']},"data.rate":{"$exists":1}}, {"_id":0, "data.term": 1,"data.rank1":1 , "data.rate": 1, "data.name":1})
    list_cursor = list(cursor)
    lc = []
    for lt in list_cursor:
        lc.append(lt['data'])
    NewBondDf = pd.DataFrame(lc)
    NewBondDf = NewBondDf.drop(NewBondDf[NewBondDf.rate == ''].index, axis=0)
    NewBondDf.columns = ['name','rank1', 'rate', 'maturity']
    NewBondDf.rate = NewBondDf.rate.astype(float)
    # NewBondDf.maturity = NewBondDf.maturity.str.split('+').str.get(0)
    # NewBondDf.maturity = NewBondDf.maturity.astype(float)
    NewBondDf = NewBondDf.sort_values('maturity')

    def ChangeTerm(t):
        term = ''
        if t < 0.25:
            # term = '90D'
            term = 0.25
        elif t > 0.27 and t < 0.55:
            # term = '180D'
            term = 0.5
        elif t > 0.55 and t < 0.83:
            # term = '270D'
            term = 0.75
        elif t == 1:
            # term = '1Y'
            term = 1
        elif t == 2:
            # term = '2Y'
            term = 2
        elif t == 3:
            # term = '3Y'
            term = 3
        elif t == 4:
            # term = '4Y'
            term = 4
        elif t == 5:
            # term = '5Y'
            term = 5
        return term

    NewBondDf.maturity = NewBondDf.maturity.apply(ChangeTerm)
    NewBondDf = NewBondDf.dropna()
    try:
        NewBondDf = NewBondDf[NewBondDf.maturity != '']
    except:
        pass
    return NewBondDf

# 资质指数
def QualificationIndex(start,end):
    Df = pd.read_sql("select updatetime td_date,qualification  from QualificationIndex where updatetime>='"+start+"' and updatetime<='"+end+"' order by updatetime",EngineIS)
    Df.td_date = Df.td_date.astype('str')
    return Df.to_json(orient="records")

# 情绪指数
def EmotionIndex(start,end):
    Df = pd.read_sql("select updatetime td_date,emotion,AAA9MYield  from EmotionIndex where updatetime>='"+start+"' and updatetime<='"+end+"'   and  emotion>0 order by updatetime",EngineIS)
    Df.td_date = Df.td_date.astype('str')
    Df.AAA9MYield = Df.AAA9MYield*(-1)
    Df.rename(columns={'emotion': '情绪指数','AAA9MYield': '中债收益率曲线_AAA_9M'}, inplace=True)
    return Df.to_json(orient="records")

# 异常倍数
def AbnormalNumber(start,end):
    Df = pd.read_sql("select * from openquery(IBOND,'select name,fullmultiple from ibond.view_dh  where category in (''超短融'',''短期融资券'',''中期票据'')and issueday <= ''"+end+"'' and paymentday >=''"+start+"''  and fullmultiple>4  ')",EngineIS)
    return Df.to_json(orient="records")