from heads import *

# 读取做做市量和点差、期限分数及总分
def CalFinalScore(initiator,date):
    BPDF = pd.read_sql("select * from openquery(TEST,'select bondcode,floor(avg(t.buyyield-t.sellyield)*100) as bp from marketanalysis.cmdscbmmarketmakerquote  t where to_char(quotedatetime, ''yyyy-mm-dd'') = ''"+date+"'' and bondname like ''%国开%''  and t.initiator = ''"+initiator+"''  group by bondcode order by bondcode')",Engine)
    AMTDF = pd.read_sql("select * from openquery(TEST,'select bondcode, (avg(t.buytotalfacevalue)/100000000 +avg(t.selltotalfacevalue)/100000000) amt  from marketanalysis.cmdscbmmarketmakerquote  t where to_char(quotedatetime, ''yyyy-mm-dd'') = ''"+date+"'' and bondname like ''%国开%''  and t.initiator = ''"+initiator+"''  group by bondcode order by bondcode')",Engine)
    # 计算点差分数
    def switchSoore(bp):
        if bp<=5:
            score = 2
        elif bp<=10:
            score = 1.3
        elif bp<=15:
            score = 1
        else:
            score = 0
        return score
    BPDF['score'] = BPDF.BP.astype(float).apply(switchSoore)

    # 计算期限分数
    def getTermscore(code):
        df = pd.read_sql("select term from [172.18.3.42,14331].[msnRobot].[dbo].[NewBond]  where code = '"+code+"'",Engine)
        term = df.ix[0,0]
        if term<=1:
            termScore = 1
        elif term<=3:
            termScore = 1.5
        elif term<=10:
            termScore = 2.5
        else:
            termScore = 3
        return termScore
    BPDF['termScore'] = BPDF.BONDCODE.apply(getTermscore)
    # 合并Dataframe
    AMTDF.set_index(['BONDCODE'], inplace = True)
    BPDF.set_index(['BONDCODE'], inplace = True)
    DF = pd.concat([BPDF, AMTDF], axis=1)
    DF.AMT = DF.AMT.astype(float)
    DF['total'] = DF.AMT*DF.score*DF.termScore
    FinalScore = DF.total.sum()
    ScoreDF = pd.DataFrame([[initiator, FinalScore, date]], columns=['initiator', 'score', 'date'])
    return ScoreDF,DF


# 写入数据库
def ScoreToSql(ScoreDF):
    ScoreDF.to_sql('MarketScore', EngineIS, if_exists='append', index=False, index_label=ScoreDF.columns,
                    dtype={'date': sqlalchemy.DateTime, 'initiator': sqlalchemy.String})
# 将当日的机构全部计算写入数据库
def AllToSql(date):
    initiatorList = ['东海证券','东方证券','中信证券','中信建投证券','第一创业证券']
    for i in initiatorList:
        ScoreDF,DF = CalFinalScore(i, date)
        ScoreToSql(ScoreDF)

# 将未计算分数的交易日计算并写入数据库
def MarketScoreBag():
    # 获取已计算最大交易日
    maxDate = pd.read_sql("select max(date) from MarketScore",EngineIS)
    maxday = maxDate.astype(str).ix[0, 0]
    # 获取未计算的交易日
    DS = pd.read_sql("select * from openquery(TEST,'select distinct(to_char(quotedatetime, ''yyyy-mm-dd'')) ds from  marketanalysis.cmdscbmmarketmakerquote where quotedatetime >''"+maxday+"''')",Engine)
    TimeToCal = DS[DS.DS > maxday].sort(['DS'])
    if TimeToCal.shape[0]>0:
        for d in TimeToCal.DS:
            AllToSql(d)


MarketScoreBag()