from heads import *

# 获取区间的隐含评级
def QueryTermHidenCredit(start,end):
    Df = pd.read_sql("select * from openquery(WINDNEW,'select S_INFO_NAME name, a.s_info_windcode code,B_INFO_ISSUER issuer, oldcredit, day1, newcredit, day2   ,d.B_ANAL_YIELD_CNBD EYIELD,d.B_ANAL_MATU_CNBD RESTYEAR  from (select t.s_info_windcode, t.cnbd_creditrating oldcredit, t.trade_dt  day1,  b.cnbd_creditrating newcredit, b.trade_dt   day2 from CBondCurveMembersCNBD t inner join (select s_info_windcode, cnbd_creditrating, trade_dt from CBondCurveMembersCNBD b  where trade_dt = ''"+end+"'') b on t.s_info_windcode = b.s_info_windcode   where t.trade_dt = ''"+start+"''  and t.cnbd_creditrating <> b.cnbd_creditrating) a inner join CBondDescription c   on a.s_info_windcode = c.s_info_windcode    inner join CBondAnalysisCNBD d on a.s_info_windcode = d.s_info_windcode   where d.trade_dt = ''"+end+"'' ')",Engine)
    return Df

# 计算评级调整
def ToScore(credit):
    score={
        'AAA+': 17,
        'AAA': 16,
        'AAA-': 15,
        'AA+': 14,
        'AA': 13,
        'AA(2)': 12,
        'AA-': 11,
        'A+': 10,
        'A': 9,
        'A-': 8,
        'BBB+': 7,
        'BBB': 6,
        'BB': 5,
        'B': 4,
        'CCC': 3,
        'CC': 2,
        'C': 1,
    }
    return score.get(credit,0)
def ToChange(c1,c2):
    m = ToScore(c2)-ToScore(c1)
    if m<0:
        return '下调'
    else:
        return '上调'
def CalCreditChange(Df):
    Df['change'] = Df.apply(lambda x:ToChange(x['OLDCREDIT'],x['NEWCREDIT']),axis=1)
    return Df.sort_values('ISSUER')

# 找到最近有数据的交易日
def FindTradeDay(day):
    dayKey = day
    dayValue = r.get(dayKey)
    if dayValue==None:
        dayDf = pd.read_sql("select * from openquery(WINDNEW,'select  max(t.trade_dt) td from CBondCurveMembersCNBD t where trade_dt<=''"+day+"'' ')",Engine)
        r.set(dayKey, dayDf.ix[0,0])
        return dayDf.ix[0,0]
    else:
        return dayValue


def HidenCreditBag(start,end):
    start = FindTradeDay(start)
    end = FindTradeDay(end)
    if start == end:
        import datetime
        start = (datetime.datetime.strptime(start, "%Y%m%d").date() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        start = FindTradeDay(start)
    hidenKey = start + '-' + end
    hidenValue = r.get(hidenKey)
    if hidenValue==None:
        Df = QueryTermHidenCredit(start, end)
        if Df.shape[0]==0:
            Rst = Df
            return Rst.to_json(orient="records")
        else:
            Rst = CalCreditChange(Df)
            hidenValue = Rst.to_json(orient="records")
            r.set(hidenKey, hidenValue)
            return hidenValue
    else:
        return hidenValue

# ----------------   隐含评级调整历史  ---------------------
def QueryHidenCreditHistory(code):
    HistoryDf = pd.read_sql("select * from openquery(WINDNEW,'select b.s_info_windcode code, a.tddate,  a.cnbd_creditrating HIDENCREDIT,  b.B_ANAL_YIELD_CNBD EYIELD from (select min(t.trade_dt) tddate, t.cnbd_creditrating  from CBondCurveMembersCNBD t  where t.s_info_windcode = ''"+code+"''  and length(t.cnbd_creditrating) > 0  group by t.cnbd_creditrating  order by tddate) a inner join CBondAnalysisCNBD  b  on a.tddate = b.trade_dt where b.s_info_windcode = ''"+code+"''   and b.B_ANAL_CREDIBILITY=''推荐'' ')",Engine)
    HistoryDf = HistoryDf.sort_values('TDDATE')
    return HistoryDf

# 计算上下调整
def Change(minscore):
    if minscore < 0:
        return '下调'
    elif minscore > 0:
        return '上调'
    else:
        return '--'

def CalChange(HistoryDf):
    HistoryDf['CREDITSCORE'] = HistoryDf['HIDENCREDIT'].apply(ToScore)
    HistoryDf['CREDITSCORE1'] = HistoryDf['CREDITSCORE'].shift(1)
    HistoryDf['CHANGESCORE'] = HistoryDf['CREDITSCORE'] - HistoryDf['CREDITSCORE1']
    HistoryDf['CHANGE'] = HistoryDf['CHANGESCORE'].apply(Change)
    return HistoryDf.ix[:,['CODE', 'TDDATE', 'HIDENCREDIT', 'EYIELD','CHANGE']]

def CreditChangeHistoryBag(code):
    HistoryDf = QueryHidenCreditHistory(code)
    RstDf = CalChange(HistoryDf)
    return RstDf.to_json(orient="records")