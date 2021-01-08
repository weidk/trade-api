from heads import *

# 读取曲线估值
def GetCurve(bondtype,date):
    hidenValue = r.get('bondcurve_'+bondtype+date)
    if hidenValue != None:
        return hidenValue
    maxDate = pd.read_sql("select * from openquery(WINDNEW,'select max(trade_days)  from CBondCalendar t  where S_INFO_EXCHMARKET = ''NIB''  and trade_days <''"+date+"'' ')",Engine).ix[0,0]
    ddd = datetime.datetime.strptime(maxDate, "%Y%m%d").date() + datetime.timedelta(days=-1095)
    DfBond = pd.read_sql("select * from openquery(WINDNEW,'select  b.s_info_windcode code,B_ANAL_MATU_CNBD bondterm,B_ANAL_YIELD_CNBD bondyield  from CBONDANALYSISCNBD1 a inner join CBondDescription b on  a.s_info_windcode = b.s_info_windcode  where B_ANAL_MATU_CNBD < 10  and B_ANAL_MATU_CNBD > 1  and a.trade_dt = ''"+maxDate+"''  and b.s_info_name like ''%"+bondtype+"%'' and b.s_info_exchmarket = ''NIB'' and b.B_ISSUE_FIRSTISSUE>=''"+ddd.strftime('%Y%m%d')+"''   and length(b.s_info_windcode) =9    and   b_anal_credibility=''推荐''   and  b_info_subordinateornot = 0  order by B_ANAL_MATU_CNBD')",Engine)
    DfCurve = pd.read_sql("select * from openquery(WINDNEW,'select t.b_anal_curveterm curveterm,t.b_anal_yield curveyield from CBondCurveCNBD t  where  trade_dt = ''"+maxDate+"''  and B_ANAL_CURVETYPE = 2  and B_ANAL_CURVETERM <= 10 and B_ANAL_CURVETERM >= 1  and B_ANAL_CURVENAME  like ''%"+bondtype+"%''  order by B_ANAL_CURVETERM')",Engine)
    Df = pd.DataFrame(DfBond.BONDTERM.append(DfCurve.CURVETERM).unique(),columns=['TERM'])
    Df = Df.sort_values('TERM')
    Df = Df.set_index('TERM')
    DfBond = DfBond.set_index('BONDTERM')
    DfCurve = DfCurve.set_index('CURVETERM')
    Df = Df.join(DfCurve).join(DfBond)
    Df = Df.reset_index()
    Df.columns=['TERM', 'CURVEYIELD', 'CODE', 'BONDYIELD']
    Df.CODE = Df.CODE.fillna('CURVE')
    Df = Df.fillna(0)
    Df['YIELD'] = Df.CURVEYIELD + Df.BONDYIELD
    tempDf = Df[(Df.CURVEYIELD>0) &  (Df.BONDYIELD>0)]
    for tempIndex in tempDf.index:
        Df.set_value(tempIndex,'YIELD',Df.iloc[tempIndex,3])
    return Df.to_json(orient="records")


def GetDeals():
    Df = pd.read_sql("select * from openquery(QB,'select distinct code DEALBONDCODE from QBTRADEHISTORYVIEW t where substr(t.dealtime,0,10) >= to_char(sysdate - 3, ''yyyy-mm-dd'')  ')",Engine)
    Df.DEALBONDCODE = Df.DEALBONDCODE + '.IB'
    return Df.to_json(orient="records")