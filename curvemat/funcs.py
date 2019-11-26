from heads import *

# 读取整理原始数据
def QueryRawCurve(date):
    Df1 = pd.read_sql("select * from openquery(WIND,'select t.b_anal_curvename CURVE, t.b_anal_curveterm TERM, B_ANAL_YIELD YIELD from CBondCurveCNBD t   where t.trade_dt = ''"+date+"'' and t.b_anal_curvetype = 2 and b_anal_curveterm in (1, 3, 5, 7, 10)  and b_anal_curvename in (''中债城投债收益率曲线(AA(2))'',''中债城投债收益率曲线(AA)'', ''中债城投债收益率曲线(AA+)'',''中债城投债收益率曲线(AAA)'', ''中债地方政府债收益率曲线(AAA)'',''中债地方政府债收益率曲线(AAA-)'',''中债国开债收益率曲线'',''中债国债收益率曲线'', ''中债进出口行债收益率曲线'', ''中债农发行债收益率曲线'',''中债企业债收益率曲线(AA)'',''中债企业债收益率曲线(AA+)'',''中债企业债收益率曲线(AAA)'', ''中债企业债收益率曲线(AAA-)'', ''中债铁道债收益率曲线'')  order by b_anal_curvename, b_anal_curveterm')",Engine)
    # Df1 = pd.read_sql("select * from openquery(WIND,'select t.b_anal_curvename CURVE, t.b_anal_curveterm TERM, B_ANAL_YIELD YIELD from CBondCurveCNBD t   where t.trade_dt = ''"+date+"'' and t.b_anal_curvetype = 2 and b_anal_curveterm in (1, 3, 5, 7, 10)  and b_anal_curvename in ( ''中债城投债收益率曲线(AAA)'', ''中债地方政府债收益率曲线(AAA)'',''中债国开债收益率曲线'',''中债国债收益率曲线'',  ''中债农发行债收益率曲线'',''中债企业债收益率曲线(AAA)'')  order by b_anal_curvename, b_anal_curveterm')",Engine)
    Df2 = pd.read_sql("select * from openquery(WIND,'select t.b_anal_curvename CURVE, t.b_anal_curveterm TERM, B_ANAL_YIELD YIELD from CBondCurveSHC t   where t.trade_dt = ''"+date+"'' and t.b_anal_curvetype = 2 and b_anal_curveterm =0.25  and b_anal_curvename = ''上海清算所固定利率同业存单收益率曲线(AAA)''  order by b_anal_curvename, b_anal_curveterm')",Engine)
    Df3 = pd.read_sql("select * from openquery(WIND,'select CASE s_info_windcode    WHEN ''T.CFE'' THEN  10  WHEN ''TF.CFE'' THEN  5  END TERM  , CASE s_info_windcode  WHEN ''T.CFE'' THEN  3 + (100 - S_DQ_CLOSE) / 8  WHEN ''TF.CFE'' THEN  3 + (100 - S_DQ_CLOSE) / 4 END YIELD   from CBondFuturesEODPrices t  where    t.trade_dt = ''"+date+"'' and   t.s_info_windcode in (''T.CFE'', ''TF.CFE'')   ')",Engine)
    Df3['CURVE'] = '国债期货'
    Df = Df1.append(Df2,ignore_index=True)
    Df = Df.append(Df3,ignore_index=True)
    Df.TERM = Df.TERM.astype('str')
    Df.YIELD = Df.YIELD.astype('float')
    Df['TYPE'] = Df.CURVE+"@"+Df.TERM
    Df.TYPE = Df.TYPE.str.replace('中债', '')
    Df.TYPE = Df.TYPE.str.replace('收益率曲线', '')
    Df.TYPE = Df.TYPE.str.replace('上海清算所固定利率', '')
    Df.TYPE = Df.TYPE.str.replace('上海清算所固定利率', '')
    Df.TYPE = Df.TYPE.str.replace('政府', '')
    RstDf = Df.ix[:,['TYPE','YIELD']]
    return RstDf

# 计算点差
def CalBpGap(RstDf):
    MatDf = pd.DataFrame(columns=['BP'])
    for i in range(0,RstDf.shape[0]-1):
        x = RstDf.iloc[i]
        for j in range(i+1, RstDf.shape[0]):
            y = RstDf.iloc[j]
            MatDf.loc[x.TYPE+'-'+y.TYPE] = int((x.YIELD-y.YIELD)*100)
    MatDf = MatDf.sort_index()
    MatDf = MatDf.reset_index()
    return MatDf

# 计算百分比
def CalPercent(MatDf):
    HisDf = pd.read_sql("select * from MatTs",Engine)
    def cal(x):
        try:
            kde = st.kde.gaussian_kde(HisDf[HisDf['index'] == x['index']].BP)
            return kde.integrate_box_1d(-np.inf, x.BP)
        except:
            return np.nan
    MatDf['CDF'] = MatDf.apply(cal,axis=1)
    return MatDf


# 写入数据库
def WriteToDb(RstDf,MatDf,date):
    RstDf['Date'] = date
    MatDf['Date'] = date
    RstDf.to_sql('CurveTs', Engine, if_exists='append', index=False, index_label=RstDf.columns,dtype={'Date': sqlalchemy.String,'TYPE':sqlalchemy.String})
    MatDf.to_sql('MatTs', Engine, if_exists='append', index=False, index_label=RstDf.columns,dtype={'Date': sqlalchemy.String,'index':sqlalchemy.String})

# 写入数据库
def WriteToDbFuture(RstDf,MatDf,date):
    RstDf['Date'] = date
    MatDf['Date'] = date
    RstDf = RstDf[RstDf.TYPE.str.contains('期货')]
    MatDf = MatDf[MatDf['index'].str.contains('期货')]
    RstDf.to_sql('CurveTs', Engine, if_exists='append', index=False, index_label=RstDf.columns,dtype={'Date': sqlalchemy.String,'TYPE':sqlalchemy.String})
    MatDf.to_sql('MatTs', Engine, if_exists='append', index=False, index_label=RstDf.columns,dtype={'Date': sqlalchemy.String,'index':sqlalchemy.String})


def CurveBag(date):
    try:
        RstDf = QueryRawCurve(date)
        MatDf = CalBpGap(RstDf)
        MatDf = CalPercent(MatDf)
        WriteToDb(RstDf, MatDf, date)
        print(date+'-success')
    except:
        print(date + '-error')

def CurveBagFuture(date):
    try:
        RstDf = QueryRawCurve(date)
        MatDf = CalBpGap(RstDf)
        # MatDf = CalPercent(MatDf)
        WriteToDbFuture(RstDf, MatDf, date)
        print(date+'-success')
    except:
        print(date + '-error')