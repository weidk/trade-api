from heads import *

# 读取当天的矩阵信息
def QueryLatestMat():
    Df = pd.read_sql("select name,cdf from CDFMatView ",EngineIS)
    Df = Df[Df.name.str.contains(r'企业债\(AAA\)@10.0')==False]
    Df = Df[Df.name.str.contains(r'企业债\(AAA\)@7.0')==False]
    Df = Df[Df.name.str.contains(r'地方债\(AAA\)@10.0')==False]
    Df = Df[Df.name.str.contains(r'地方债\(AAA\)@7.0')==False]
    Df = Df[Df.name.str.contains(r'城投债\(AAA\)@10.0')==False]
    Df = Df[Df.name.str.contains(r'城投债\(AAA\)@7.0')==False]
    Df = Df[Df.name.str.contains(r'城投债\(AAA\)@1.0')==False]
    Df.cdf = Df.cdf.astype('int')
    # A = Df.name.str.split('-', expand=True)
    # NameDf = pd.DataFrame(pd.concat([A[0], A[1]]).unique())
    # NameDf[0].to_json(orient='values')
    return Df.to_json(orient='records')

# 读取相对价值数据
def ReadRelativePrice(startday):
    Df = pd.read_sql("select  * from relativepricecomparison where 日期>'"+startday+"'  order by 日期",EngineIS)
    ReletiveDf = pd.DataFrame()
    for c in Df.columns:
        if c=='日期':
            pass
        else:
            tempDf = Df.ix[:,['日期',c]]
            tempDf['类型'] = c
            tempDf.columns = ['日期', '相对价值', '类型']
            ReletiveDf = ReletiveDf.append(tempDf, ignore_index=True)
    ReletiveDf.日期 = ReletiveDf.日期.astype('str')
    ReletiveDf = ReletiveDf.round(2)
    ReletiveDf.类型 = ReletiveDf.类型.str.replace('中债', '').str.replace('到期收益率', '')
    RCredit = ReletiveDf[ReletiveDf.类型.str.contains('中短期票据')]
    RCredit = RCredit[RCredit.类型.str.contains('1年|3年|5年')]
    RankDf = ReletiveDf[ReletiveDf.日期==ReletiveDf.日期.max()].sort('相对价值')

    return json.dumps({
        '国债':ReletiveDf[ReletiveDf.类型.str.contains('国债')].to_dict(orient='records'),
        '地方政府债':ReletiveDf[ReletiveDf.类型.str.contains('地方政府债')].to_dict(orient='records'),
        '国开':ReletiveDf[ReletiveDf.类型.str.contains('国开')].to_dict(orient='records'),
        '进出口':ReletiveDf[ReletiveDf.类型.str.contains('进出口')].to_dict(orient='records'),
        '农发':ReletiveDf[ReletiveDf.类型.str.contains('农发')].to_dict(orient='records'),
        '中票短融AAA':RCredit[RCredit.类型.str.contains('(AAA)',regex=False)].to_dict(orient='records'),
        '中票短融AAplus':RCredit[RCredit.类型.str.contains('(AA+)',regex=False)].to_dict(orient='records'),
        '中票短融AA':RCredit[RCredit.类型.str.contains('(AA)',regex=False)].to_dict(orient='records'),
        '中票短融1年':RCredit[RCredit.类型.str.contains('1年')].to_dict(orient='records'),
        '中票短融3年':RCredit[RCredit.类型.str.contains('3年')].to_dict(orient='records'),
        '中票短融5年':RCredit[RCredit.类型.str.contains('5年')].to_dict(orient='records'),
        '利率互换':ReletiveDf[ReletiveDf.类型.str.contains('利率互换')].to_dict(orient='records'),
        '一年': ReletiveDf[ReletiveDf.类型.str.contains('1年')].to_dict(orient='records'),
        '三年': ReletiveDf[ReletiveDf.类型.str.contains('3年')].to_dict(orient='records'),
        '五年': ReletiveDf[ReletiveDf.类型.str.contains('5年')].to_dict(orient='records'),
        '七年': ReletiveDf[ReletiveDf.类型.str.contains('7年')].to_dict(orient='records'),
        '十年': ReletiveDf[ReletiveDf.类型.str.contains('10年')].to_dict(orient='records'),
        '排序':RankDf.to_dict(orient='records'),
        'all': ReletiveDf.to_dict(orient='records'),
    })


# 读取绝对价值数据
def ReadAbsolutePrice(startday,endday):
    Df = pd.read_sql("select  * from VarietiesYield where 日期>='"+startday+"'     and 日期<='"+endday+"'   order by 日期",EngineIS)
    AbsoluteDf = pd.DataFrame()
    for c in Df.columns:
        if c=='日期':
            pass
        else:
            tempDf = Df.ix[:,['日期',c]]
            tempDf['类型'] = c
            tempDf.columns = ['日期', '收益率', '类型']
            AbsoluteDf = AbsoluteDf.append(tempDf, ignore_index=True)
    AbsoluteDf.日期 = AbsoluteDf.日期.astype('str')
    AbsoluteDf = AbsoluteDf.round(2)
    AbsoluteDf.类型 = AbsoluteDf.类型.str.replace('中债', '').str.replace('到期收益率', '')
    return AbsoluteDf.to_json(orient='records')
