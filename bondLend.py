from heads import *
import datetime as d

def bondLendBag(code):
    startDay = "2015-01-01"
    endDay = dh.today2str()
    rawDf = importDataFromSql(code, Engine)
    # 计算新增、待偿还和余额
    plusDf, paybackDf, restDf = calDeatailAmt(rawDf, code)
    restDf = restDf.reset_index(['日期'])
    restDf.columns = ['date', 'amt']
    restDf['date'] = restDf['date'].astype(str)
    return restDf.to_json(orient="records")

def bondLendToExcel(code):
    startDay = "2015-01-01"
    endDay = dh.today2str()
    rawDf = importDataFromSql(code, Engine)
    # 计算新增、待偿还和余额
    plusDf, paybackDf, restDf = calDeatailAmt(rawDf, code)
    restDf = restDf.reset_index(['日期'])
    restDf.columns = ['date', 'amt']
    restDf['date'] = restDf['date'].astype(str)
    restDf.to_excel(code+'.xls')

# 读取原始数据
def importData(fileName):
    rawDf = pd.read_excel(fileName)
    rawDf['日期'] = rawDf['日期'].apply(lambda x: x.date())  # 转换日期格式
    return rawDf

# 从数据库读取数据
def importDataFromSql(code,engine):
    rawDf = pd.read_sql("SELECT * FROM openquery(TEST,'select tradedate 日期,underlyingsymbol 简称, underlyingsecurityid 代码,underlyingqty/100000000 券面总额,securityid 借贷期限 from marketanalysis.CMDSSLMDEALT where underlyingsecurityid = ''"+code+"'' order by tradedate' )",engine)
    rawDf['日期'] = rawDf['日期'].apply(lambda x: x.date())  # 转换日期格式
    rawDf['券面总额'] = rawDf['券面总额'].apply(lambda x: float(x))  # 将字符串券面总额转换为浮点数
    return rawDf

# 获取某券的新增、待偿还、余额
def calDeatailAmt(rawDf,code):
    Df = rawDf[rawDf['代码'] == code]
    plusDf = Df.ix[:, ['日期', '券面总额']].groupby('日期').sum() #计算新增量
    paybackDf = GetMinusAmt(Df)  #计算待偿还
    restDf = getRestAmt(plusDf,paybackDf)
    return plusDf,paybackDf,restDf


# 到期量
def GetMinusAmt(BondDf):
    # 计算偿还日期
    PayBackDate = BondDf.apply(lambda x: x['日期'] + d.timedelta(days=GetDays(x['借贷期限'])), axis=1)
    # 组合DataFrame，并按时间排序
    PayBackDf = pd.concat([PayBackDate, BondDf['券面总额']], axis=1).sort(0)
    return PayBackDf.groupby(0).sum() #日期加总结果

# 获取不同期限对应的天数
def GetDays(TermType):
    switcher = {
        'L001': 1,
        'L007': 7,
        'L014': 14,
        'L021': 21,
        'L1M': 30,
        'L2M': 60,
        'L3M': 90,
        'L4M': 120,
        'L5M': 150,
        'L6M': 180,
        'L7M': 210,
        'L8M': 240,
        'L9M': 270,
        'L10M': 300,
        'L11M': 330,
        'L1Y': 365,
    }
    return switcher.get(TermType,0)


# 计算当日余额
def getRestAmt(plusDf,paybackDf):
    # sum(截止当日增量) - sum(截止当日到期量)
    restDf = pd.DataFrame()
    for dateIndex in plusDf.index:
        try:
            tempPlus = plusDf[plusDf.index <= dateIndex]
            tempPayback = paybackDf[paybackDf.index <= dateIndex]
            tempRest = tempPlus.sum() - tempPayback.sum()
            restDf[dateIndex] = tempRest
        except:
            print(dateIndex)
    return restDf.transpose()


