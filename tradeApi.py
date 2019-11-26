from heads import *
from functools import wraps


app = Flask(__name__)

def allow_cross_domain(fun):
    @wraps(fun)
    def wrapper_fun(*args, **kwargs):
        rst = make_response(fun(*args, **kwargs))
        rst.headers['Access-Control-Allow-Origin'] = '*'
        rst.headers['Access-Control-Allow-Methods'] = 'PUT,GET,POST,DELETE'
        allow_headers = "Referer,Accept,Origin,User-Agent"
        rst.headers['Access-Control-Allow-Headers'] = allow_headers
        return rst
    return wrapper_fun

@app.route('/')
def hello_world():
   return 'tradeplateform test page'


# 新增一条待结算持仓
@app.route('/api/postposition', methods=['POST'])
def CreatPosition():
    try:
        data = request.json
        df = json_normalize(data)
        PO.createNewPositon(df)
    except:
        pass

# 查询所有持仓
@app.route('/api/position')
def GetPosition():
    df = PO.getAllPosition()
    return df

# 删除持仓
@app.route('/api/deleteposition',methods=['POST'])
def DeletePosition():
    try:
        data = request.json
        PO.deleteData(data)
    except:
        pass

# --------------------------------------------------------------------------------
# ------------------------ 结算头寸相关 -----------------------------------------
# 新增一条待结算持仓
@app.route('/api/postsettle', methods=['POST'])
def postsettle():
    try:
        data = request.json
        df = json_normalize(data)
        PO.createNewSettle(df)
        return 'success'
    except:
        pass

# 查询所有结算头寸
@app.route('/api/settleposition')
def settleposition():
    df = PO.getSettle()
    return df

# 查询所有结算头寸
@app.route('/api/settlepositionpost',methods=['POST'])
def settlepositionpost():
    df = PO.getSettle(date=request.json)
    return df


# 删除结算头寸
@app.route('/api/deletesettle',methods=['POST'])
def deletesettle():
    try:
        data = request.json
        PO.deleteSettle(data)
        return 'success'
    except:
        return 'fail'

# 查询明日结算金额
@app.route('/api/totalsettle')
def totalsettle():
    try:
        df = PO.getTotalAmt()
        return df
    except:
        pass

# 查询头寸密码
@app.route('/api/positionpsw')
def positionpsw():
    try:
        df = PO.getPSW()
        return df
    except:
        pass

# 查询头寸添加状态
@app.route('/api/positionstatus')
def positionstatus():
    try:
        df = PO.getAllowStatus()
        return df
    except:
        pass

# 查询明日非现券结算头寸
@app.route('/api/nonbondinfo')
def nonbondinfo():
    try:
        df = PO.getNonBond()
        return df
    except:
        pass

# 查询净卖空债券
@app.route('/api/netsellbond')
def netsellbond():
    try:
        df = PO.getNetBond()
        return df
    except:
        pass

# 修改头寸状态
@app.route('/api/changepositionstatus', methods=['POST'])
def changepositionstatus():
    try:
        data = request.json
        df = PO.ChangAllowStatus(data['data'])
        return df
    except:
        pass

@app.route('/api/netsellbondpost', methods=['POST'])
def netsellbondpost():
    try:
        df = PO.getNetBond(date=request.json)
        return df
    except:
        pass

@app.route('/api/nonbondinfopost', methods=['POST'])
def nonbondinfopost():
    try:
        df = PO.getNonBond(date=request.json)
        return df
    except:
        pass


# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------

# 新增一条待提醒事项
@app.route('/api/posttodo', methods=['POST'])
def CreatTodo():
    try:
        data = request.json
        df = json_normalize(data)
        TD.createNewTodo(df)
    except:
        pass

# 查询所有待提醒事项
@app.route('/api/todo')
def GetTodo():
    df = TD.getAllTodo()
    return df

# 删除持仓
@app.route('/api/deletetodo',methods=['POST'])
def DeleteTodo():
    try:
        data = request.json
        TD.deleteTodo(data)
    except:
        pass

# --------------------------------------------------------------------------------
# 一级发行收益率管道图
@app.route('/api/issueyield', methods=['POST'])
def CalIssueYield():
    try:
        date = request.json
        rst = NB.GetIssueBag(date['start'],date['end'])
        return rst
    except:
        pass

# 做市分数
@app.route('/api/marketscores')
def GetMS():
    df = MS.ReadScore()
    return df

# 做市分数
@app.route('/api/creditmarketscores')
def GetCreditMS():
    df = MS.ReadCreditScore()
    return df

# 结算持仓
@app.route('/api/xbondcounter', methods=['POST'])
def GetXBondCounterData():
    try:
        data = request.json
        df = MS.XBondCounter(data['insname'])
        return df
    except:
        pass

# CD持仓
@app.route('/api/cdpositon')
def GetCDPosition():
    df = CD.ReadPosition()
    return df

# BPM持仓
@app.route('/api/bpmpositon')
def GetBPMPosition():
    df = CD.ReadBPMPosition()
    return df

# 结算持仓
@app.route('/api/settledownpositon', methods=['POST'])
def GetSettlePosition():
    try:
        df = CD.QuerySettlePosition(request.json)
        return df
    except:
        pass


# 报表——利润走势图
@app.route('/api/reprotprofit', methods=['POST'])
def GetReportProfit():
    try:
        date = request.json
        df = RC.ReadProfitDF(date)
        return df
    except:
        pass

# 报表——债券类型持仓饼状图
@app.route('/api/reprotpositionpie')
def GetReportPosition():
    df = RC.PositionComposition()
    return df

# 现券市场成交——券种
@app.route('/api/marketdeal', methods=['POST'])
def CalMarketDealAmt():
    try:
        date = request.json
        rst = MD.TrimTypeData(date['start'],date['end'])
        return rst
    except:
        pass

# 现券市场成交走势图
@app.route('/api/marketdealts', methods=['POST'])
def CalMarketDealTs():
    try:
        date = request.json
        rst = MD.QueryTSData(date['ins'], date['type'])
        return rst
    except:
        pass

@app.route('/api/marketdealts1', methods=['POST'])
def CalMarketDealTs1():
    try:
        date = request.json
        rst = MD.QueryTSData1(date['ins'], date['type'])
        return rst
    except:
        pass

# 现券市场成交走势图新版
@app.route('/api/marketdealtsnew', methods=['POST'])
def CalMarketDealTsNew():
    try:
        date = request.json
        rst = MD.QueryTSDataNew(date['ins'], date['type'], date['term'])
        return rst
    except:
        pass

# 现券市场成交——期限
@app.route('/api/markettermdeal', methods=['POST'])
def CalMarketTermDealAmt():
    try:
        date = request.json
        rst = MD.TrimTermData(date['start'],date['end'])
        return rst
    except:
        pass

# 现券市场成交——券种
@app.route('/api/marketdealnew', methods=['POST'])
def CalMarketDealAmtNew():
    try:
        date = request.json
        rst = MD.NewData(date['start'], date['end'])
        return rst
    except:
        pass
# -------------------------------------------------------------------

# 债券借贷
@app.route('/api/bondlend', methods=['POST'])
def GetBondLenddata():
    try:
        code = request.json
        rst = BD.bondLendBag(code)
        return rst
    except:
        pass

# 国开借贷图
@app.route('/api/bondlendgk')
def GetBondLenddataGK():
    try:
        rst = BD.bondLendBagGK()
        return rst
    except:
        pass

# 本方做市成交柱状图
@app.route('/api/marketmakerdeal', methods=['POST'])
def CalMarketMakerdeal():
    try:
        date = request.json
        rst = MS.CalDealAmt(date['start'],date['end'])
        return rst
    except:
        pass

# 实现盈亏和浮动盈亏
@app.route('/api/reportfrprofit')
def GetFRprofit():
    try:
        df = RC.ProfitDistrubution()
        return df
    except:
        pass

# 策略明细
@app.route('/api/reportaccdetail')
def GetAccDetail():
    try:
        df = RC.AccDetail()
        return df
    except:
        pass


# 分债券类型的浮动收益和实现收益
@app.route('/api/reportfrprofitbyType')
def GetFRprofitbyType():
    try:
        df = RC.ProfitDistrubutionGroupbyStype()
        return df
    except:
        pass

# 查询成交
@app.route('/api/secondmarketdeal', methods=['POST'])
def GetMarketDealDetail():
    try:
        date = request.json
        rst = MD.QueryDeal(date['endDate'],date['bondname'])
        return rst
    except:
        pass

# 策略明细
@app.route('/api/selfmarketquote')
def GetSelfMarketQuote():
    try:
        df = SM.QuerySelfQuote()
        return df
    except:
        pass
# ————————————————————  系统监控  ——————————————————————————
@app.route('/api/monitormaxinterval')
def GetMaxInterval():
    try:
        df = SysMoniotor.QueryMaxInterval()
        return df
    except:
        pass

@app.route('/api/monitorlatestinterval')
def GetLatestInterval():
    try:
        df = SysMoniotor.QueryLastestInterval()
        return df
    except:
        pass

@app.route('/api/monitorlatestupdate')
def GetLatestUpdate():
    try:
        rst = SysMoniotor.QueryLatestUpdate()
        return rst
    except:
        pass

# ————————————————————   隐含评级调整   ———————————————————————————
@app.route('/api/hidencredit', methods=['POST'])
def GetHidenCreditChange():
    try:
        date = request.json
        rst = HC.HidenCreditBag(date['start'],date['end'])
        return rst
    except:
        pass

# 某个券评级调整历史
@app.route('/api/hidencreditchangehistory', methods=['POST'])
def GetBondHidenCreditChangeHistory():
    try:
        date = request.json
        rst = HC.CreditChangeHistoryBag(date['code'])
        return rst
    except:
        pass


# ————————————————————   销售统计   ———————————————————————————
# —————————————————————————————————————————————————————————————
@app.route('/api/saleallocateamt')
def GetAllocateamt():
    try:
        df = Sale.QueryRawSales()
        return df
    except:
        pass

@app.route('/api/saleallocateamtinterest')
def GetAllocateamtInterest():
    try:
        df = Sale.QueryInterestSales()
        return df
    except:
        pass

@app.route('/api/saleallocateamtcredit')
def GetAllocateamtCredit():
    try:
        df = Sale.QueryCreditSales()
        return df
    except:
        pass

# 交易员排名——所有
@app.route('/api/traderrankall', methods=['POST'])
def TraderRankAll():
    try:
        date = request.json
        rst = Sale.QureyTraderAll(date['start'], date['end'])
        return rst
    except:
        pass

# 交易员排名——利率
@app.route('/api/traderrankinterest', methods=['POST'])
def TraderRankInterest():
    try:
        date = request.json
        rst = Sale.QureyTraderInterest(date['start'], date['end'])
        return rst
    except:
        pass

# 交易员排名——信用
@app.route('/api/traderrankcredit', methods=['POST'])
def TraderRankCredit():
    try:
        date = request.json
        rst = Sale.QureyTraderCredit(date['start'], date['end'])
        return rst
    except:
        pass

# 交易员属性
@app.route('/api/traderprops', methods=['POST'])
def TraderPopos():
    try:
        date = request.json
        rst = Sale.QueryTraderProps(date['start'], date['end'],date['name'])
        return rst
    except:
        pass

# ----------------------

# 机构排名——所有
@app.route('/api/instituterankall', methods=['POST'])
def InstituteRankAll():
    try:
        date = request.json
        rst = Sale.QureyInstituteAll(date['start'], date['end'])
        return rst
    except:
        pass

# 机构排名——利率
@app.route('/api/instituterankinterest', methods=['POST'])
def InstituteRankInterest():
    try:
        date = request.json
        rst = Sale.QureyInstituteInterest(date['start'], date['end'])
        return rst
    except:
        pass

# 机构排名——信用
@app.route('/api/instituterankcredit', methods=['POST'])
def InstituteRankCredit():
    try:
        date = request.json
        rst = Sale.QureyInstituteCredit(date['start'], date['end'])
        return rst
    except:
        pass

# 机构属性
@app.route('/api/instituteprops', methods=['POST'])
def InstitutePopos():
    try:
        date = request.json
        rst = Sale.QueryInstituteProps(date['start'], date['end'],date['name'])
        return rst
    except:
        pass

@app.route('/api/tradersaleallocateamtinterest', methods=['POST'])
def GetTraderAllocateamtInterest():
    try:
        date = request.json
        df = Sale.QueryTraderInterestTs(date['name'])
        return df
    except:
        pass
@app.route('/api/tradersaleallocateamtcredit', methods=['POST'])
def GetTraderAllocateamtCredit():
    try:
        date = request.json
        df = Sale.QueryTraderCreditTs(date['name'])
        return df
    except:
        pass

@app.route('/api/inssaleallocateamtinterest', methods=['POST'])
def GetInsAllocateamtInterest():
    try:
        date = request.json
        df = Sale.QueryInsInterestTs(date['name'])
        return df
    except:
        pass
@app.route('/api/inssaleallocateamtcredit', methods=['POST'])
def GetInsAllocateamtCredit():
    try:
        date = request.json
        df = Sale.QueryInsCreditTs(date['name'])
        return df
    except:
        pass

# ----- 机构交易员对应关系 ------
@app.route('/api/traderandins', methods=['POST'])
def TraderandIns():
    try:
        date = request.json
        rst = Sale.BubbleData(date['start'], date['end'])
        return rst
    except:
        pass

# ----- 银行交易员对应关系 ------
@app.route('/api/banktraderrelative', methods=['POST'])
def TraderandBanks():
    try:
        date = request.json
        rst = Sale.BankBubbleData(date['start'], date['end'])
        return rst
    except:
        pass

# ----- 银行客户的地理分布 ------
@app.route('/api/banksgeo', methods=['POST'])
def BankGeo():
    try:
        date = request.json
        rst = Sale.BankGeoData(date['start'], date['end'])
        return rst
    except:
        pass

@app.route('/api/traderbankts', methods=['POST'])
def GetTraderBanksTsData():
    try:
        date = request.json
        df = Sale.QueryTraderBankTs(date['name'],date['org'])
        return df
    except:
        pass

# ------------------ 收益率曲线矩阵  ---------------------
@app.route('/api/curvemat')
def GetCurvemat():
    try:
        df = CM.QueryLatestMat()
        return df
    except:
        pass

# 相对价值曲线
@app.route('/api/reletivecurve', methods=['POST'])
def GetReletiveCurve():
    try:
        date = request.json
        df = CM.ReadRelativePrice(date['start'])
        return df
    except:
        pass

# 绝对价值曲线
@app.route('/api/absolutecurve', methods=['POST'])
def GetAbsoluteCurve():
    try:
        date = request.json
        rst = CM.ReadAbsolutePrice(date['start'], date['end'])
        return rst
    except:
        pass

# -----------------  XBOND日内交易报表  -------------------------
@app.route('/api/xbondreport')
def GetXBond():
    try:
        df = XBond.QueryXBond()
        return df
    except:
        pass

# -----------------  大事件  -------------------------
@app.route('/api/bignews')
def GetBigNews():
    try:
        df = Review.QueryBigNews(G.BIGNEWS)
        return df
    except:
        pass

@app.route('/api/bignewsnote')
def GetBigNewsNote():
    try:
        df = Review.QueryBigNewsNote(G.BIGNEWS)
        return df
    except:
        pass


# ------------------------   一级资质和情绪指数  -------------------------------
# ----- 一级资质指数 ------
@app.route('/api/qualification', methods=['POST'])
def IssueQualification():
    try:
        date = request.json
        rst = NB.QualificationIndex(date['start'],date['end'])
        return rst
    except:
        pass

# ----- 一级情绪指数 ------
@app.route('/api/emotion', methods=['POST'])
def IssueEmotion():
    try:
        date = request.json
        rst = NB.EmotionIndex(date['start'], date['end'])
        return rst
    except:
        pass

# ----- 异常倍数 ------
@app.route('/api/abnormal', methods=['POST'])
def FindAbnormal():
    try:
        date = request.json
        rst = NB.AbnormalNumber(date['start'], date['end'])
        return rst
    except:
        pass


# ------------------------   纯债基金指数  -----------------------------
@app.route('/api/purebondindex', methods=['POST'])
def PureBond():
    try:
        date = request.json
        rst = LongBond.LBIndex(date['start'], date['end'])
        return rst
    except:
        pass

# ------------------------   转债转股溢价率  -----------------------------
@app.route('/api/convpratio', methods=['POST'])
def convpratio():
    try:
        date = request.json
        rst = LongBond.ConvpRatio(date['start'], date['end'])
        return rst
    except:
        pass

# ------------------------   转债个券历史  -----------------------------
@app.route('/api/cbhistory', methods=['POST'])
def cbhistory():
    try:
        data = request.json
        rst = LongBond.ConvpRatioHistory(data['start'], data['end'],data['code'])
        return rst
    except:
        pass

# ------------------------   转债列表  -----------------------------
@app.route('/api/getcbondlist')
def getcbondlist():
    try:
        rst = LongBond.CBondList()
        return rst
    except:
        pas


# ------------------------   期货持仓变动  -----------------------------
@app.route('/api/futurepositionchange', methods=['POST'])
def PostFuturePosChange():
    try:
        date = request.json
        rst = FuturePos.ReadFuturePos(date['start'], date['end'])
        return rst
    except:
        pass

# 主力持仓走势
@app.route('/api/futurepositionts', methods=['POST'])
def PostMainPositionTs():
    try:
        data = request.json
        rst = FuturePos.QueryMainPosition(data['start'], data['end'], data['insname'])
        return rst
    except:
        pass

# 主力净持仓走势
@app.route('/api/futurenetpositionts', methods=['POST'])
def PostNetMainPositionTs():
    try:
        data = request.json
        rst = FuturePos.QueryMainNetPosition(data['start'], data['end'], data['insname'])
        return rst
    except:
        pass

# ------------------------   宏观回测  -----------------------------
@app.route('/api/review', methods=['POST'])
def MacroReview():
    try:
        date = request.json
        rst = ReStrategy.ReviewBag(date['data'])
        return rst
    except:
        pass

# ------------------------   预测指标  -----------------------------
@app.route('/api/xbondindex')
def XBondPredit():
    try:
        rst = Predit.XBondIndex()
        return rst
    except:
        pass

# 城投按省份统计
@app.route('/api/cityprovince', methods=['POST'])
def cityprovince():
    try:
        data = request.json
        rst = CreditAnalysis.CityDealsProvince(data['start'], data['end'])
        return rst
    except:
        pass

# 信用按行业统计
@app.route('/api/industrydeals', methods=['POST'])
def industrydeals():
    try:
        data = request.json
        rst = CreditAnalysis.IndustryDealsProvince(data['start'], data['end'])
        return rst
    except:
        pass

# 查询成交明细
@app.route('/api/creditdealdetail', methods=['POST'])
def creditdealdetail():
    try:
        data = request.json
        rst = CreditAnalysis.QueryDealsDetail(data['industry'], data['tdday'])
        return rst
    except:
        pass

# 查询换手率
@app.route('/api/creditturnoverrate', methods=['POST'])
def creditturnoverrate():
    try:
        rst = CreditAnalysis.QueryTurnoverRate(request.json)
        return rst
    except:
        pass


# 个券和曲线图
@app.route('/api/bondincurve', methods=['POST'])
def bondincurve():
    try:
        data = request.json
        rst = BIC.GetCurve(data['bondtype'])
        return rst
    except:
        pass

@app.route('/api/dealsbonds')
def dealsbonds():
    try:
        df = BIC.GetDeals()
        return df
    except:
        pass

@app.route('/api/bloomenews')
def bloomenews():
    try:
        df = Bloom.GetTodayAllNews()
        return df
    except:
        pass

# ————————————————————————————————————————————————————————————————
# 利差监控最新成交价
@app.route('/api/spreadlatestyield', methods=['POST'])
def spreadlatestyield():
    try:
        data = request.json
        rst = Spread.QueryLatestDealYield(data)
        return rst
    except:
        pass




# *******************************************************************
# *******************************************************************
if __name__ == '__main__':
    # app.run()
    try:
        # G.INSTYPEDICT = Ini.InitialAll()
        http_server = WSGIServer(('', 6000), app)
        http_server.serve_forever()
    except Exception:
        http_server = WSGIServer(('', 6000), app)
        http_server.serve_forever()


