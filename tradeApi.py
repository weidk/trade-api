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

# 现券市场成交——期限
@app.route('/api/markettermdeal', methods=['POST'])
def CalMarketTermDealAmt():
    try:
        date = request.json
        rst = MD.TrimTermData(date['start'],date['end'])
        return rst
    except:
        pass

# 债券借贷
@app.route('/api/bondlend', methods=['POST'])
def GetBondLenddata():
    try:
        code = request.json
        rst = BD.bondLendBag(code)
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


# *******************************************************************
# *******************************************************************
if __name__ == '__main__':
    # app.run()
    try:
        http_server = WSGIServer(('', 6000), app)
        http_server.serve_forever()
    except Exception:
        http_server = WSGIServer(('', 6000), app)
        http_server.serve_forever()