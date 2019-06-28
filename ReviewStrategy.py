from heads import *

def ReviewBag(data_str):
    RawDf = pd.DataFrame(data_str)
    # RawDf = pd.read_excel('123.xlsx')
    Df = ModifyRawData(RawDf)
    RstDf = CalProfit(Df)
    # t = {}
    # t['b'] = SymbolDf.to_dict(orient="records")
    # t['a'] = RstDf.to_dict(orient="records")
    # return json.dumps(t,ensure_ascii=False)
    return RstDf.to_json(orient="records")

# 整理原始数据
def ModifyRawData(RawDf):
    Df = RawDf.ix[:,['date','price','position']]
    Df.date = Df.date.astype('str')
    Df.price = Df.price.astype('float')
    return Df

# 计算每日盈亏
def CalProfit(Df):
    # 计算每日涨跌幅
    Df['price0'] = Df.price.shift(1)
    Df['changerate'] = Df.price/Df.price0
    Df['netchangerate'] = Df.changerate - 1
    Df['beta'] = 100*Df.price/Df.price[0]-100
    Df['comsumprincipal'] = 0.00
    for i in range(0,Df.price.shape[0]):
        if i==0:
            Df.comsumprincipal[i] = 100
        else:
            Df.comsumprincipal[i] = Df.comsumprincipal[i-1] + Df.comsumprincipal[i-1]*Df.position[i]*Df.netchangerate[i]
    Df['comsumreturn'] = Df.comsumprincipal-100
    Df['drawdown'] = 0.00
    for i in range(0, Df.price.shape[0]):
        if i == 0:
            Df.drawdown[i] = 0
        else:
            Df.drawdown[i] = Df.comsumprincipal[i] - max(Df.comsumprincipal[0:i+1])
    # Df.ix[:, ['beta', 'comsumreturn', 'drawdown']].plot()
    def position(x, sig, t):
        if x * sig > 0:
            return t
        else:
            return np.nan

    Df['BUY'] = Df.apply(lambda x: position(x.position, 1, x.price), axis=1)
    Df['SELL'] = Df.apply(lambda x: position(x.position, -1, x.price), axis=1)
    Df['NONE'] = Df.apply(lambda x: x.price if x.position==0 else np.nan, axis=1)
    for i in range(0,Df.shape[0]-1):
        if pd.isnull(Df.BUY[i]) and not pd.isnull(Df.BUY[i+1]):
            Df.set_value(i,'BUY',Df.price[i])
        if pd.isnull(Df.SELL[i]) and not pd.isnull(Df.SELL[i+1]):
            Df.set_value(i,'SELL',Df.price[i])
        if pd.isnull(Df.NONE[i]) and not pd.isnull(Df.NONE[i+1]):
            Df.set_value(i,'NONE',Df.price[i])

    RstDf = Df.ix[:, ['date', 'price', 'position', 'beta', 'comsumreturn', 'BUY', 'SELL', 'NONE', 'drawdown']]
    WinDf = Df.position*Df.netchangerate
    WinRate = 100*WinDf[WinDf > 0].count()/WinDf[WinDf!=0].count()
    DealTimes = WinDf[WinDf!=0].count()
    MaxDrawDown = min(Df.drawdown)
    DateSeries = pd.to_datetime(Df.date)
    YearlyReturn = 365*Df.comsumreturn[Df.shape[0] - 1]/(DateSeries[Df.shape[0]-1] - DateSeries[0]).days
    # 历史波动率
    # logreturn = np.log(Df.comsumprincipal/Df.comsumprincipal.shift(1))
    logreturn = np.diff(np.log(Df.comsumprincipal))
    Volatility = np.std(logreturn) / np.mean(logreturn)
    if (DateSeries[1]-DateSeries[0]).days>=360:
        Volatility = Volatility / np.sqrt(1)
    elif (DateSeries[1]-DateSeries[0]).days>=28:
        Volatility = Volatility / np.sqrt(1/12)
    else:
        Volatility = Volatility / np.sqrt(1/252)
    SymbolDf  = pd.DataFrame({'WinRate':float('%.4f' % WinRate),'DealTimes':DealTimes,'MaxDrawDown':float('%.4f' % MaxDrawDown),'YearlyReturn':float('%.4f' % YearlyReturn),'Volatility':float('%.4f' % Volatility)},index=[0])
    RstDf['WinRate'] = float('%.2f' % WinRate)
    RstDf['DealTimes'] = DealTimes
    RstDf['MaxDrawDown'] = float('%.2f' % MaxDrawDown)
    RstDf['YearlyReturn'] = float('%.4f' % YearlyReturn)
    RstDf['Volatility'] = float('%.2f' % Volatility)
    Returnperdrawdown = -YearlyReturn / MaxDrawDown
    RstDf['Returnperdrawdown'] = float('%.2f' % Returnperdrawdown)
    return RstDf