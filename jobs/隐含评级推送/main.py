from heads import *


def ToDataBase(href, title, src, date):
    Df = pd.DataFrame({'href': href,
                       'title': title,
                       'src': src,
                       'date': date}, index=[0])
    try:
        Df.to_sql('ImportInfo', Engine, if_exists='append', index=False, index_label=Df.columns,
                  dtype={'date': sqlalchemy.DateTime, 'href': sqlalchemy.String,
                         'title': sqlalchemy.String, 'src': sqlalchemy.String, 'reader': sqlalchemy.String})
        print(src + " : " + title)
    except:
        pass


def GetHidenChangeData():
    start = (datetime.date.today() + datetime.timedelta(days=-2)).strftime('%Y%m%d')
    end = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    data = r.get(start + '-' + end)
    Df = pd.read_json(data)
    NewDf = Df.ix[:, ['ISSUER', 'NEWCREDIT', 'OLDCREDIT', 'change']].drop_duplicates()
    NewDf['CREDITINFO'] = NewDf.OLDCREDIT + '  调为  ' + NewDf.NEWCREDIT
    if NewDf.shape[0] > 0:
        src = '投研系统'
        date = DH.today2str()
        for index, row in NewDf.iterrows():
            title = '【' + row.ISSUER + '】' + '隐含评级' + '【' +row.change + '】'+ DH.today2str()
            ToDataBase(' ', title, row.CREDITINFO, date)


if __name__ == '__main__':
    try:
        print('jobs is running')
        GetHidenChangeData()

    except Exception:
        print('come through some errors')
