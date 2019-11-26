from heads import *

def GetTodayAllNews():
    Df = pd.read_sql("SELECT distinct [newsinfo],[newstime] FROM [InvestSystem].[dbo].[BloombergNews] where newstime>= '"+dh.today2str()+"'  order by newstime desc",Engine)
    Df.newstime = Df.newstime.astype('str')
    return Df.to_json(orient="records")