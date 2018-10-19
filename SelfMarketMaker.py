from heads import *

def QuerySelfQuote():
    Df = pd.read_sql("select * from [marketmaker].[dbo].[vty_marketquote]",Engine73)
    Df.OrdPrice = Df.OrdPrice.round(4)
    return Df.to_json(orient='records')