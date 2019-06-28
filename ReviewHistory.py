from heads import *

def QueryBigNews(RawData):
    RawData.DATE = RawData.DATE.astype('str')
    # Df = RawData.ix[:,['DATE','CNYIELD']]
    return RawData.to_json(orient='records')

def QueryBigNewsNote(RawData):
    RawData.DATE = RawData.DATE.astype('str')
    Df = RawData.dropna()
    return Df.to_json(orient='records')