import pandas as pd




# 计算机构类别
def ClassifyInsType():
    Df = pd.read_excel('中债交割排名.xlsx')
    First = Df.债券交割量.quantile(0.7)
    Second = Df.债券交割量.quantile(0.4)
    def classify(x):
        if x >= First:
            return 'A'
        elif x >= Second:
            return 'B'
        else:
            return 'C'

    Df['类别'] = Df.债券交割量.apply(classify)
    Df = Df.ix[:, ['机构名称', '类别']]
    Df.机构名称 = Df.机构名称.str[0:2]
    InsDict = Df.set_index('机构名称').T.to_dict('list')
    return InsDict

def InitialAll():
    INSTYPEDICT = ClassifyInsType()
    return INSTYPEDICT