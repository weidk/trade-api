from heads import *
from FunctionHeads import *

if __name__ == '__main__':
    try:
        print('redis jobs is running')
        HidenCredit.HidenCreditBag(start = (datetime.date.today() + datetime.timedelta(days=-2)).strftime('%Y%m%d'), end = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
        BondCurve.CurveBag()
        CurveMat.CurveMatBag()
        BondLend.bondLendBagGK()
        print('redis jobs is finished')
    except Exception:
        print('come through some errors')