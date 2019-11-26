from flask import Flask,jsonify,make_response,request,Response
from flask_socketio import SocketIO,emit
# gevent
from gevent import monkey
from gevent.pywsgi import WSGIServer
monkey.patch_all()
# gevent end
import json
import datetime
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import sqlalchemy
from functools import wraps
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)
# import cx_Oracle
# conn=cx_Oracle.connect('FITRADINGSERVER/FITRADINGSERVER@FITRADE')    #连接数据库
# conn=cx_Oracle.connect('tmpuser/tmpuser@10.28.7.51:1521/orcl')    #连接数据库
Engine = DB.getEngine('10.28.7.43', 'bond', 'bond', 'VirtualExchange')
Engine73 = DB.getEngine('192.168.87.73', 'sa', 'tcl+nftx', 'MarketMaker')
EngineIS = DB.getEngine('10.28.7.43', 'bond', 'bond', 'InvestSystem')
Engine103 = DB.getEngine('192.168.87.103', 'sa', 'sa123', 'Invest')

import Tools.MongoHelper as MG
crmDb = MG._connect_mongo("10.28.7.35",39362,"datacenter","datacenter","datacenter")

import redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)


import Global as G

import quoteStatus as QS
import bondLend as BD
import position as PO
import NewBondRate as NB
import MarketScore as MS
import CDFunctionsDailyJob as CD
import ReportChart as RC
import MarketDeal as MD
import SelfMarketMaker as SM
import SystemMonitor as SysMoniotor
import HidenCredit as HC
import SaleStatistic as Sale
import Initial as Ini
import CureveMat as CM
import Todo as TD
import XBonds as XBond
import ReviewHistory as Review
import LongBondIndex as LongBond
import FuturePosition as FuturePos
import ReviewStrategy as ReStrategy
import PredictIndex as Predit
import CreditSecondAnalysis as CreditAnalysis
import BondInCurve as BIC
import Bloomebergnews as Bloom
import SpreadMonitor as Spread