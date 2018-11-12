from flask import Flask,jsonify,make_response,request
from flask_socketio import SocketIO,emit
# gevent
from gevent import monkey
from gevent.pywsgi import WSGIServer
monkey.patch_all()
# gevent end

import datetime
import pandas as pd
from pandas.io.json import json_normalize
import cx_Oracle
import sqlalchemy
from functools import wraps
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)
# conn=cx_Oracle.connect('FITRADINGSERVER/FITRADINGSERVER@FITRADE')    #连接数据库
# Engine = DB.getEngine('172.18.3.43', 'sa', 'tcl+nftx', 'VirtualExchange')
Engine = DB.getEngine('10.28.7.43', 'sa', 'tcl+nftx', 'VirtualExchange')
Engine73 = DB.getEngine('192.168.87.73', 'sa', 'tcl+nftx', 'MarketMaker')
EngineIS = DB.getEngine('10.28.7.43', 'sa', 'tcl+nftx', 'InvestSystem')

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
