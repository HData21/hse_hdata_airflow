import os

import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def connect_hse():
    os.environ["NLS_LANG"] = ".UTF8"
    # dsn_tns = cx_Oracle.makedsn('IP', 1521, service_name='sml')  # 172.17.0.1
    dsn_tns = cx_Oracle.makedsn('10.0.1.3', 1521, service_name='sml')  # 172.17.0.1
    return cx_Oracle.connect('hdata', 'hd4t4', dsn_tns)

def connect_hdata():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('orclstage-1.cxp7emb18yqw.us-east-2.rds.amazonaws.com', 61521, service_name='orcl')
    return cx_Oracle.connect('HSE', 'HSE', dsn_tns)

def connect():
    connect_hse_hdata_2 = 'oracle+cx_oracle://' + 'HSE' + ':' + 'HSE' + '@' + '/ORCL'
    return connect_hse_hdata_2

def engine():
    engine = create_engine(connect(), max_identifier_length=128)
    return engine

def engine_hse():
    engine_hse = create_engine(connect_hse_hdata(), max_identifier_length=128)
    return engine_hse

Session = sessionmaker(bind=engine)
session = Session()

Session_engine = sessionmaker(bind=engine)
session_engine = Session_engine()