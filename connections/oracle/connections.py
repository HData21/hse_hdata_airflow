import os

import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def connect_ugo():
    os.environ["NLS_LANG"] = ".UTF8"
    # dsn_tns = cx_Oracle.makedsn('IP', 1521, service_name='sml')  # 172.17.0.1
    dsn_tns = cx_Oracle.makedsn('IP', 1521, service_name='prd')  # 172.17.0.1
    return cx_Oracle.connect('USER', 'PWD', dsn_tns)

def connect_rhp_hdata():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('orclstage-1.cxp7emb18yqw.us-east-2.rds.amazonaws.com', 61521, service_name='orcl')
    return cx_Oracle.connect('UNIMED_GYN', 'UNIMEDGYN', dsn_tns)

def connect():
    connect_rhp_hdata_2 = 'oracle+cx_oracle://' + 'UNIMED_GYN' + ':' + 'UNIMEDGYN' + '@' + '/ORCL'
    return connect_rhp_hdata_2

def engine():
    engine = create_engine(connect(), max_identifier_length=128)
    return engine

def engine_rhp():
    engine_rhp = create_engine(connect_rhp_hdata(), max_identifier_length=128)
    return engine_rhp

Session = sessionmaker(bind=engine)
session = Session()

Session_engine = sessionmaker(bind=engine)
session_engine = Session_engine()