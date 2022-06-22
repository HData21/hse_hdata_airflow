import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from connections.oracle.connections_sml import connect_hse, connect_hdata, engine_hse, connect
from connections.oracle.connections import connect_hse, connect_hdata, engine_hse, connect
from collections import OrderedDict as od
from queries.hse.queries import *
from queries.hse.queries_hdata import *

from utils.integrity_checker import notify_email

START_DATE = airflow.utils.dates.days_ago(0)

default_args = {
    "owner": "raphael",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["raphael.queiroz@hdata.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

HOSPITAL = "HSE"

def update_cells(df_eq, table_name, CD):
    d = df_eq.to_dict(orient='split')
    print(d)
    for dado in d['data']:
        for i in range(len(dado) - 1):
            conn = connect_hdata()
            cursor = conn.cursor()

            query = ''
            query = 'UPDATE {nome_tabela} '.format(nome_tabela=table_name)
            if pd.isna(dado[i + 1]):
                query += 'SET {nome_coluna} is null '.format(nome_coluna=d['columns'][i + 1])
            else:
                if type(dado[i + 1]) == np.int64 or type(dado[i + 1]) == np.float64:
                    query += 'SET {nome_coluna} = {novo_valor} '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
                else:
                    query += 'SET {nome_coluna} = \'{novo_valor}\' '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
            query += 'WHERE {cd} IN({todos_cds})'.format(cd=CD, todos_cds=dado[0])

            # print(query)
            cursor.execute(query)
            conn.commit()
            conn.close()

def df_atendime():
    print("Entrou no df_atendime")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_atendime.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())
        print(df_dim.info())

        df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(999888)
        df_dim["CD_PACIENTE"] = df_dim["CD_PACIENTE"].fillna(999888)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(999888)
        df_dim["CD_CID"] = df_dim["CD_CID"].fillna("N/A")
        df_dim["CD_MOT_ALT"] = df_dim["CD_MOT_ALT"].fillna(999888)
        df_dim["CD_TIP_RES"] = df_dim["CD_TIP_RES"].fillna(999888)
        df_dim["CD_CONVENIO"] = df_dim["CD_CONVENIO"].fillna(999888)
        df_dim["CD_ESPECIALID"] = df_dim["CD_ESPECIALID"].fillna(999888)
        df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(999888)
        df_dim["CD_ATENDIMENTO_PAI"] = df_dim["CD_ATENDIMENTO_PAI"].fillna(999888)
        df_dim["CD_LEITO"] = df_dim["CD_LEITO"].fillna(999888)
        df_dim["CD_ORI_ATE"] = df_dim["CD_ORI_ATE"].fillna(999888)
        df_dim["CD_SERVICO"] = df_dim["CD_SERVICO"].fillna(999888)
        df_dim["TP_ATENDIMENTO"] = df_dim["TP_ATENDIMENTO"].fillna("N/A")
        df_dim["CD_TIP_MAR"] = df_dim["CD_TIP_MAR"].fillna(999888)
        df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(999888)
        df_dim["NM_USUARIO_ALTA_MEDICA"] = df_dim["NM_USUARIO_ALTA_MEDICA"].fillna("N/A")
        df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(999888)

        df_dim['HR_ALTA'] = df_dim['HR_ALTA'].astype(str)
        df_dim['HR_ALTA_MEDICA'] = df_dim['HR_ALTA_MEDICA'].astype(str)

        lista_cds_atendimentos = df_dim['CD_ATENDIMENTO'].to_list()
        lista_cds_atendimentos = [str(cd) for cd in lista_cds_atendimentos]
        atendimentos = ','.join(lista_cds_atendimentos)

        print(df_dim.info())

        df_stage = pd.read_sql(query_atendime_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_stage['HR_ALTA'] = df_stage['HR_ALTA'].astype(str)
        df_stage['HR_ALTA_MEDICA'] = df_stage['HR_ALTA_MEDICA'].astype(str)

        print(df_stage.info())

        df_diff = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        df_diff['HR_ALTA'] = pd.to_datetime(df_diff['HR_ALTA'])
        df_diff['HR_ALTA_MEDICA'] = pd.to_datetime(df_diff['HR_ALTA_MEDICA'])

        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()
        cursor = con.cursor()

        sql="INSERT INTO HSE.ATENDIME (CD_ATENDIMENTO, CD_MULTI_EMPRESA, CD_PACIENTE, CD_CID, CD_MOT_ALT, CD_TIP_RES, CD_CONVENIO, CD_ESPECIALID, CD_PRESTADOR, CD_ATENDIMENTO_PAI, CD_LEITO, CD_ORI_ATE, CD_SERVICO, TP_ATENDIMENTO, DT_ATENDIMENTO, HR_ATENDIMENTO, HR_ALTA, HR_ALTA_MEDICA, CD_TIP_MAR, CD_SINTOMA_AVALIACAO, NM_USUARIO_ALTA_MEDICA, CD_SETOR) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)
        con.commit()
        
        print("ATENDIME ATUALIZADOS")

        df_upd = df_dim[df_dim['CD_ATENDIMENTO'].isin(df_stage['CD_ATENDIMENTO'])]

        df_upd['HR_ALTA'] = pd.to_datetime(df_upd['HR_ALTA'])
        df_upd['HR_ALTA_MEDICA'] = pd.to_datetime(df_upd['HR_ALTA_MEDICA'])

        print("dados para update")
        print(df_upd.info())

        sql = "UPDATE HSE.ATENDIME SET HR_ALTA_MEDICA = NULL WHERE HR_ALTA_MEDICA < TIMESTAMP'2019-01-01 00:00:00.000'"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET HR_ALTA = NULL WHERE HR_ALTA < TIMESTAMP'2019-01-01 00:00:00.000'"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET HR_ATENDIMENTO = NULL WHERE HR_ATENDIMENTO < TIMESTAMP'2019-01-01 00:00:00.000'"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_MULTI_EMPRESA = NULL WHERE CD_MULTI_EMPRESA = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_PACIENTE = NULL WHERE CD_PACIENTE = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_ATENDIMENTO = NULL WHERE CD_ATENDIMENTO = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_MOT_ALT = NULL WHERE CD_MOT_ALT = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_TIP_RES = NULL WHERE CD_TIP_RES = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_CONVENIO = NULL WHERE CD_CONVENIO = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_PRESTADOR = NULL WHERE CD_PRESTADOR = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_ATENDIMENTO_PAI = NULL WHERE CD_ATENDIMENTO_PAI = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_ORI_ATE = NULL WHERE CD_ORI_ATE = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_SERVICO = NULL WHERE CD_SERVICO = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_TIP_MAR = NULL WHERE CD_TIP_MAR = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_SINTOMA_AVALIACAO = NULL WHERE CD_SINTOMA_AVALIACAO = 999888"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET CD_CID = NULL WHERE CD_CID = 'N/A'"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET TP_ATENDIMENTO = NULL WHERE TP_ATENDIMENTO = 'N/A'"

        cursor.execute(sql)
        con.commit()

        sql = "UPDATE HSE.ATENDIME SET NM_USUARIO_ALTA_MEDICA = NULL WHERE NM_USUARIO_ALTA_MEDICA = 'N/A'"

        cursor.execute(sql)
        con.commit()

        if len(lista_cds_atendimentos) > 0:
            df_diagnostico_atendime(atendimentos)

def df_cid():
    print("Entrou no df_cid")

    df_dim = pd.read_sql(query_cid, connect_hse())

    df_stage = pd.read_sql(query_cid_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.CID (CD_CID, DS_CID, CD_SGRU_CID) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_classificacao_risco():
    print("Entrou no df_classificacao_risco")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_classificacao_risco.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())

        df_dim["CD_CLASSIFICACAO_RISCO"] = df_dim["CD_CLASSIFICACAO_RISCO"].fillna(0)
        df_dim["CD_COR_REFERENCIA"] = df_dim["CD_COR_REFERENCIA"].fillna(0)
        df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)
        df_dim["CD_CLASSIFICACAO"] = df_dim["CD_CLASSIFICACAO"].fillna(0)

        df_stage = pd.read_sql(query_classificacao_risco_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_diff = df_dim.merge(df_stage["CD_CLASSIFICACAO_RISCO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()

        cursor = con.cursor()

        sql="INSERT INTO HSE.SACR_CLASSIFICACAO_RISCO (CD_CLASSIFICACAO_RISCO, CD_COR_REFERENCIA, CD_TRIAGEM_ATENDIMENTO, DH_CLASSIFICACAO_RISCO, CD_CLASSIFICACAO) VALUES (:1, :2, :3, :4, :5)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)

        con.commit()
        cursor.close
        con.close

        print("Dados SACR_CLASSIFICACAO_RISCO inseridos")

        df_upd = df_dim[df_dim['CD_CLASSIFICACAO_RISCO'].isin(df_stage['CD_CLASSIFICACAO_RISCO'])]

        print("dados para update")
        print(df_upd.info())

        # if not df_upd.empty:

        #     update_cells(df_upd,
        #                 'HSE.SACR_CLASSIFICACAO_RISCO',
        #                 'CD_CLASSIFICACAO_RISCO')

def df_classificacao():
    print("Entrou no df_classificacao")

    df_dim = pd.read_sql(query_classificacao, connect_hse())

    df_stage = pd.read_sql(query_classificacao_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_CLASSIFICACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.SACR_CLASSIFICACAO (CD_CLASSIFICACAO, DS_TIPO_RISCO, CD_COR_REFERENCIA) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados SACR_CLASSIFICACAO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CLASSIFICACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_convenio():
    print("Entrou no df_convenio")

    df_dim = pd.read_sql(query_convenio, connect_hse())

    df_stage = pd.read_sql(query_convenio_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.CONVENIO (CD_CONVENIO, NM_CONVENIO) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados CONVENIO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_cor_referencia():
    print("Entrou no df_cor_referencia")

    df_dim = pd.read_sql(query_cor_referencia, connect_hse())

    df_stage = pd.read_sql(query_cor_referencia_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_COR_REFERENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.SACR_COR_REFERENCIA (CD_COR_REFERENCIA, NM_COR) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados SACR_COR_REFERENCIA inseridos")

    df_upd = df_dim.merge(df_stage["CD_COR_REFERENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_diagnostico_atendime(atendimentos):
    print("Entrou no df_diagnostico_atendime")

    df_dim = pd.read_sql(query_diagnostico_atendime.format(atendimentos=atendimentos), connect_hse())

    df_stage = pd.read_sql(query_diagnostico_atendime_hdata.format(atendimentos=atendimentos), connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_DIAGNOSTICO_ATENDIME"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.DIAGNOSTICO_ATENDIME (CD_CID, CD_DIAGNOSTICO_ATENDIME, CD_ATENDIMENTO) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados DIAGNOSTICO_ATENDIME inseridos")

    df_upd = df_dim.merge(df_stage["CD_DIAGNOSTICO_ATENDIME"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_documento_clinico():
    print("Entrou no df_documento_clinico")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_documento_clinico.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())

        df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_TIPO_DOCUMENTO"] = df_dim["CD_TIPO_DOCUMENTO"].fillna(0)
        df_dim["TP_STATUS"] = df_dim["TP_STATUS"].fillna("0")

        df_stage = pd.read_sql(query_documento_clinico_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_diff = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()

        cursor = con.cursor()

        sql="INSERT INTO HSE.PW_DOCUMENTO_CLINICO (CD_OBJETO, CD_ATENDIMENTO, CD_TIPO_DOCUMENTO, TP_STATUS, DH_CRIACAO) VALUES (:1, :2, :3, :4, :5)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)

        con.commit()
        cursor.close
        con.close

        print("Dados PW_DOCUMENTO_CLINICO inseridos")

        df_upd = df_dim[df_dim['CD_OBJETO'].isin(df_stage['CD_OBJETO'])]

        print("dados para update")
        print(df_upd.info())

        # if not df_upd.empty:

        #     update_cells(df_upd, 'HSE.PW_DOCUMENTO_CLINICO', 'CD_OBJETO')

def df_esp_med():
    print("Entrou no df_esp_med")

    df_dim = pd.read_sql(query_esp_med, connect_hse())

    print(df_dim)

    df_stage = pd.read_sql(query_esp_med_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.ESP_MED (CD_ESPECIALID, CD_PRESTADOR, SN_ESPECIAL_PRINCIPAL) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados ESP_MED inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_especialidad():
    print("Entrou no df_especialidad")

    df_dim = pd.read_sql(query_especialidad, connect_hse())

    df_stage = pd.read_sql(query_especialidad_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.ESPECIALID (CD_ESPECIALID, DS_ESPECIALID) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados ESPECIALID inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_gru_cid():
    print("Entrou no df_gru_cid")

    df_dim = pd.read_sql(query_gru_cid, connect_hse())

    df_stage = pd.read_sql(query_gru_cid_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_GRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.GRU_CID (CD_GRU_CID, DS_GRU_CID) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados GRU_CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_GRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_mot_alt():
    print("Entrou no df_mot_alt")

    df_dim = pd.read_sql(query_mot_alt, connect_hse())

    df_stage = pd.read_sql(query_mot_alt_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_MOT_ALT"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.MOT_ALT (CD_MOT_ALT, DS_MOT_ALT, TP_MOT_ALTA) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados MOT_ALT inseridos")

def df_multi_empresa():
    print("Entrou no df_multi_empresa")

    df_dim = pd.read_sql(query_multi_empresa, connect_hse())

    df_stage = pd.read_sql(query_multi_empresa_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_MULTI_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.MULTI_EMPRESAS (CD_MULTI_EMPRESA, DS_MULTI_EMPRESA) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados MULTI_EMPRESAS inseridos")

    df_upd = df_dim.merge(df_stage["CD_MULTI_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_ori_ate():
    print("Entrou no df_ori_ate")

    df_dim = pd.read_sql(query_ori_ate, connect_hse())

    df_stage = pd.read_sql(query_ori_ate_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_ORI_ATE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.ORI_ATE (CD_ORI_ATE, DS_ORI_ATE, TP_ORIGEM, CD_SETOR) VALUES (:1, :2, :3, :4)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados ORI_ATE inseridos")

    df_upd = df_dim.merge(df_stage["CD_ORI_ATE"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_prestador():
    print("Entrou no df_prestador")

    df_dim = pd.read_sql(query_prestador, connect_hse())

    df_stage = pd.read_sql(query_prestador_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_PRESTADOR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.PRESTADOR (CD_PRESTADOR, NM_PRESTADOR, DT_NASCIMENTO, TP_PRESTADOR, CD_TIP_PRESTA) VALUES (:1, :2, :3, :4, :5)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados PRESTADOR inseridos")

    df_upd = df_dim.merge(df_stage["CD_PRESTADOR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_paciente():
    print("Entrou no df_paciente")

    df_dim = pd.read_sql(query_paciente, connect_hse())

    print(df_dim.info())

    df_stage = pd.read_sql(query_paciente_hdata, connect_hdata())
    df_stage["DT_NASCIMENTO"] = df_stage["DT_NASCIMENTO"].astype(str)
    print(df_stage.info())

    df_diff = df_dim.merge(df_stage["CD_PACIENTE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print(df_diff['DT_NASCIMENTO'])

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.PACIENTE (CD_PACIENTE, DT_NASCIMENTO, TP_SEXO, DT_CADASTRO, NM_BAIRRO) VALUES (:1, :2, :3, :4, :5)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados PACIENTE inseridos")

    df_upd = df_dim.merge(df_stage["CD_PACIENTE"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_pagu_objeto():
    print("Entrou no df_pagu_objeto")

    df_dim = pd.read_sql(query_pagu_objeto, connect_hse())

    df_stage = pd.read_sql(query_pagu_objeto_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.PAGU_OBJETO (CD_OBJETO, TP_OBJETO) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados PAGU_OBJETO inseridos")

    df_upd = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_registro_alta():
    print("Entrou no df_registro_alta")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_registro_alta.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())
        print(df_dim.info())
        
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

        df_stage = pd.read_sql(query_registro_alta_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_diff = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()

        cursor = con.cursor()

        sql="INSERT INTO HSE.PW_REGISTRO_ALTA (CD_ATENDIMENTO, HR_ALTA_MEDICA) VALUES (:1, :2)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)

        con.commit()
        cursor.close
        con.close

        print("Dados PW_REGISTRO_ALTA inseridos")

        df_upd = df_dim[df_dim['CD_ATENDIMENTO'].isin(df_stage['CD_ATENDIMENTO'])]

        print("dados para update")
        print(df_upd.info())

        # if not df_upd.empty:

        #     update_cells(df_upd, 'HSE.PW_REGITRO_ALTA', 'CD_ATENDIMENTO')

def df_setor():
    print("Entrou no df_setor")

    df_dim = pd.read_sql(query_setor, connect_hse())

    df_stage = pd.read_sql(query_setor_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_SETOR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.SETOR (CD_SETOR, NM_SETOR) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados SETOR inseridos")

    df_upd = df_dim.merge(df_stage["CD_SETOR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_sgru_cid():
    print("Entrou no df_sgru_cid")

    df_dim = pd.read_sql(query_sgru_cid, connect_hse())

    df_stage = pd.read_sql(query_sgru_cid_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_SGRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.SGRU_CID (CD_SGRU_CID, CD_GRU_CID, DS_SGRU_CID) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados SGRU_CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_SGRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_sintoma_avaliacao():
    print("Entrou no df_sintoma_avaliacao")

    df_dim = pd.read_sql(query_sintoma_avaliacao, connect_hse())

    df_stage = pd.read_sql(query_sintoma_avaliacao_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_SINTOMA_AVALIACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.SACR_SINTOMA_AVALIACAO (CD_SINTOMA_AVALIACAO, DS_SINTOMA) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados SACR_SINTOMA_AVALIACAO inseridos")

    df_upd = df_dim.merge(df_stage["CD_SINTOMA_AVALIACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_tempo_processo():
    print("Entrou no df_tempo_processo")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_tempo_processo.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())

        df_dim["CD_TIPO_TEMPO_PROCESSO"] = df_dim["CD_TIPO_TEMPO_PROCESSO"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

        df_stage = pd.read_sql(query_tempo_processo_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()

        cursor = con.cursor()

        sql="INSERT INTO HSE.SACR_TEMPO_PROCESSO (DH_PROCESSO, CD_TIPO_TEMPO_PROCESSO, CD_ATENDIMENTO) VALUES (:1, :2, :3)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)

        con.commit()
        cursor.close
        con.close

        print("Dados SACR_TEMPO_PROCESSO inseridos")

        # df_upd = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        # df_upd = df_upd.drop(columns=['_merge'])
        # df_upd = df_upd.reset_index(drop=True)

        # print("dados para update")
        # print(df_upd.info())

        # update_cells(df_upd, 'HSE.SACR_TEMPO_PROCESSO', 'CD_ATENDIMENTO')

def df_tip_mar():
    print("Entrou no df_tip_mar")

    df_dim = pd.read_sql(query_tip_mar, connect_hse())

    df_stage = pd.read_sql(query_tip_mar_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_MAR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.TIP_MAR (CD_TIP_MAR) VALUES (:1)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados TIP_MAR inseridos")

    df_upd = df_dim.merge(df_stage["CD_TIP_MAR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_tip_res():
    print("Entrou no df_tip_res")

    df_dim = pd.read_sql(query_tip_res, connect_hse())

    df_stage = pd.read_sql(query_tip_res_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_RES"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.TIP_RES (CD_TIP_RES, DS_TIP_RES, SN_OBITO) VALUES (:1, :2, :3)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados TIP_RES inseridos")

    df_upd = df_dim.merge(df_stage["CD_TIP_RES"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_triagem_atendimento():
    print("Entrou no df_triagem_atendimento")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_triagem_atendimento.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hse())

        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)
        df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
        df_dim["DS_SENHA"] = df_dim["DS_SENHA"].fillna("0")

        df_stage = pd.read_sql(query_triagem_atendimento_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_hdata())

        df_diff = df_dim.merge(df_stage["CD_TRIAGEM_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        
        print("dados para incremento")
        print(df_diff.info())

        con = connect_hdata()

        cursor = con.cursor()

        sql="INSERT INTO HSE.TRIAGEM_ATENDIMENTO (CD_TRIAGEM_ATENDIMENTO, CD_ATENDIMENTO, CD_SINTOMA_AVALIACAO, DS_SENHA, DH_PRE_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)

        con.commit()
        cursor.close
        con.close

        print("Dados TRIAGEM_ATENDIMENTO inseridos")

        df_upd = df_dim[df_dim['CD_TRIAGEM_ATENDIMENTO'].isin(df_stage['CD_TRIAGEM_ATENDIMENTO'])]

        print("dados para update")
        print(df_upd.info())

        # if not df_upd.empty:

        #     update_cells(df_upd, 'HSE.TRIAGEM_ATENDIMENTO', 'CD_TRIAGEM_ATENDIMENTO')

def df_usuario():
    print("Entrou no df_usuario")

    df_dim = pd.read_sql(query_usuario, connect_hse())

    df_stage = pd.read_sql(query_usuario_hdata, connect_hdata())

    df_diff = df_dim.merge(df_stage["CD_USUARIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_hdata()

    cursor = con.cursor()

    sql="INSERT INTO HSE.USUARIOS (CD_USUARIO, NM_USUARIO) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1

    cursor.executemany(sql, cols)

    con.commit()
    cursor.close
    con.close

    print("Dados USUARIOS inseridos")

    df_upd = df_dim.merge(df_stage["CD_USUARIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)
# dt_ontem = datetime.datetime(2020, 2, 15)
# dt_ini = datetime.datetime(2021, 12, 10)

# dag = DAG("captura_dados_hse_sml", default_args=default_args, schedule_interval=None)
dag = DAG("captura_dados_hse_prod", default_args=default_args, schedule_interval="0 23,0 * * *")

t0 = PythonOperator(
    task_id="captura_atendime_hse",
    python_callable=df_atendime,
    on_failure_callback=notify_email,
    dag=dag)

t1 = PythonOperator(
    task_id="captura_cid_hse",
    python_callable=df_cid,
    on_failure_callback=notify_email,
    dag=dag)

t2 = PythonOperator(
    task_id="captura_classificacao_risco_hse",
    python_callable=df_classificacao_risco,
    on_failure_callback=notify_email,
    dag=dag)

t3 = PythonOperator(
    task_id="captura_classificacao_hse",
    python_callable=df_classificacao,
    on_failure_callback=notify_email,
    dag=dag)

t4 = PythonOperator(
    task_id="captura_convenio_hse",
    python_callable=df_convenio,
    on_failure_callback=notify_email,
    dag=dag)

t5 = PythonOperator(
    task_id="captura_cor_referencia_hse",
    python_callable=df_cor_referencia,
    on_failure_callback=notify_email,
    dag=dag)

t6 = PythonOperator(
    task_id="captura_documento_clinico_hse",
    python_callable=df_documento_clinico,
    on_failure_callback=notify_email,
    dag=dag)

t7 = PythonOperator(
    task_id="captura_esp_med_hse",
    python_callable=df_esp_med,
    on_failure_callback=notify_email,
    dag=dag)

t8 = PythonOperator(
    task_id="captura_especialidad_hse",
    python_callable=df_especialidad,
    on_failure_callback=notify_email,
    dag=dag)

t9 = PythonOperator(
    task_id="captura_gru_cid_hse",
    python_callable=df_gru_cid,
    on_failure_callback=notify_email,
    dag=dag)

t10 = PythonOperator(
    task_id="captura_prestador_hse",
    python_callable=df_prestador,
    on_failure_callback=notify_email,
    dag=dag)

t11 = PythonOperator(
    task_id="captura_mot_alt_hse",
    python_callable=df_mot_alt,
    dag=dag)

t12 = PythonOperator(
    task_id="captura_multi_empresa_hse",
    python_callable=df_multi_empresa,
    on_failure_callback=notify_email,
    dag=dag)

t13 = PythonOperator(
    task_id="captura_ori_ate_hse",
    python_callable=df_ori_ate,
    on_failure_callback=notify_email,
    dag=dag)

t14 = PythonOperator(
    task_id="captura_paciente_hse",
    python_callable=df_paciente,
    on_failure_callback=notify_email,
    dag=dag)

t15 = PythonOperator(
    task_id="captura_pagu_objeto_hse",
    python_callable=df_pagu_objeto,
    on_failure_callback=notify_email,
    dag=dag)

t16 = PythonOperator(
    task_id="captura_registro_alta_hse",
    python_callable=df_registro_alta,
    on_failure_callback=notify_email,
    dag=dag)

t17 = PythonOperator(
    task_id="captura_setor_hse",
    python_callable=df_setor,
    on_failure_callback=notify_email,
    dag=dag)

t18 = PythonOperator(
    task_id="captura_sgru_cid_hse",
    python_callable=df_sgru_cid,
    on_failure_callback=notify_email,
    dag=dag)

# t19 = PythonOperator(
#     task_id="captura_sintoma_avaliacao_hse",
#     python_callable=df_sintoma_avaliacao,
#     on_failure_callback=notify_email,
#     dag=dag)

t20 = PythonOperator(
    task_id="captura_tempo_processo_hse",
    python_callable=df_tempo_processo,
    on_failure_callback=notify_email,
    dag=dag)

t21 = PythonOperator(
    task_id="captura_tip_mar_hse",
    python_callable=df_tip_mar,
    on_failure_callback=notify_email,
    dag=dag)

t22 = PythonOperator(
    task_id="captura_tip_res_hse",
    python_callable=df_tip_res,
    on_failure_callback=notify_email,
    dag=dag)

t23 = PythonOperator(
    task_id="captura_triagem_atendimento_hse",
    python_callable=df_triagem_atendimento,
    on_failure_callback=notify_email,
    dag=dag)

t24 = PythonOperator(
    task_id="captura_usuario_hse",
    python_callable=df_usuario,
    on_failure_callback=notify_email,
    dag=dag)

# t16 >> t23 >> t20 >> t6 >> t2 >> t0
t4 >> t5 >> t8 >> t11 >> t12 >> t13 >> t17 >> t18 >> t21 >> t22 >> t24 >> t3 >> t9 >> t7 >> t15 >> t1 >> t14 >> t10 >> t16 >> t23 >> t20 >> t6 >> t2 >> t0