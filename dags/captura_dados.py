import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connections.oracle.connections import connect_ugo, connect_ugo_hdata, engine_ugo, connect
from collections import OrderedDict as od
from queries.unimed_go.queries import *
from queries.unimed_go.queries_hdata import *

from utils.integrity_checker import notify_email

START_DATE = airflow.utils.dates.days_ago(2)

default_args = {
    "owner": "raphael",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["raphael.queiroz@eximio.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

HOSPITAL = "UNIMED GO"

def update_cells(df_eq, table_name, CD):
    d = df_eq.to_dict(orient='split')
    print(d)
    for dado in d['data']:
        for i in range(len(dado) - 1):
            conn = connect_ugo_hdata()
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

def df_cid_doenca():
    print("Entrou no df_cid_doenca")

    df_dim = pd.read_sql(query_cid_doenca, connect_ugo())

    df_stage = pd.read_sql(query_cid_doenca_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.CID_DOENCA (CD_DOENCA_CID, DS_DOENCA_CID) VALUES (:1, :2)"

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

def df_estabelecimento():
    print("Entrou no df_estabelecimento")

    df_dim = pd.read_sql(query_estabelecimento, connect_ugo())

    df_stage = pd.read_sql(query_estabelecimento_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESTABELECIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.ESTABELECIMENTO (CD_EMPRESA, CD_ESTABELECIMENTO, NM_FANTASIA_ESTAB) VALUES (:1, :2, :3)"

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

    print("Dados ESTABELECIMENTO inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESTABELECIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_empresa():
    print("Entrou no df_empresa")

    df_dim = pd.read_sql(query_empresa, connect_ugo())

    df_stage = pd.read_sql(query_empresa_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.EMPRESA (CD_EMPRESA, NM_RAZAO_SOCIAL, DS_NOME_CURTO) VALUES (:1, :2, :3)"

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

    print("Dados EMPRESA inseridos")

    df_upd = df_dim.merge(df_stage["CD_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_ped_ex_ext_item():
    print("Entrou no df_ped_ex_ext_item")

    df_dim = pd.read_sql(query_ped_ex_ext_item, connect_ugo())

    df_stage = pd.read_sql(query_ped_ex_ext_item_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["NR_PROC_INTERNO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.PED_EX_EXT_IT (NR_PROC_INTERNO, NR_SEQ_PEDIDO) VALUES (:1, :2)"

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

    print("Dados PED_EX_EXT_IT inseridos")

    df_upd = df_dim.merge(df_stage["NR_PROC_INTERNO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_ped_ex_ext():
    print("Entrou no df_ped_ex_ext")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_ped_ex_ext.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_ped_ex_ext_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["NR_SEQUENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.PED_EX_EXT (NR_SEQUENCIA, NR_ATENDIMENTO, DT_INATIVACAO) VALUES (:1, :2, :3)"

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

        print("Dados PED_EX_EXT inseridos")

        df_upd = df_dim.merge(df_stage["NR_SEQUENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_exame_lab():
    print("Entrou no df_exame_lab")

    df_dim = pd.read_sql(query_exame_lab, connect_ugo())

    df_stage = pd.read_sql(query_exame_lab_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["NR_SEQ_EXAME"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.EXAME_LABORATORIO (NR_SEQ_EXAME, NR_SEQ_GRUPO, NM_EXAME, DS_UNIDADE_MEDIDA) VALUES (:1, :2, :3, :4)"

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

    print("Dados EXAME_LABORATORIO inseridos")

    df_upd = df_dim.merge(df_stage["NR_SEQ_EXAME"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_prescr_procedimento():
    print("Entrou no df_prescr_procedimento")

    df_dim = pd.read_sql(query_prescr_procedimento, connect_ugo())

    df_stage = pd.read_sql(query_prescr_procedimento_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_PROCEDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.PRESCR_PROCEDIMENTO (IE_STATUS_ATEND, NR_SEQ_EXAME, NR_PRESCRICAO, CD_PROCEDIMENTO, IE_ORIGEM_PROCED, NR_SEQUENCIA, IE_VIA_APLICACAO, DS_HORARIOS, DS_JUSTIFICATIVA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)"

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

    print("Dados PRESCR_PROCEDIMENTO inseridos")

    df_upd = df_dim.merge(df_stage["CD_PROCEDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_prescr_medica_v():
    print("Entrou no df_prescr_medica_v")

    df_dim = pd.read_sql(query_prescr_medica_v, connect_ugo())

    df_stage = pd.read_sql(query_prescr_medica_v_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage, indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.PRESCR_MEDICA_V (NR_PRESCRICAO, NR_ATENDIMENTO) VALUES (:1, :2)"

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

    print("Dados PRESCR_MEDICA_V inseridos")

    df_upd = df_dim.merge(df_stage, indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_diagnostico_doenca():
    print("Entrou no df_diagnostico_doenca")

    df_dim = pd.read_sql(query_diagnostico_doenca, connect_ugo())

    df_stage = pd.read_sql(query_diagnostico_doenca_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["NR_SEQ_INTERNO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.DIAGNOSTICO_DOENCA (CD_DOENCA, NR_ATENDIMENTO, NR_SEQ_INTERNO) VALUES (:1, :2, :3)"

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

    print("Dados DIAGNOSTICO_DOENCA inseridos")

    df_upd = df_dim.merge(df_stage["NR_SEQ_INTERNO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_atendimento_paciente():
    print("Entrou no df_atendimento_paciente")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_atendimento_paciente.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_atendimento_paciente_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.ATENDIMENTO_PACIENTE (NR_ATENDIMENTO, NR_ATENDIMENTO_MAE, DT_ENTRADA, DT_INICIO_ATENDIMENTO, DT_ATEND_MEDICO, DT_ALTA, DT_ALTA_MEDICO, DT_FIM_TRIAGEM, NR_SEQ_TRIAGEM, DT_MEDICACAO, CD_MOTIVO_ALTA, CD_MOTIVO_ALTA_MEDICA, IE_TIPO_ATENDIMENTO, CD_PESSOA_FISICA, CD_MEDICO_RESP, NR_SEQ_PAC_SENHA_FILA, IE_CLINICA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)"

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

        print("Dados ATENDIMENTO_PACIENTE inseridos")

        df_upd = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_atend_paciente_unidade():
    print("Entrou no df_atend_paciente_unidade")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_atend_paciente_unidade.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_atend_paciente_unidade_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.ATEND_PAC_UNID (NR_ATENDIMENTO, CD_SETOR_ATENDIMENTO, NR_SEQ_INTERNO, DT_ENTRADA_UNIDADE, DT_SAIDA_UNIDADE) VALUES (:1, :2, :3, :4, :5)"

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

        print("Dados ATEND_PAC_UNID inseridos")

        df_upd = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_setor_atendimento():
    print("Entrou no df_setor_atendimento")

    df_dim = pd.read_sql(query_setor_atendimento, connect_ugo())

    df_stage = pd.read_sql(query_setor_atendimento_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_SETOR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.SETOR_ATENDIMENTO (CD_SETOR_ATENDIMENTO, DS_SETOR_ATENDIMENTO, CD_CLASSIF_SETOR) VALUES (:1, :2, :3)"

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

    print("Dados SETOR_ATENDIMENTO inseridos")

    df_upd = df_dim.merge(df_stage["CD_SETOR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_atend_categoria_convenio():
    print("Entrou no df_atend_categoria_convenio")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_atend_categoria_convenio.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_atend_categoria_convenio_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.ATEND_CATEG_CONVENIO (NR_ATENDIMENTO, CD_CONVENIO, NR_SEQ_INTERNO, CD_CATEGORIA, DT_INICIO_VIGENCIA) VALUES (:1, :2, :3, :4, :5)"

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

        print("Dados ATEND_CATEG_CONVENIO inseridos")

        df_upd = df_dim.merge(df_stage["NR_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_convenio():
    print("Entrou no df_convenio")

    df_dim = pd.read_sql(query_convenio, connect_ugo())

    df_stage = pd.read_sql(query_convenio_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.CONVENIO (CD_CONVENIO, DS_CONVENIO) VALUES (:1, :2)"

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

def df_categoria_convenio():
    print("Entrou no df_categoria_convenio")

    df_dim = pd.read_sql(query_categoria_convenio, connect_ugo())

    df_stage = pd.read_sql(query_categoria_convenio_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.CATEGORIA_CONVENIO (CD_CONVENIO, CD_CATEGORIA) VALUES (:1, :2)"

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

    print("Dados CATEGORIA_CONVENIO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_pessoa_fisica_medico():
    print("Entrou no df_pessoa_fisica_medico")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_pessoa_fisica_medico.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_pessoa_fisica_medico_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.PESSOA_FISICA_MEDICO (CD_PESSOA_FISICA, IE_SEXO, DT_CADASTRO_ORIGINAL, NM_PESSOA_PESQUISA) VALUES (:1, :2, :3, :4)"

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

        print("Dados PESSOA_FISICA_MEDICO inseridos")

        df_upd = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_pessoa_fiica_pac():
    print("Entrou no df_pessoa_fiica_pac")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_pessoa_fisica_pac.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_pessoa_fisica_pac_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.PESSOA_FISICA_PAC (CD_PESSOA_FISICA, DT_NASCIMENTO, IE_SEXO, DT_CADASTRO_ORIGINAL) VALUES (:1, :2, :3, :4)"

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

        print("Dados PESSOA_FISICA_PAC inseridos")

        df_upd = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_pac_senha_fila():
    print("Entrou no df_pac_senha_fila")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(2019,1,1), until=datetime(2022,3,31)):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_pac_senha_fila.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo())

        df_stage = pd.read_sql(query_pac_senha_fila_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_ugo_hdata())

        df_diff = df_dim.merge(df_stage["NR_SEQ_FILA_SENHA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_ugo_hdata()

        cursor = con.cursor()

        sql="INSERT INTO UNIMED_GYN.PAC_SENHA_FILA (DT_INICIO_ATENDIMENTO, DT_GERACAO_SENHA, DT_FIM_ATENDIMENTO, NR_SEQ_FILA_SENHA, NR_SEQUENCIA) VALUES (:1, :2, :3, :4, :5)"

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

        print("Dados PAC_SENHA_FILA inseridos")

        df_upd = df_dim.merge(df_stage["NR_SEQ_FILA_SENHA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        df_upd = df_upd.drop(columns=['_merge'])
        df_upd = df_upd.reset_index(drop=True)

        print("dados para update")
        print(df_upd.info())

def df_motivo_alta():
    print("Entrou no df_motivo_alta")

    df_dim = pd.read_sql(query_motivo_alta, connect_ugo())

    df_stage = pd.read_sql(query_motivo_alta_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_MOTIVO_ALTA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.MOTIVO_ALTA (CD_MOTIVO_ALTA, DS_MOTIVO_ALTA) VALUES (:1, :2)"

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

    print("Dados MOTIVO_ALTA inseridos")

    df_upd = df_dim.merge(df_stage["CD_MOTIVO_ALTA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_valor_dominio():
    print("Entrou no df_valor_dominio")

    df_dim = pd.read_sql(query_valor_dominio, connect_ugo())

    df_stage = pd.read_sql(query_valor_dominio_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_DOMINIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.VALOR_DOMINIO (CD_DOMINIO, VL_DOMINIO, DS_VALOR_DOMINIO) VALUES (:1, :2, :3)"

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

    print("Dados ESTABELECIMENTOS inseridos")

    df_upd = df_dim.merge(df_stage["CD_DOMINIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_triagem_classif_risco():
    print("Entrou no df_triagem_classif_risco")

    df_dim = pd.read_sql(query_triagem_classif_risco, connect_ugo())

    df_stage = pd.read_sql(query_triagem_classif_risco_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["NR_SEQUENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.TRIAGEM_CLASSIF_RISCO (NR_SEQUENCIA, DS_CLASSIFICACAO) VALUES (:1, :2)"

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

    print("Dados TRIAGEM_CLASSIF_RISCO inseridos")

    df_upd = df_dim.merge(df_stage["NR_SEQUENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_medico_especialidade():
    print("Entrou no df_medico_especialidade")

    df_dim = pd.read_sql(query_medico_especialidade, connect_ugo())

    df_stage = pd.read_sql(query_medico_especialidade_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.MEDICO_ESPECIALIDADE (CD_PESSOA_FISICA, CD_ESPECIALIDADE) VALUES (:1, :2)"

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

    print("Dados ESTABELECIMENTOS inseridos")

    df_upd = df_dim.merge(df_stage["CD_PESSOA_FISICA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

def df_especialidade_medica():
    print("Entrou no df_especialidade_medica")

    df_dim = pd.read_sql(query_especialidade_medica, connect_ugo())

    df_stage = pd.read_sql(query_especialidade_medica_hdata, connect_ugo_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIALIDADE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_ugo_hdata()

    cursor = con.cursor()

    sql="INSERT INTO UNIMED_GYN.ESPECIALIDADE_MEDICA (CD_ESPECIALIDADE, DS_ESPECIALIDADE) VALUES (:1, :2)"

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

    print("Dados ESPECIALIDADE_MEDICA inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESPECIALIDADE"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)

# dag = DAG("insert_dados_unimed_go", default_args=default_args, schedule_interval=None)
dag = DAG("captura_dados_unimed_go", default_args=default_args, schedule_interval="0 6,7,8,9 * * *")

t0 = PythonOperator(
    task_id="captura_atendimento_paciente_rhp",
    python_callable=df_atendimento_paciente,
    on_failure_callback=notify_email,
    dag=dag)

t1 = PythonOperator(
    task_id="captura_estabelecimento_rhp",
    python_callable=df_estabelecimento,
    on_failure_callback=notify_email,
    dag=dag)

t2 = PythonOperator(
    task_id="captura_empresa_rhp",
    python_callable=df_empresa,
    on_failure_callback=notify_email,
    dag=dag)

t3 = PythonOperator(
    task_id="captura_ped_ex_ext_item_rhp",
    python_callable=df_ped_ex_ext_item,
    on_failure_callback=notify_email,
    dag=dag)

t4 = PythonOperator(
    task_id="captura_ped_ex_ext_rhp",
    python_callable=df_ped_ex_ext,
    on_failure_callback=notify_email,
    dag=dag)

t5 = PythonOperator(
    task_id="captura_exame_lab_rhp",
    python_callable=df_exame_lab,
    on_failure_callback=notify_email,
    dag=dag)

t6 = PythonOperator(
    task_id="captura_prescr_procedimento_rhp",
    python_callable=df_prescr_procedimento,
    on_failure_callback=notify_email,
    dag=dag)

t7 = PythonOperator(
    task_id="captura_prescr_medica_v_rhp",
    python_callable=df_prescr_medica_v,
    on_failure_callback=notify_email,
    dag=dag)

t8 = PythonOperator(
    task_id="captura_diagnostico_doenca_rhp",
    python_callable=df_diagnostico_doenca,
    on_failure_callback=notify_email,
    dag=dag)

t9 = PythonOperator(
    task_id="captura_atend_paciente_unidade_rhp",
    python_callable=df_atend_paciente_unidade,
    on_failure_callback=notify_email,
    dag=dag)

t10 = PythonOperator(
    task_id="captura_setor_atendimento_rhp",
    python_callable=df_setor_atendimento,
    on_failure_callback=notify_email,
    dag=dag)

t11 = PythonOperator(
    task_id="captura_atend_categoria_convenio_rhp",
    python_callable=df_atend_categoria_convenio,
    on_failure_callback=notify_email,
    dag=dag)

t12 = PythonOperator(
    task_id="captura_convenio_rhp",
    python_callable=df_convenio,
    on_failure_callback=notify_email,
    dag=dag)

t13 = PythonOperator(
    task_id="captura_categoria_convenio_rhp",
    python_callable=df_categoria_convenio,
    on_failure_callback=notify_email,
    dag=dag)

t14 = PythonOperator(
    task_id="captura_pessoa_fisica_medico_rhp",
    python_callable=df_pessoa_fisica_medico,
    on_failure_callback=notify_email,
    dag=dag)

t15 = PythonOperator(
    task_id="captura_pessoa_fisica_pac_rhp",
    python_callable=df_pessoa_fisica_pac,
    on_failure_callback=notify_email,
    dag=dag)

t16 = PythonOperator(
    task_id="captura_pac_senha_fila_rhp",
    python_callable=df_pac_senha_fila,
    on_failure_callback=notify_email,
    dag=dag)

t17 = PythonOperator(
    task_id="captura_motivo_alta_rhp",
    python_callable=df_motivo_alta,
    on_failure_callback=notify_email,
    dag=dag)

t18 = PythonOperator(
    task_id="captura_valor_dominio_rhp",
    python_callable=df_valor_dominio,
    on_failure_callback=notify_email,
    dag=dag)

t19 = PythonOperator(
    task_id="captura_cid_doenca_rhp",
    python_callable=df_cid_doenca,
    on_failure_callback=notify_email,
    dag=dag)

t20 = PythonOperator(
    task_id="captura_triagem_classif_risco_rhp",
    python_callable=df_triagem_classif_risco,
    on_failure_callback=notify_email,
    dag=dag)

t21 = PythonOperator(
    task_id="captura_medico_especialidade_rhp",
    python_callable=df_medico_especialidade,
    on_failure_callback=notify_email,
    dag=dag)

t22 = PythonOperator(
    task_id="captura_especialidade_medica_rhp",
    python_callable=df_especialidade_medica,
    on_failure_callback=notify_email,
    dag=dag)

(t1, t2, t3, t5, t7, t8, t10, t12, t13, t17, t18, t19, t20, t21, t22) >> t16 >> t15 >> t14 >> t11 >> t9 >> t6 >> t4 >> t0