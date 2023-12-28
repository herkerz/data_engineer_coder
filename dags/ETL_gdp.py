from datetime import timedelta,datetime, date
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os
import smtplib

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()
with open(dag_path+'/keys/'+"api_key.txt",'r') as f:
    api_key= f.read()
    
redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'HK',
    'start_date': datetime(2023,12,11),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

gdp_dag = DAG(
    dag_id='GDP_dag',
    default_args=default_args,
    description='Extraccion, transformacion y carga de datos de GDP en FRED',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def extraer_data():
    try:
         print(f"Adquiriendo data!")
         today = date.today()
         url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key={api_key}&file_type=json"
         headers = {"Accept-Encoding": "gzip, deflate"}
         response = requests.get(url, headers=headers)
         if response:
              print('Success!')
              data = response.json()
              with open(dag_path+'/raw_data/'+"data_"+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+".json", "w") as json_file:
                   json.dump(data, json_file)
         else:
              print('An error has occurred.') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# Funcion de transformacion en tabla
def transformar_data():       
    print(f"Transformando la data") 
    today = date.today()
    with open(dag_path+'/raw_data/'+"data_"+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
    # Extraer la data en tabla
    df = pd.DataFrame( loaded_data['observations'] )
    df = df[df.value != "."] 
    df['date'] = df['date'].astype('datetime64[ns]').dt.date
    df = df[['date', 'value']]
   
    df.to_csv(dag_path+'/processed_data/'+"data_"+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+".csv", index=False, mode='w')

# Funcion conexion a redshift
def conexion_redshift():
    try:
        conn = psycopg2.connect(
            host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
            port=5439,
            database=data_base,
            user=user,
            password=pwd
        )
        print('Connected to Redshift')
    except Exception as e:
        print(e)

from psycopg2.extras import execute_values
# Funcion de envio de data
def cargar_en_redshift( ):
    
    conn = psycopg2.connect(
            host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
            port=5439,
            database=data_base,
            user=user,
            password=pwd
        )
            
            
    today = date.today()
    df=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+".csv")
    df['date'] = pd.to_datetime(df['date'])
    dtypes = df.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(50)','datetime64[ns]' : 'DATE'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS GDP (
            {', '.join(column_defs)}
        );
    """
    cur = conn.cursor()
    cur.execute(table_schema)
    
    # Consulta para obtener las fechas existentes en la tabla
    cur.execute(f"SELECT DISTINCT date FROM GDP;")
    existing_dates = set(row[0] for row in cur.fetchall())
    # Filtra los datos para insertar solo los que no existen en la tabla
    values = [tuple(x) for x in df.to_numpy() if x[0] not in existing_dates]
    if not values:
        print('No new data to insert.')
    else:
        insert_sql = f"INSERT INTO GDP ({', '.join(cols)}) VALUES %s"
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print(f'Proceso terminado. Insertados {len(values)} nuevos registros.')
    
def enviar():
    try:
        today = date.today()
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('hgkerz@gmail.com','xfuzqeopwyrwffrj')
        subject=f'GDP {today.year}-{today.month}-{today.day} Successfull'
        body_text='La actualizacion de la data de GDP fue exitosa'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('hgkerz@gmail.com','hgkerz@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        
        
# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    dag=gdp_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    dag=gdp_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    dag=gdp_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_en_redshift,
    dag=gdp_dag,
)


# 3.2 Envio final
task_4= PythonOperator(
    task_id='envio_mail',
    python_callable=enviar,
    dag=gdp_dag,
)
# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32 >> task_4