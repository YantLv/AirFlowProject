from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import logging
import pandas as pd
from io import StringIO
import time
import os 

def generar_archivo_trigger():
    ruta = "/tmp/ventas_trigger.txt"

    # Borra si ya existe
    if os.path.exists(ruta):
        os.remove(ruta)
    time.sleep(40)
    with open(ruta, "w") as f:
        f.write("Archivo listo")

def crear_tabla():
    tabla = Variable.get("tabla_ventas_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla} (
        id SERIAL PRIMARY KEY,
        cliente VARCHAR(50),
        monto FLOAT
        );
    """)
    logging.info("Tabla creada correctamente.")

def insertar_datos():
    tabla = Variable.get("tabla_ventas_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        TRUNCATE TABLE {tabla};
        INSERT INTO {tabla} (cliente, monto) VALUES
        ('Juan', 500),
        ('Ana', 1500),
        ('Luis', 2500),
        ('Maria', 800);
    """)
    logging.info("Datos insertados correctamente.")

def leer_postgres(ti):
    tabla = Variable.get("tabla_ventas_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    df = hook.get_pandas_df(f"SELECT * FROM {tabla};")
    ti.xcom_push(key = "ventas_data", value = df.to_json())
    logging.info("Datos leidos desde Postgres.")

def transformar_datos(ti):
    data_json = ti.xcom_pull(key = "ventas_data", task_ids= "leer_postgres")
    df = pd.read_json(StringIO(data_json))

    df["monto_con_impuesto"] = df["monto"] * 1.16
    df["es_alto_valor"] = df["monto"] > 1000
    logging.info("Datos transformados")
    print(df)

    ti.xcom_push(key="ventas_transformadas", value=df.to_json())

def crear_tabla_destino():
    tabla_destino = Variable.get("tabla_ventas_destino")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla_destino} (
        id INT PRIMARY KEY,
        cliente VARCHAR(50),
        monto FLOAT,
        monto_con_impuesto FLOAT,
        es_alto_valor BOOLEAN
        );
    """)
    logging.info("Tabla destino verificada.")

def cargar_datos_transformados(ti):
    tabla_destino = Variable.get("tabla_ventas_destino")
    data_json = ti.xcom_pull(key="ventas_transformadas", task_ids="transformar_datos")
    df = pd.read_json(StringIO(data_json))
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"TRUNCATE TABLE {tabla_destino};")
    for _, row in df.iterrows():
        hook.run(f"""
        INSERT INTO {tabla_destino}
        (id, cliente, monto, monto_con_impuesto, es_alto_valor)
        VALUES (%s, %s, %s, %s, %s);
        """,
        parameters=(int(row["id"]), row["cliente"],
                float(row["monto"]),
                float(row["monto_con_impuesto"]),
                bool(row["es_alto_valor"]))
        )
    logging.info("Datos transformados cargados correctamente.")

def tarea_que_falla():
    raise ValueError("Error intencional para aplicar trigger rules.")

def reporte_final():
    print("Pipeline finalizado")

with DAG(
    dag_id = "etl_ventas_final",
    start_date=datetime(2026, 5, 19),
    schedule=None,
    catchup=False
) as dag:

    inicio = EmptyOperator(task_id = "Inicio")
    fin = EmptyOperator(task_id = "Fin")

    generar_archivo = PythonOperator(
        task_id = "generar_archivo_trigger",
        python_callable = generar_archivo_trigger
    )
    esperar_archivo = FileSensor(
    task_id="esperar_archivo",
    fs_conn_id="fs_default",
    filepath="/tmp/ventas_trigger.txt",
    poke_interval=10,
    timeout=120,
    mode="poke"
    )

    crear = PythonOperator(
        task_id="crear_tabla",
        python_callable=crear_tabla
    )
 
    insertar = PythonOperator(
        task_id="insertar_datos",
        python_callable=insertar_datos
    )
    
    leer_postgres = PythonOperator(
        task_id = "leer_postgres",
        python_callable = leer_postgres
    )
    transformar_datos = PythonOperator(
        task_id = "transformar_datos",
        python_callable = transformar_datos
    )
    crear_tabla_destino = PythonOperator(
        task_id = "crear_tabla_destino",
        python_callable = crear_tabla_destino
    )
    cargar_datos_transformados = PythonOperator(
        task_id = "cargar_datos_transformados",
        python_callable = cargar_datos_transformados
    )


    falla = PythonOperator(
        task_id ="falla",
        python_callable = tarea_que_falla
    )

    reporte = PythonOperator(
    task_id="reporte_final",
    python_callable=reporte_final,
    trigger_rule="all_done"
    )
# flujo
inicio >> [generar_archivo, esperar_archivo] >> crear >> insertar >> leer_postgres >> transformar_datos >> crear_tabla_destino >> cargar_datos_transformados >> falla >> reporte >> fin